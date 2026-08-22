"""Сквозной прогон саги на поднятом стеке.

Проверяется то, что не проверить ни юнит-, ни интеграционными тестами:
события действительно доезжают через Kafka, консьюмеры их разбирают, а
заказ сам приходит в терминальный статус без единого ручного вызова.
"""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest

from tests.e2e.conftest import SAGA_TIMEOUT_SEC

pytestmark = pytest.mark.e2e

#: Полная цепочка событий успешной саги.
HAPPY_PATH = [
    'order.created',
    'order.notify_requested',
    'order.notified',
    'order.confirmed',
]


async def create_order(
    client: httpx.AsyncClient, headers: dict[str, str], price: str = '199.99'
) -> int:
    response = await client.post(
        '/orders/api/v1/orders',
        json={'total_price': price},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return int(response.json()['id'])


async def wait_for_status(
    client: httpx.AsyncClient, order_id: int, expected: set[str]
) -> str:
    """Ждёт, пока сага доведёт заказ до одного из ожидаемых статусов."""
    deadline = time.monotonic() + SAGA_TIMEOUT_SEC
    last = ''
    while time.monotonic() < deadline:
        response = await client.get(f'/orders/api/v1/orders/{order_id}')
        assert response.status_code == 200, response.text
        body = response.json()
        last = str(body.get('status', ''))
        if last in expected:
            return last
        await asyncio.sleep(0.5)
    raise AssertionError(
        f'заказ {order_id} за {SAGA_TIMEOUT_SEC}s не дошёл до {expected}, '
        f'последний статус: {last!r}'
    )


async def fetch_history(
    client: httpx.AsyncClient, order_id: int
) -> list[dict[str, object]]:
    response = await client.get(
        '/history/api/v1/orders_history/history',
        params={'order_id': order_id},
    )
    assert response.status_code == 200, response.text
    return list(response.json()['items'])


async def test_order_reaches_confirmed_by_itself(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    order_id = await create_order(client, auth_headers)

    status = await wait_for_status(client, order_id, {'CONFIRMED'})

    assert status == 'CONFIRMED'


async def test_history_collects_the_whole_chain(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    order_id = await create_order(client, auth_headers)
    await wait_for_status(client, order_id, {'CONFIRMED'})

    # Истории нужен ещё один оборот консьюмера после order.confirmed.
    deadline = time.monotonic() + SAGA_TIMEOUT_SEC
    items: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        items = await fetch_history(client, order_id)
        if len(items) >= len(HAPPY_PATH):
            break
        await asyncio.sleep(0.5)

    assert sorted(str(i['event_type']) for i in items) == sorted(HAPPY_PATH)


async def test_whole_chain_shares_one_saga_id(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    order_id = await create_order(client, auth_headers)
    await wait_for_status(client, order_id, {'CONFIRMED'})

    deadline = time.monotonic() + SAGA_TIMEOUT_SEC
    items: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        items = await fetch_history(client, order_id)
        if len(items) >= len(HAPPY_PATH):
            break
        await asyncio.sleep(0.5)

    assert len({str(i['saga_id']) for i in items}) == 1


async def test_notification_is_visible(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    order_id = await create_order(client, auth_headers)
    await wait_for_status(client, order_id, {'CONFIRMED'})

    response = await client.get(
        '/notifications/api/v1/notifications',
        params={'order_id': order_id},
    )

    assert response.status_code == 200, response.text
    assert response.json()['total'] >= 1


@pytest.mark.skipif(
    os.getenv('E2E_EXPECT_COMPENSATION') != '1',
    reason=(
        'ветка компенсации требует стека с NOTIFICATION_FAIL_RATE=1.0; '
        'включается E2E_EXPECT_COMPENSATION=1'
    ),
)
async def test_failed_notification_cancels_the_order(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    order_id = await create_order(client, auth_headers)

    status = await wait_for_status(client, order_id, {'CANCELLED'})

    assert status == 'CANCELLED'
