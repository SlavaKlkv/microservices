"""HTTP-слой сервиса заказов на живой базе.

Проверяется контракт наружу: статусы кодов, тело ответа и то, что
операции, завершающие заказ, действительно публикуют события.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ms_events import EventType
from orders.models import OrderStatus
from orders.schemas import OrderCreate, OrderRead, OrderUpdate
from orders.service import OrderService
from tests.integration.test_orders_saga import (
    USER_ID,
    create_order,
    outbox_rows,
)

pytestmark = [pytest.mark.integration, pytest.mark.service('orders')]


async def test_created_order_is_readable_with_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # status в OrderRead — то, по чему клиент узнаёт исход саги.
    order_id = await create_order(session_factory)

    async with session_factory() as session:
        order: OrderRead = await OrderService(session).get_by_id(order_id)

    assert order.id == order_id
    assert order.user_id == USER_ID
    assert order.status is OrderStatus.PENDING


async def test_missing_order_raises_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        with pytest.raises(HTTPException) as exc:
            await OrderService(session).get_by_id(999_999)

    assert exc.value.status_code == 404


async def test_listing_is_paginated(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(3):
        await create_order(session_factory)

    async with session_factory() as session:
        page = await OrderService(session).get_all(limit=2, offset=0)

    assert page.total == 3
    assert len(page.items) == 2


async def test_price_change_publishes_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory, price='100.00')

    async with session_factory() as session:
        await OrderService(session).update(
            order_id, OrderUpdate(total_price=Decimal('150.00'))
        )

    last = (await outbox_rows(session_factory, order_id))[-1]
    assert last.event_type == str(EventType.ORDER_PRICE_CHANGED)
    assert last.payload['old_total_price'] == '100.00'
    assert last.payload['new_total_price'] == '150.00'


async def test_price_kept_the_same_publishes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory, price='100.00')
    before = len(await outbox_rows(session_factory, order_id))

    async with session_factory() as session:
        await OrderService(session).update(
            order_id, OrderUpdate(total_price=Decimal('100.00'))
        )

    assert len(await outbox_rows(session_factory, order_id)) == before


async def test_manual_confirm_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)

    async with session_factory() as session:
        await OrderService(session).confirm(order_id)
    before = len(await outbox_rows(session_factory, order_id))

    async with session_factory() as session:
        result = await OrderService(session).confirm(order_id)

    assert result.status is OrderStatus.CONFIRMED
    assert len(await outbox_rows(session_factory, order_id)) == before


async def test_cancel_after_confirm_is_a_conflict(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Через HTTP несовместимый переход — ошибка запроса (409). В саге
    # такой же случай, наоборот, тихий no-op: там некому показать ошибку.
    order_id = await create_order(session_factory)
    async with session_factory() as session:
        await OrderService(session).confirm(order_id)

    async with session_factory() as session:
        with pytest.raises(HTTPException) as exc:
            await OrderService(session).cancel(order_id)

    assert exc.value.status_code == 409


async def test_delete_removes_the_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)

    async with session_factory() as session:
        deleted = await OrderService(session).delete(order_id)

    assert deleted.deleted is True
    async with session_factory() as session:
        with pytest.raises(HTTPException):
            await OrderService(session).get_by_id(order_id)


async def test_create_accepts_zero_price(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        order = await OrderService(session).create(
            OrderCreate(total_price=Decimal('0')), user_id=USER_ID
        )

    assert order.total_price == Decimal('0')
