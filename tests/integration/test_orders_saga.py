"""Сага заказа на живом PostgreSQL.

Проверяется главное обещание системы: заказ и события о нём пишутся в
одной транзакции, а переходы идемпотентны — повтор события при
at-least-once доставке не должен ни ломать заказ, ни плодить события.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ms_events import EventEnvelope, EventType, Producer
from orders.models import Order, OrderSaga, OrderStatus, OutboxEvent
from orders.schemas import OrderCreate
from orders.service import OrderService

pytestmark = [pytest.mark.integration, pytest.mark.service('orders')]

USER_ID = 7


async def create_order(
    factory: async_sessionmaker[AsyncSession], price: str = '199.99'
) -> int:
    async with factory() as session:
        order = await OrderService(session).create(
            OrderCreate(total_price=Decimal(price)), user_id=USER_ID
        )
    return order.id


async def outbox_rows(
    factory: async_sessionmaker[AsyncSession], order_id: int
) -> list[OutboxEvent]:
    async with factory() as session:
        stmt = (
            sa.select(OutboxEvent)
            .where(OutboxEvent.aggregate_id == order_id)
            .order_by(OutboxEvent.id)
        )
        return list((await session.execute(stmt)).scalars().all())


async def get_order(
    factory: async_sessionmaker[AsyncSession], order_id: int
) -> Order:
    async with factory() as session:
        order = await session.get(Order, order_id)
        assert order is not None
        return order


async def get_saga(
    factory: async_sessionmaker[AsyncSession], order_id: int
) -> OrderSaga | None:
    async with factory() as session:
        stmt = sa.select(OrderSaga).where(OrderSaga.order_id == order_id)
        return (await session.execute(stmt)).scalar_one_or_none()


def notification_result(
    order_id: int,
    saga_id: UUID,
    *,
    succeeded: bool,
    reason: str | None = None,
) -> EventEnvelope:
    """Событие, которое присылает сервис уведомлений."""
    payload: dict[str, object] = {'order_id': order_id, 'user_id': USER_ID}
    if reason is not None:
        payload['reason'] = reason
    return EventEnvelope(
        event_type=(
            EventType.ORDER_NOTIFIED
            if succeeded
            else EventType.ORDER_NOTIFICATION_FAILED
        ),
        saga_id=saga_id,
        producer=Producer.NOTIFICATION,
        aggregate_id=str(order_id),
        payload=payload,
    )


async def apply(
    factory: async_sessionmaker[AsyncSession], envelope: EventEnvelope
) -> OrderStatus | None:
    async with factory() as session:
        return await OrderService(session).apply_notification_result(envelope)


# --------------------------------------------------------------------
# Старт саги
# --------------------------------------------------------------------


async def test_create_writes_order_and_two_events_atomically(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)

    order = await get_order(session_factory, order_id)
    rows = await outbox_rows(session_factory, order_id)

    assert order.status is OrderStatus.PENDING
    assert [r.event_type for r in rows] == [
        str(EventType.ORDER_CREATED),
        str(EventType.ORDER_NOTIFY_REQUESTED),
    ]


async def test_start_events_share_one_saga_id(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Общий saga_id — единственное, что связывает события в цепочку.
    order_id = await create_order(session_factory)

    rows = await outbox_rows(session_factory, order_id)

    assert len({r.saga_id for r in rows}) == 1


async def test_notify_requested_points_at_created(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)

    created, notify = await outbox_rows(session_factory, order_id)

    assert notify.causation_id == created.event_id


# --------------------------------------------------------------------
# Успешная ветка
# --------------------------------------------------------------------


async def test_notified_confirms_the_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    status = await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=True),
    )

    assert status is OrderStatus.CONFIRMED
    assert (
        await get_order(session_factory, order_id)
    ).status is OrderStatus.CONFIRMED


async def test_confirmation_publishes_order_confirmed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=True),
    )

    rows = await outbox_rows(session_factory, order_id)
    assert [r.event_type for r in rows][-1] == str(EventType.ORDER_CONFIRMED)
    # Событие остаётся в той же саге, иначе история распадётся.
    assert rows[-1].saga_id == str(saga_id)


async def test_saga_row_reaches_terminal_state(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=True),
    )

    saga = await get_saga(session_factory, order_id)
    assert saga is not None
    assert saga.state == 'CONFIRMED'


# --------------------------------------------------------------------
# Компенсация
# --------------------------------------------------------------------


async def test_failed_notification_cancels_the_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    status = await apply(
        session_factory,
        notification_result(
            order_id, saga_id, succeeded=False, reason='smtp is down'
        ),
    )

    order = await get_order(session_factory, order_id)
    assert status is OrderStatus.CANCELLED
    assert order.status is OrderStatus.CANCELLED
    assert order.cancel_reason == 'smtp is down'


async def test_compensation_publishes_order_cancelled(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    await apply(
        session_factory,
        notification_result(
            order_id, saga_id, succeeded=False, reason='smtp is down'
        ),
    )

    last = (await outbox_rows(session_factory, order_id))[-1]
    assert last.event_type == str(EventType.ORDER_CANCELLED)
    assert last.payload['reason'] == 'smtp is down'


async def test_failure_without_reason_still_cancels(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=False),
    )

    order = await get_order(session_factory, order_id)
    assert order.status is OrderStatus.CANCELLED
    assert order.cancel_reason


# --------------------------------------------------------------------
# Идемпотентность
# --------------------------------------------------------------------


async def test_same_event_twice_changes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Ровно тот сценарий, ради которого существует processed_event:
    # Kafka доставляет at-least-once, дубли неизбежны.
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)
    event = notification_result(order_id, saga_id, succeeded=True)

    await apply(session_factory, event)
    before = [r.event_id for r in await outbox_rows(session_factory, order_id)]

    # None — «событие уже обработано», а не «что-то пошло не так».
    assert await apply(session_factory, event) is None
    after = [r.event_id for r in await outbox_rows(session_factory, order_id)]
    assert after == before


async def test_duplicate_does_not_publish_second_confirmation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)
    event = notification_result(order_id, saga_id, succeeded=True)

    await apply(session_factory, event)
    await apply(session_factory, event)

    rows = await outbox_rows(session_factory, order_id)
    confirmed = [
        r for r in rows if r.event_type == str(EventType.ORDER_CONFIRMED)
    ]
    assert len(confirmed) == 1


async def test_new_event_on_already_confirmed_order_is_noop(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Другой event_id, но заказ уже в целевом статусе: 409 здесь был бы
    # ошибкой — консьюмер начал бы гонять событие по кругу до DLQ.
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)

    await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=True),
    )
    before = len(await outbox_rows(session_factory, order_id))

    status = await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=True),
    )

    assert status is OrderStatus.CONFIRMED
    assert len(await outbox_rows(session_factory, order_id)) == before


async def test_saga_does_not_override_manual_cancel(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Оператор отменил заказ руками — пришедшее следом order.notified
    # не должно его «воскрешать».
    order_id = await create_order(session_factory)
    saga_id = UUID((await outbox_rows(session_factory, order_id))[0].saga_id)
    async with session_factory() as session:
        await OrderService(session).cancel(order_id)

    status = await apply(
        session_factory,
        notification_result(order_id, saga_id, succeeded=True),
    )

    assert status is OrderStatus.CANCELLED
    assert (
        await get_order(session_factory, order_id)
    ).status is OrderStatus.CANCELLED


async def test_event_for_missing_order_is_ignored(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Заказ удалили, событие о нём осталось в топике: консьюмер обязан
    # спокойно его проглотить, а не падать в бесконечные повторы.
    status = await apply(
        session_factory,
        notification_result(999_999, uuid4(), succeeded=True),
    )

    assert status is None
