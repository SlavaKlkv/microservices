"""История заказов: журнал всей саги, ровно по одной строке на событие."""

from __future__ import annotations

from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ms_events import EventType
from orders_history.models import OrderHistory
from orders_history.schemas import HistoryEventIn
from orders_history.service import HistoryService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.service('orders_history'),
]

ORDER_ID = 42
USER_ID = 7


def history_event(
    event_type: EventType = EventType.ORDER_CREATED,
    *,
    event_id: str | None = None,
    saga_id: str | None = None,
) -> HistoryEventIn:
    return HistoryEventIn(
        event_id=event_id or str(uuid4()),
        event_type=str(event_type),
        order_id=ORDER_ID,
        user_id=USER_ID,
        saga_id=saga_id or str(uuid4()),
        payload={'order_id': ORDER_ID, 'user_id': USER_ID},
    )


async def record(
    factory: async_sessionmaker[AsyncSession], event: HistoryEventIn
) -> object | None:
    async with factory() as session:
        return await HistoryService(session).record_event(event)


async def count_rows(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as session:
        stmt = sa.select(sa.func.count()).select_from(OrderHistory)
        return int((await session.execute(stmt)).scalar_one())


async def test_event_is_recorded(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert await record(session_factory, history_event()) is not None
    assert await count_rows(session_factory) == 1


async def test_duplicate_event_is_skipped(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = history_event()

    assert await record(session_factory, event) is not None
    assert await record(session_factory, event) is None
    assert await count_rows(session_factory) == 1


async def test_whole_saga_chain_lands_in_history(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Четыре события успешной саги должны лежать под одним saga_id —
    # именно так историю потом и читают при разборе инцидента.
    saga_id = str(uuid4())
    chain = [
        EventType.ORDER_CREATED,
        EventType.ORDER_NOTIFY_REQUESTED,
        EventType.ORDER_NOTIFIED,
        EventType.ORDER_CONFIRMED,
    ]
    for event_type in chain:
        await record(
            session_factory, history_event(event_type, saga_id=saga_id)
        )

    async with session_factory() as session:
        stmt = (
            sa.select(OrderHistory)
            .where(OrderHistory.saga_id == saga_id)
            .order_by(OrderHistory.id)
        )
        rows = list((await session.execute(stmt)).scalars().all())

    assert [r.event_type for r in rows] == [str(e) for e in chain]


async def test_listing_filters_by_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await record(session_factory, history_event())
    other = history_event()
    other.order_id = 999
    await record(session_factory, other)

    async with session_factory() as session:
        page = await HistoryService(session).list_history(
            limit=50, offset=0, order_id=ORDER_ID
        )

    assert page.total == 1
    assert page.items[0].order_id == ORDER_ID
