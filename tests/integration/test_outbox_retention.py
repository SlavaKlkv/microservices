"""Уборка outbox и журнала обработанных событий.

Обе таблицы растут на каждое событие саги и без retention не
ограничены ничем. Проверяется, что уборка забирает именно отработавшее и
не трогает то, что ещё нужно: неотправленные строки и след события,
ушедшего в DLQ.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ms_events import OutboxWorker
from ms_events.db import OutboxStatus
from ms_events.envelope import utcnow
from orders.models import OutboxEvent, ProcessedEvent
from orders.settings import settings

pytestmark = [pytest.mark.integration, pytest.mark.service('orders')]


def _worker(factory: async_sessionmaker[AsyncSession]) -> OutboxWorker:
    return OutboxWorker(
        service='orders',
        model=OutboxEvent,
        processed_model=ProcessedEvent,
        session_factory=factory,
        settings=settings,
    )


async def _add_outbox_row(
    session: AsyncSession,
    *,
    status: OutboxStatus,
    age_days: int,
) -> str:
    event_id = str(uuid4())
    sent_at = utcnow() - timedelta(days=age_days)
    session.add(
        OutboxEvent(
            event_id=event_id,
            event_type='order.created',
            saga_id=str(uuid4()),
            producer='orders',
            topic='order.events',
            aggregate_type='order',
            aggregate_id=1,
            payload={},
            status=status,
            sent_at=sent_at if status is not OutboxStatus.NEW else None,
        )
    )
    return event_id


async def _outbox_ids(
    factory: async_sessionmaker[AsyncSession],
) -> set[str]:
    async with factory() as session:
        rows = await session.execute(sa.select(OutboxEvent.event_id))
        return set(rows.scalars().all())


async def test_cleanup_removes_only_old_sent_rows(session_factory) -> None:
    async with session_factory() as session:
        old_sent = await _add_outbox_row(
            session, status=OutboxStatus.SENT, age_days=30
        )
        fresh_sent = await _add_outbox_row(
            session, status=OutboxStatus.SENT, age_days=1
        )
        new_row = await _add_outbox_row(
            session, status=OutboxStatus.NEW, age_days=30
        )
        await session.commit()

    async with session_factory() as session:
        await _worker(session_factory).cleanup(session)

    remaining = await _outbox_ids(session_factory)

    assert old_sent not in remaining, 'отправленная давно строка должна уйти'
    assert fresh_sent in remaining, 'свежая отправленная строка нужна'
    assert new_row in remaining, 'неотправленная строка не трогается никогда'


async def test_cleanup_keeps_dead_rows(session_factory) -> None:
    """DEAD — единственный след события, ушедшего в DLQ."""

    async with session_factory() as session:
        dead = await _add_outbox_row(
            session, status=OutboxStatus.DEAD, age_days=365
        )
        await session.commit()

    async with session_factory() as session:
        await _worker(session_factory).cleanup(session)

    assert dead in await _outbox_ids(session_factory)


async def test_cleanup_prunes_processed_events(session_factory) -> None:
    old_id, fresh_id = str(uuid4()), str(uuid4())

    async with session_factory() as session:
        session.add(
            ProcessedEvent(
                event_id=old_id,
                event_type='order.created',
                processed_at=utcnow() - timedelta(days=90),
            )
        )
        session.add(
            ProcessedEvent(
                event_id=fresh_id,
                event_type='order.created',
                processed_at=utcnow() - timedelta(days=1),
            )
        )
        await session.commit()

    async with session_factory() as session:
        await _worker(session_factory).cleanup(session)

    async with session_factory() as session:
        rows = await session.execute(sa.select(ProcessedEvent.event_id))
        remaining = set(rows.scalars().all())

    assert old_id not in remaining
    assert fresh_id in remaining, 'свежая отметка ещё защищает от дубля'


async def test_cleanup_is_disabled_by_zero_retention(
    session_factory, monkeypatch
) -> None:
    monkeypatch.setattr(settings, 'OUTBOX_RETENTION_DAYS', 0)
    monkeypatch.setattr(settings, 'PROCESSED_EVENT_RETENTION_DAYS', 0)

    async with session_factory() as session:
        old_sent = await _add_outbox_row(
            session, status=OutboxStatus.SENT, age_days=365
        )
        await session.commit()

    async with session_factory() as session:
        deleted = await _worker(session_factory).cleanup(session)

    assert deleted == 0
    assert old_sent in await _outbox_ids(session_factory)
