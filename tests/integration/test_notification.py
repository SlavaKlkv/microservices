"""Сервис уведомлений: дедупликация и публикация результата саги.

Запись уведомления и строка outbox с результатом обязаны появляться в
одной транзакции. Иначе возможен худший из исходов: письмо отправлено,
а сага о нём не узнала и отменила заказ.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ms_events import EventEnvelope, EventType, Producer
from notification.models import (
    Notification,
    NotificationStatus,
    OutboxEvent,
    ProcessedEvent,
)
from notification.service import (
    NotificationService,
    build_message,
    resolve_recipient,
)
from notification.settings import settings

pytestmark = [pytest.mark.integration, pytest.mark.service('notification')]

ORDER_ID = 42
USER_ID = 7


def notify_requested(
    *, order_id: int = ORDER_ID, saga_id: UUID | None = None
) -> EventEnvelope:
    return EventEnvelope(
        event_type=EventType.ORDER_NOTIFY_REQUESTED,
        saga_id=saga_id or uuid4(),
        producer=Producer.ORDERS,
        aggregate_id=str(order_id),
        payload={
            'order_id': order_id,
            'user_id': USER_ID,
            'total_price': '199.99',
            'status': 'PENDING',
        },
    )


async def handle(
    factory: async_sessionmaker[AsyncSession], envelope: EventEnvelope
) -> Notification | None:
    async with factory() as session:
        return await NotificationService(session).handle_notify_requested(
            envelope
        )


async def outbox_rows(
    factory: async_sessionmaker[AsyncSession],
) -> list[OutboxEvent]:
    async with factory() as session:
        stmt = sa.select(OutboxEvent).order_by(OutboxEvent.id)
        return list((await session.execute(stmt)).scalars().all())


async def count(factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with factory() as session:
        stmt = sa.select(sa.func.count()).select_from(model)
        return int((await session.execute(stmt)).scalar_one())


@pytest.fixture
def always_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Гарантированный отказ отправки — ветка компенсации саги."""
    monkeypatch.setattr(settings, 'NOTIFICATION_FAIL_RATE', 1.0)


async def test_notification_is_recorded(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    notification = await handle(session_factory, notify_requested())

    assert notification is not None
    assert notification.order_id == ORDER_ID
    assert notification.user_id == USER_ID
    assert notification.status is NotificationStatus.SENT


async def test_success_publishes_order_notified(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    envelope = notify_requested()

    await handle(session_factory, envelope)

    rows = await outbox_rows(session_factory)
    assert [r.event_type for r in rows] == [str(EventType.ORDER_NOTIFIED)]
    # Результат остаётся в той же саге и ссылается на свою причину.
    assert rows[0].saga_id == str(envelope.saga_id)
    assert rows[0].causation_id == str(envelope.event_id)


async def test_failure_publishes_notification_failed(
    session_factory: async_sessionmaker[AsyncSession], always_fail: None
) -> None:
    notification = await handle(session_factory, notify_requested())

    assert notification is not None
    assert notification.status is NotificationStatus.FAILED
    rows = await outbox_rows(session_factory)
    assert [r.event_type for r in rows] == [
        str(EventType.ORDER_NOTIFICATION_FAILED)
    ]
    assert rows[0].payload['reason']


async def test_duplicate_event_is_ignored(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    envelope = notify_requested()

    first = await handle(session_factory, envelope)
    second = await handle(session_factory, envelope)

    assert first is not None
    assert second is None
    assert await count(session_factory, Notification) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_duplicate_does_not_send_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Выбор осознанный: лучше не отправить, чем отправить дважды.
    envelope = notify_requested()

    await handle(session_factory, envelope)
    await handle(session_factory, envelope)

    assert await count(session_factory, ProcessedEvent) == 1


async def test_different_events_are_processed_independently(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await handle(session_factory, notify_requested(order_id=1))
    await handle(session_factory, notify_requested(order_id=2))

    assert await count(session_factory, Notification) == 2
    assert await count(session_factory, OutboxEvent) == 2


def test_message_mentions_the_order() -> None:
    subject, body = build_message(notify_requested())

    assert str(ORDER_ID) in subject
    assert str(ORDER_ID) in body
    assert '199.99' in body


def test_recipient_prefers_email_from_payload() -> None:
    envelope = notify_requested()
    envelope.payload['user_email'] = 'buyer@example.com'

    assert resolve_recipient(envelope) == 'buyer@example.com'


def test_recipient_falls_back_to_placeholder() -> None:
    # Заглушка на несуществующем домене: письмо никуда не уйдёт, но и
    # уведомление не потеряется без адреса.
    assert resolve_recipient(notify_requested()).endswith('.invalid')
