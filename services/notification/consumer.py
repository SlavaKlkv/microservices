"""Консьюмер сервиса уведомлений.

Слушает топик заказов и реагирует только на order.notify_requested:
остальные события саги сервису не адресованы.
"""

from __future__ import annotations

import asyncio

import structlog
from ms_events import (
    EventConsumer,
    EventEnvelope,
    EventType,
    Topic,
    run_consumer,
    setup_logging,
)

from notification.core.db import SessionLocal
from notification.service import NotificationService
from notification.settings import settings

logger = structlog.get_logger('notification.consumer')

TOPICS = (Topic.ORDERS,)


async def handle_event(envelope: EventEnvelope, source_topic: str) -> None:
    if envelope.event_type != EventType.ORDER_NOTIFY_REQUESTED:
        return

    async with SessionLocal() as session:
        notification = await NotificationService(
            session
        ).handle_notify_requested(envelope)

    if notification is None:
        logger.info(
            'notification.event_skipped',
            event_id=str(envelope.event_id),
            reason='already_processed',
        )
    else:
        logger.info(
            'notification.recorded',
            event_id=str(envelope.event_id),
            notification_id=notification.id,
            status=notification.status.value,
            order_id=notification.order_id,
        )


def build_consumer() -> EventConsumer:
    return EventConsumer(
        service='notification',
        topics=TOPICS,
        group_id=settings.KAFKA_GROUP_ID,
        settings=settings,
        handler=handle_event,
    )


async def consume(stop_event: asyncio.Event | None = None) -> None:
    await build_consumer().run(stop_event)


def main() -> None:
    setup_logging('notification-consumer', level=settings.LOG_LEVEL)
    try:
        asyncio.run(run_consumer(build_consumer()))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
