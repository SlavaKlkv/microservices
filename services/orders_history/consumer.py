"""Консьюмер сервиса истории заказов.

Слушает оба топика саги — заказов и уведомлений, — чтобы в журнале была
вся цепочка целиком, а не только события orders. Разбор конверта, DLQ и
ограниченные повторы обеспечивает общий ``ms_events.EventConsumer``.
"""

from __future__ import annotations

import asyncio

import structlog
from ms_events import (
    EventConsumer,
    EventEnvelope,
    Topic,
    run_consumer,
    setup_logging,
)

from orders_history.core.db import SessionLocal
from orders_history.schemas import HistoryEventIn
from orders_history.service import HistoryService
from orders_history.settings import settings

logger = structlog.get_logger('orders_history.consumer')

#: История пишет всё, что происходит в саге.
TOPICS = (Topic.ORDERS, Topic.NOTIFICATIONS)


def to_history_event(envelope: EventEnvelope) -> HistoryEventIn:
    """Переводит конверт события в схему записи журнала."""
    payload = envelope.payload
    return HistoryEventIn(
        event_id=str(envelope.event_id),
        event_type=str(envelope.event_type),
        order_id=int(payload.get('order_id') or envelope.aggregate_id),
        user_id=int(payload['user_id']),
        saga_id=str(envelope.saga_id),
        payload=payload,
    )


async def handle_event(envelope: EventEnvelope, source_topic: str) -> None:
    """Записывает одно событие в историю (идемпотентно по event_id)."""
    event = to_history_event(envelope)

    async with SessionLocal() as session:
        result = await HistoryService(session).record_event(event)

    if result is None:
        logger.info(
            'history.event_skipped',
            event_id=event.event_id,
            reason='already_processed',
        )
    else:
        logger.info(
            'history.event_recorded',
            event_id=event.event_id,
            event_type=event.event_type,
            order_id=event.order_id,
            source_topic=source_topic,
        )


def build_consumer() -> EventConsumer:
    return EventConsumer(
        service='orders-history',
        topics=TOPICS,
        group_id=settings.KAFKA_GROUP_ID,
        settings=settings,
        handler=handle_event,
    )


async def consume(stop_event: asyncio.Event | None = None) -> None:
    await build_consumer().run(stop_event)


def main() -> None:
    setup_logging('orders-history-consumer', level=settings.LOG_LEVEL)
    try:
        asyncio.run(run_consumer(build_consumer()))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
