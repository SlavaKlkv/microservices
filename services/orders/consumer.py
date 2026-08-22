"""Сага-консьюмер сервиса заказов.

Слушает только топик уведомлений. На собственный топик сервис не
подписывается: он же в него и публикует, подписка замкнула бы сагу в
бесконечный цикл. Дополнительно стоит фильтр по producer — на случай,
если в топик уведомлений когда-нибудь начнёт писать сам orders.
"""

from __future__ import annotations

import asyncio

import structlog
from ms_events import (
    EventConsumer,
    EventEnvelope,
    EventType,
    Producer,
    Topic,
    run_consumer,
    setup_logging,
)

from orders.core.db import SessionLocal
from orders.service import OrderService
from orders.settings import settings

logger = structlog.get_logger('orders.consumer')

TOPICS = (Topic.NOTIFICATIONS,)

#: События, завершающие сагу заказа.
HANDLED = {
    EventType.ORDER_NOTIFIED,
    EventType.ORDER_NOTIFICATION_FAILED,
}


async def handle_event(envelope: EventEnvelope, source_topic: str) -> None:
    if envelope.event_type not in HANDLED:
        return

    async with SessionLocal() as session:
        status = await OrderService(session).apply_notification_result(
            envelope
        )

    logger.info(
        'orders.saga_event_processed',
        event_id=str(envelope.event_id),
        event_type=str(envelope.event_type),
        saga_id=str(envelope.saga_id),
        result=status.value if status is not None else 'skipped',
    )


def build_consumer() -> EventConsumer:
    return EventConsumer(
        service='orders-saga',
        topics=TOPICS,
        group_id=settings.KAFKA_GROUP_ID,
        settings=settings,
        handler=handle_event,
        skip_producers=(Producer.ORDERS,),
    )


async def consume(stop_event: asyncio.Event | None = None) -> None:
    await build_consumer().run(stop_event)


def main() -> None:
    setup_logging('orders-saga-consumer', level=settings.LOG_LEVEL)
    try:
        asyncio.run(run_consumer(build_consumer()))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
