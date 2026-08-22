from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import structlog
from aiokafka import AIOKafkaConsumer
from orders_history.core.db import get_session
from orders_history.schemas import HistoryEventIn
from orders_history.service import HistoryService
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value else default


KAFKA_BOOTSTRAP_SERVERS = _env('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = _env('KAFKA_TOPIC_ORDER_CREATED', 'OrderCreated')
KAFKA_GROUP_ID = _env('KAFKA_GROUP_ID', 'orders-history')


@asynccontextmanager
async def _get_async_session() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


async def handle_message(session: AsyncSession, raw: dict[str, Any]) -> None:
    """
    Обрабатывает одно сообщение из Kafka и пишет его в историю заказов.
    """
    event = HistoryEventIn(
        event_id=raw['event_id'],
        order_id=raw['order_id'],
        user_id=raw.get('user_id'),
        event_type=raw['event_type'],
        payload=raw.get('payload', {}),
    )

    service = HistoryService(session)
    result = await service.record_event(event)

    if result is None:
        logger.info(
            'history_event_skipped',
            event_id=event.event_id,
            reason='already_processed',
        )
    else:
        logger.info(
            'history_event_recorded',
            event_id=event.event_id,
            event_type=event.event_type,
            order_id=event.order_id,
        )


async def consume() -> None:
    """
    Kafka consumer для сервиса истории заказов.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    await consumer.start()
    logger.info(
        'orders_history_consumer_started',
        topic=KAFKA_TOPIC,
        bootstrap=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
    )

    try:
        async for msg in consumer:
            logger.debug(
                'kafka_message_received',
                partition=msg.partition,
                offset=msg.offset,
                key=msg.key.decode('utf-8') if msg.key else None,
            )

            async with _get_async_session() as session:
                try:
                    await handle_message(session, msg.value)
                    await consumer.commit()
                except Exception:
                    logger.exception(
                        'history_event_processing_failed',
                        message=msg.value,
                    )
                    # commit НЕ делаем — сообщение будет прочитано повторно
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())
