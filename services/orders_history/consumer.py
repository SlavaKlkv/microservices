from __future__ import annotations

import asyncio
import json
import signal
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import structlog
from aiokafka import AIOKafkaConsumer
from ms_events import Topic
from sqlalchemy.ext.asyncio import AsyncSession

from orders_history.core.db import get_session
from orders_history.schemas import HistoryEventIn
from orders_history.service import HistoryService
from orders_history.settings import settings

logger = structlog.get_logger(__name__)


KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = str(Topic.ORDERS)
KAFKA_GROUP_ID = settings.KAFKA_GROUP_ID


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
        user_id=int(raw['user_id']),
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


async def consume(stop_event: asyncio.Event | None = None) -> None:
    """
    Kafka consumer для сервиса истории заказов.
    """
    stop_event = stop_event or asyncio.Event()
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
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
            if stop_event.is_set():
                break

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
        logger.info('orders_history_consumer_stopped')


async def _amain() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await consume(stop_event)


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
