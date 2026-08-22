import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator
from uuid import uuid4

import structlog
from aiokafka import AIOKafkaProducer
from orders.core.db import get_session
from orders.models import OutboxEvent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.getLogger('orders.outbox_worker')


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if not val:
        return default
    return int(val)


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if not val:
        return default
    return float(val)


def _env_str(name: str, default: str) -> str:
    val = os.getenv(name)
    if not val:
        return default
    return val


def _build_backoff_seconds(attempts: int, base: float, cap: float) -> float:
    """Экспоненциальная задержка с ограничением сверху.

    Формула: base * 2^(attempts - 1)
    attempts начинается с 1.
    """
    if attempts <= 0:
        return base
    return min(cap, base * (2 ** (attempts - 1)))


@asynccontextmanager
async def _get_async_session() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


async def _publish_to_kafka(
    *,
    topic: str,
    key: str,
    value: dict[str, Any],
    bootstrap_servers: str,
    request_timeout_ms: int,
) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        request_timeout_ms=request_timeout_ms,
        acks='all',
        enable_idempotence=True,
    )

    await producer.start()
    try:
        await producer.send_and_wait(
            topic,
            json.dumps(value, ensure_ascii=False).encode('utf-8'),
            key=key.encode('utf-8'),
        )
    finally:
        await producer.stop()


async def run_outbox_loop() -> None:
    """Основной цикл воркера."""

    # Настройки
    poll_interval_sec = _env_float('OUTBOX_POLL_INTERVAL_SEC', 1.0)
    batch_size = _env_int('OUTBOX_BATCH_SIZE', 50)

    bootstrap = _env_str('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = _env_str('KAFKA_TOPIC_ORDER_CREATED', 'OrderCreated')
    request_timeout_ms = _env_int('KAFKA_REQUEST_TIMEOUT_MS', 10_000)

    backoff_base = _env_float('OUTBOX_BACKOFF_BASE_SEC', 1.0)
    backoff_cap = _env_float('OUTBOX_BACKOFF_CAP_SEC', 60.0)

    def status_new():
        return getattr(getattr(OutboxEvent, 'Status', None), 'NEW', 'NEW')

    def status_error():
        return getattr(getattr(OutboxEvent, 'Status', None), 'ERROR', 'ERROR')

    def status_sent():
        return getattr(getattr(OutboxEvent, 'Status', None), 'SENT', 'SENT')

    logger.info(
        'Outbox worker запущен (topic=%s, bootstrap=%s, batch=%d)',
        topic,
        bootstrap,
        batch_size,
    )

    while True:
        try:
            async with _get_async_session() as session:
                await _process_batch(
                    session=session,
                    batch_size=batch_size,
                    bootstrap=bootstrap,
                    topic=topic,
                    request_timeout_ms=request_timeout_ms,
                    status_new=status_new(),
                    status_error=status_error(),
                    status_sent=status_sent(),
                    backoff_base=backoff_base,
                    backoff_cap=backoff_cap,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception('Ошибка в цикле outbox worker: %s', exc)

        await asyncio.sleep(poll_interval_sec)


async def _process_batch(
    *,
    session: AsyncSession,
    batch_size: int,
    bootstrap: str,
    topic: str,
    request_timeout_ms: int,
    status_new: Any,
    status_error: Any,
    status_sent: Any,
    backoff_base: float,
    backoff_cap: float,
) -> None:
    """Забирает пачку событий (SKIP LOCKED) и публикует их."""

    now = _utcnow()

    # Ожидается, что OutboxEvent имеет поля:
    # id, aggregate_id, aggregate_type, event_type, payload, status,
    # attempts, last_error, next_retry_at, created_at, sent_at
    stmt = (
        select(OutboxEvent)
        .where(
            (OutboxEvent.status == status_new)
            | (
                (OutboxEvent.status == status_error)
                & (
                    (OutboxEvent.next_retry_at.is_(None))
                    | (OutboxEvent.next_retry_at <= now)
                )
            )
        )
        .order_by(OutboxEvent.created_at.asc())
        .with_for_update(skip_locked=True)
        .limit(batch_size)
    )

    result = await session.execute(stmt)
    events = list(result.scalars().all())

    if not events:
        return

    logger.info('Взято событий из outbox: %d', len(events))

    for ev in events:
        attempts = int(getattr(ev, 'attempts', 0) or 0)

        try:
            payload: dict[str, Any] = ev.payload or {}
            user_id = (
                payload.get('user_id') if isinstance(payload, dict) else None
            )

            # event_id обязателен для идемпотентности consumer'а.
            # Для старых строк, где event_id ещё не заполнен, генерируем и сохраняем.
            ev_event_id = getattr(ev, 'event_id', None)
            if not ev_event_id:
                ev_event_id = str(uuid4())
                if hasattr(ev, 'event_id'):
                    ev.event_id = ev_event_id

            event_value = {
                'event_id': str(ev_event_id),
                'event_type': ev.event_type,
                'order_id': ev.aggregate_id,
                'user_id': user_id,
                'payload': payload,
                # Оставляем служебные поля для дебага/совместимости.
                'id': ev.id,
                'aggregate_id': ev.aggregate_id,
                'aggregate_type': ev.aggregate_type,
                'created_at': ev.created_at.isoformat()
                if ev.created_at
                else None,
            }

            logger.info('Публикуем событие в Kafka', event_value=event_value)

            await _publish_to_kafka(
                topic=topic,
                key=str(ev.aggregate_id),
                value=event_value,
                bootstrap_servers=bootstrap,
                request_timeout_ms=request_timeout_ms,
            )

            ev.status = status_sent
            ev.sent_at = _utcnow()
            ev.last_error = None
            ev.next_retry_at = None

            logger.info(
                'Событие отправлено: outbox_id=%s, type=%s',
                ev.id,
                ev.event_type,
            )

        except Exception as e:
            attempts += 1
            ev.status = status_error
            ev.attempts = attempts
            ev.last_error = f'{type(e).__name__}: {e}'

            delay = _build_backoff_seconds(attempts, backoff_base, backoff_cap)
            ev.next_retry_at = (
                _utcnow()
                if delay <= 0
                else _utcnow() + timedelta(seconds=delay)
            )

            logger.exception(
                'Ошибка отправки outbox_id=%s (attempt=%d)',
                ev.id,
                attempts,
            )

        await session.flush()

    await session.commit()


def main() -> None:
    try:
        asyncio.run(run_outbox_loop())
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
