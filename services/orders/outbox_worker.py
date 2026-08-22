import asyncio
import json
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

import structlog
from aiokafka import AIOKafkaProducer
from orders.core.db import get_session
from orders.models import OutboxEvent, OutboxStatus
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger('orders.outbox_worker')


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _env_str(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val else default


def _build_backoff_seconds(attempts: int, base: float, cap: float) -> float:
    """Экспоненциальная задержка с ограничением сверху.

    Формула: base * 2^(attempts - 1), attempts начинается с 1.
    """
    if attempts <= 0:
        return base
    return min(cap, base * (2 ** (attempts - 1)))


@asynccontextmanager
async def _get_async_session() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


def _build_event_value(ev: OutboxEvent) -> dict[str, Any]:
    """Формирует сообщение для Kafka из строки outbox."""
    payload: dict[str, Any] = ev.payload or {}
    return {
        'event_id': ev.event_id,
        'event_type': ev.event_type,
        'order_id': ev.aggregate_id,
        'user_id': payload.get('user_id'),
        'payload': payload,
        # Служебные поля для отладки и трассировки.
        'id': ev.id,
        'aggregate_id': ev.aggregate_id,
        'aggregate_type': ev.aggregate_type,
        'created_at': ev.created_at.isoformat() if ev.created_at else None,
    }


async def _publish(
    producer: AIOKafkaProducer,
    *,
    topic: str,
    key: str,
    value: dict[str, Any],
) -> None:
    await producer.send_and_wait(
        topic,
        json.dumps(value, ensure_ascii=False).encode('utf-8'),
        key=key.encode('utf-8'),
    )


async def _process_batch(
    *,
    session: AsyncSession,
    producer: AIOKafkaProducer,
    batch_size: int,
    topic: str,
    backoff_base: float,
    backoff_cap: float,
) -> None:
    """Забирает пачку событий (SKIP LOCKED) и публикует их."""
    now = _utcnow()
    stmt = (
        select(OutboxEvent)
        .where(
            (OutboxEvent.status == OutboxStatus.NEW)
            | (
                (OutboxEvent.status == OutboxStatus.ERROR)
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

    logger.info('outbox.batch_taken', count=len(events))

    for ev in events:
        try:
            await _publish(
                producer,
                topic=topic,
                key=str(ev.aggregate_id),
                value=_build_event_value(ev),
            )
        except Exception as exc:
            attempts = int(ev.attempts or 0) + 1
            delay = _build_backoff_seconds(attempts, backoff_base, backoff_cap)

            ev.status = OutboxStatus.ERROR
            ev.attempts = attempts
            ev.last_error = f'{type(exc).__name__}: {exc}'
            ev.next_retry_at = _utcnow() + timedelta(seconds=max(delay, 0))

            logger.exception(
                'outbox.publish_failed',
                outbox_id=ev.id,
                event_type=ev.event_type,
                attempts=attempts,
                retry_in_sec=delay,
            )
        else:
            ev.status = OutboxStatus.SENT
            ev.sent_at = _utcnow()
            ev.last_error = None
            ev.next_retry_at = None

            logger.info(
                'outbox.published',
                outbox_id=ev.id,
                event_id=ev.event_id,
                event_type=ev.event_type,
                topic=topic,
            )

        await session.flush()

    await session.commit()


async def run_outbox_loop(stop_event: asyncio.Event | None = None) -> None:
    """Основной цикл воркера: один продюсер на всё время работы."""
    stop_event = stop_event or asyncio.Event()

    poll_interval_sec = _env_float('OUTBOX_POLL_INTERVAL_SEC', 1.0)
    batch_size = _env_int('OUTBOX_BATCH_SIZE', 50)
    bootstrap = _env_str('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = _env_str('KAFKA_TOPIC_ORDER_CREATED', 'OrderCreated')
    request_timeout_ms = _env_int('KAFKA_REQUEST_TIMEOUT_MS', 10_000)
    backoff_base = _env_float('OUTBOX_BACKOFF_BASE_SEC', 1.0)
    backoff_cap = _env_float('OUTBOX_BACKOFF_CAP_SEC', 60.0)

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        request_timeout_ms=request_timeout_ms,
        acks='all',
        enable_idempotence=True,
    )
    await producer.start()

    logger.info(
        'outbox.worker_started',
        topic=topic,
        bootstrap=bootstrap,
        batch_size=batch_size,
    )

    try:
        while not stop_event.is_set():
            try:
                async with _get_async_session() as session:
                    await _process_batch(
                        session=session,
                        producer=producer,
                        batch_size=batch_size,
                        topic=topic,
                        backoff_base=backoff_base,
                        backoff_cap=backoff_cap,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('outbox.loop_error')

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=poll_interval_sec
                )
            except asyncio.TimeoutError:
                continue
    finally:
        await producer.stop()
        logger.info('outbox.worker_stopped')


async def _amain() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await run_outbox_loop(stop_event)


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
