"""Общий outbox-воркер: читает таблицу outbox и публикует события.

Воркер одинаков для всех сервисов-издателей, отличается только моделью
и фабрикой сессий, поэтому живёт в общем пакете.
"""

from __future__ import annotations

import asyncio
import signal
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ms_events.db import OutboxStatus
from ms_events.envelope import utcnow
from ms_events.metrics import (
    OUTBOX_BATCH,
    OUTBOX_DEAD,
    OUTBOX_FAILED,
    OUTBOX_PRUNED,
    OUTBOX_PUBLISHED,
)
from ms_events.producer import EventProducer
from ms_events.retry import backoff_seconds
from ms_events.settings import KafkaSettings
from ms_events.types import Topic

logger = structlog.get_logger('ms_events.outbox')


class OutboxWorker:
    """Публикует накопленные в outbox события в Kafka.

    Строки берутся пачкой с ``FOR UPDATE SKIP LOCKED``, поэтому несколько
    экземпляров воркера могут работать параллельно, не мешая друг другу.
    """

    def __init__(
        self,
        *,
        service: str,
        model: type[Any],
        session_factory: async_sessionmaker[AsyncSession],
        settings: KafkaSettings,
        processed_model: type[Any] | None = None,
    ) -> None:
        self._service = service
        self._model = model
        self._processed_model = processed_model
        self._session_factory = session_factory
        self._settings = settings

    async def _publish_to_dlq(self, producer: EventProducer, row: Any) -> None:
        """Отправляет безнадёжное событие в парный DLQ-топик."""
        try:
            dlq_topic = Topic(row.topic).dlq
        except ValueError:
            logger.error('outbox.unknown_topic', topic=row.topic)
            return

        try:
            await producer.send(str(dlq_topic), row.to_envelope())
        except Exception:
            logger.exception('outbox.dlq_publish_failed', outbox_id=row.id)
        else:
            OUTBOX_DEAD.labels(
                service=self._service, event_type=row.event_type
            ).inc()
            logger.warning(
                'outbox.moved_to_dlq',
                outbox_id=row.id,
                event_id=row.event_id,
                event_type=row.event_type,
                topic=str(dlq_topic),
            )

    async def _process_row(self, producer: EventProducer, row: Any) -> None:
        try:
            await producer.send(row.topic, row.to_envelope())
        except Exception as exc:
            attempts = int(row.attempts or 0) + 1
            row.attempts = attempts
            row.last_error = f'{type(exc).__name__}: {exc}'
            OUTBOX_FAILED.labels(
                service=self._service, event_type=row.event_type
            ).inc()

            if attempts >= int(row.max_attempts):
                row.status = OutboxStatus.DEAD
                row.next_retry_at = None
                logger.exception(
                    'outbox.attempts_exhausted',
                    outbox_id=row.id,
                    event_type=row.event_type,
                    attempts=attempts,
                )
                await self._publish_to_dlq(producer, row)
                return

            delay = backoff_seconds(
                attempts,
                self._settings.OUTBOX_BACKOFF_BASE_SEC,
                self._settings.OUTBOX_BACKOFF_CAP_SEC,
            )
            row.status = OutboxStatus.ERROR
            row.next_retry_at = utcnow() + timedelta(seconds=max(delay, 0.0))
            logger.exception(
                'outbox.publish_failed',
                outbox_id=row.id,
                event_type=row.event_type,
                attempts=attempts,
                retry_in_sec=delay,
            )
        else:
            row.status = OutboxStatus.SENT
            row.sent_at = utcnow()
            row.last_error = None
            row.next_retry_at = None
            OUTBOX_PUBLISHED.labels(
                service=self._service,
                event_type=row.event_type,
                topic=row.topic,
            ).inc()
            logger.info(
                'outbox.published',
                outbox_id=row.id,
                event_id=row.event_id,
                event_type=row.event_type,
                topic=row.topic,
            )

    async def _process_batch(
        self, session: AsyncSession, producer: EventProducer
    ) -> int:
        now = utcnow()
        model = self._model
        stmt = (
            select(model)
            .where(
                (model.status == OutboxStatus.NEW)
                | (
                    (model.status == OutboxStatus.ERROR)
                    & (
                        model.next_retry_at.is_(None)
                        | (model.next_retry_at <= now)
                    )
                )
            )
            .order_by(model.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(self._settings.OUTBOX_BATCH_SIZE)
        )

        rows = list((await session.execute(stmt)).scalars().all())
        OUTBOX_BATCH.labels(service=self._service).set(len(rows))
        if not rows:
            return 0

        logger.debug('outbox.batch_taken', count=len(rows))
        for row in rows:
            await self._process_row(producer, row)
            await session.flush()

        await session.commit()
        return len(rows)

    async def _prune(
        self,
        session: AsyncSession,
        model: type[Any],
        column: Any,
        days: int,
        *,
        table: str,
        extra_where: Any = None,
    ) -> int:
        """Удаляет строки старше ``days`` и возвращает их число."""
        if days <= 0:
            return 0

        stmt = delete(model).where(column < utcnow() - timedelta(days=days))
        if extra_where is not None:
            stmt = stmt.where(extra_where)

        result = await session.execute(stmt)
        deleted = int(getattr(result, 'rowcount', 0) or 0)

        if deleted:
            OUTBOX_PRUNED.labels(service=self._service, table=table).inc(
                deleted
            )
            logger.info(
                'outbox.pruned', table=table, deleted=deleted, older_than=days
            )
        return deleted

    async def cleanup(self, session: AsyncSession) -> int:
        """Убирает то, что уже сделало свою работу.

        Отправленные строки outbox — след публикации, отметки в
        ``processed_event`` — защита от повторной доставки. И то, и другое
        нужно ограниченное время; без уборки обе таблицы растут вечно.

        Строки в статусе ``DEAD`` не трогаются: это единственный след
        события, ушедшего в DLQ, и разбирают его руками.
        """
        model = self._model
        deleted = await self._prune(
            session,
            model,
            model.sent_at,
            self._settings.OUTBOX_RETENTION_DAYS,
            table='outbox',
            extra_where=model.status == OutboxStatus.SENT,
        )

        if self._processed_model is not None:
            processed = self._processed_model
            deleted += await self._prune(
                session,
                processed,
                processed.processed_at,
                self._settings.PROCESSED_EVENT_RETENTION_DAYS,
                table='processed_event',
            )

        await session.commit()
        return deleted

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Основной цикл: один продюсер на всё время жизни воркера."""
        stop_event = stop_event or asyncio.Event()
        settings = self._settings

        producer = EventProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=f'{settings.KAFKA_CLIENT_ID}-outbox',
            request_timeout_ms=settings.KAFKA_REQUEST_TIMEOUT_MS,
        )
        await producer.start()
        logger.info(
            'outbox.worker_started',
            service=self._service,
            bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS,
            batch_size=settings.OUTBOX_BATCH_SIZE,
        )

        # первая уборка — не на старте: воркер должен сначала начать
        # публиковать, а не разгребать историю
        next_cleanup = utcnow() + timedelta(
            seconds=settings.OUTBOX_CLEANUP_INTERVAL_SEC
        )

        try:
            while not stop_event.is_set():
                try:
                    async with self._session_factory() as session:
                        await self._process_batch(session, producer)

                        if utcnow() >= next_cleanup:
                            await self.cleanup(session)
                            next_cleanup = utcnow() + timedelta(
                                seconds=settings.OUTBOX_CLEANUP_INTERVAL_SEC
                            )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception('outbox.loop_error')

                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=settings.OUTBOX_POLL_INTERVAL_SEC,
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            await producer.stop()
            logger.info('outbox.worker_stopped', service=self._service)


async def run_worker(worker: OutboxWorker) -> None:
    """Запускает воркер с корректной обработкой SIGINT/SIGTERM."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    await worker.run(stop_event)
