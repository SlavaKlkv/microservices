"""Общий Kafka-консьюмер с DLQ и ограниченными повторами.

Правила одинаковы во всех сервисах-подписчиках:

* сообщение, которое не разбирается в ``EventEnvelope``, — «отравленная
  таблетка»: повторы бессмысленны, оно сразу уходит в DLQ, а офсет
  коммитится, иначе консьюмер зациклится на нём навсегда;
* транзиентная ошибка обработки (недоступна БД и т.п.) повторяется с
  экспоненциальной задержкой до ``CONSUMER_MAX_ATTEMPTS``, после чего
  событие тоже уходит в DLQ, чтобы не блокировать партицию.
"""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Awaitable, Callable, Sequence

import structlog
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from ms_events.envelope import EventEnvelope
from ms_events.idempotency import IdempotencyCache
from ms_events.metrics import (
    CONSUMER_DLQ,
    CONSUMER_EVENTS,
    CONSUMER_HANDLER_DURATION,
)
from ms_events.producer import EventProducer, RawSender
from ms_events.retry import backoff_seconds
from ms_events.settings import KafkaSettings, RedisSettings
from ms_events.types import Producer, Topic

logger = structlog.get_logger('ms_events.consumer')

#: Обработчик одного события: получает конверт и топик-источник.
Handler = Callable[[EventEnvelope, str], Awaitable[None]]


class EventConsumer:
    """Читает топики и передаёт разобранные конверты в обработчик."""

    def __init__(
        self,
        *,
        service: str,
        topics: Sequence[Topic],
        group_id: str,
        settings: KafkaSettings,
        handler: Handler,
        skip_producers: Sequence[Producer] = (),
        redis_settings: RedisSettings | None = None,
    ) -> None:
        self._service = service
        self._topics = list(topics)
        self._group_id = group_id
        self._settings = settings
        self._handler = handler
        #: Собственные события пропускаем: подписка сервиса на свой же
        #: топик замкнула бы сагу в бесконечный цикл.
        self._skip_producers = {str(p) for p in skip_producers}
        #: Быстрый путь идемпотентности; источник истины — Postgres.
        self._idempotency = (
            IdempotencyCache(redis_settings, namespace=group_id)
            if redis_settings is not None
            else None
        )

    def _count(self, *, event_type: str, topic: str, outcome: str) -> None:
        CONSUMER_EVENTS.labels(
            service=self._service,
            event_type=event_type,
            topic=topic,
            outcome=outcome,
        ).inc()

    @staticmethod
    def _dlq_topic(source_topic: str) -> str | None:
        try:
            return str(Topic(source_topic).dlq)
        except ValueError:
            logger.error('consumer.unknown_topic', topic=source_topic)
            return None

    async def _to_dlq(
        self,
        producer: RawSender,
        *,
        source_topic: str,
        value: bytes,
        key: bytes | None,
        reason: str,
    ) -> None:
        dlq = self._dlq_topic(source_topic)
        if dlq is None:
            return
        try:
            await producer.send_raw(
                dlq,
                value,
                key=key.decode('utf-8') if key else None,
            )
        except Exception:
            logger.exception('consumer.dlq_publish_failed', topic=dlq)
        else:
            CONSUMER_DLQ.labels(
                service=self._service, topic=dlq, reason=reason
            ).inc()
            logger.warning(
                'consumer.moved_to_dlq',
                topic=dlq,
                source_topic=source_topic,
                reason=reason,
            )

    async def _handle_with_retries(
        self,
        producer: RawSender,
        envelope: EventEnvelope,
        *,
        source_topic: str,
        raw_value: bytes,
        key: bytes | None,
    ) -> None:
        max_attempts = max(1, self._settings.CONSUMER_MAX_ATTEMPTS)

        event_id = str(envelope.event_id)
        event_type = str(envelope.event_type)
        if self._idempotency is not None and not await self._idempotency.claim(
            event_id
        ):
            self._count(
                event_type=event_type,
                topic=source_topic,
                outcome='duplicate',
            )
            logger.debug('consumer.duplicate_skipped', event_id=event_id)
            return

        for attempt in range(1, max_attempts + 1):
            started = time.perf_counter()
            try:
                await self._handler(envelope, source_topic)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    'consumer.handler_failed',
                    event_id=str(envelope.event_id),
                    event_type=str(envelope.event_type),
                    attempt=attempt,
                )
                self._count(
                    event_type=event_type,
                    topic=source_topic,
                    outcome='failed',
                )
                # Заявку снимаем: в Postgres обработка не закоммитилась,
                # иначе повтор был бы отброшен как «дубль».
                if self._idempotency is not None:
                    await self._idempotency.release(event_id)
                if attempt >= max_attempts:
                    await self._to_dlq(
                        producer,
                        source_topic=source_topic,
                        value=raw_value,
                        key=key,
                        reason='max_attempts_exhausted',
                    )
                    return
                await asyncio.sleep(
                    backoff_seconds(
                        attempt,
                        self._settings.CONSUMER_BACKOFF_BASE_SEC,
                        self._settings.CONSUMER_BACKOFF_CAP_SEC,
                    )
                )
            else:
                CONSUMER_HANDLER_DURATION.labels(
                    service=self._service, event_type=event_type
                ).observe(time.perf_counter() - started)
                self._count(
                    event_type=event_type,
                    topic=source_topic,
                    outcome='processed',
                )
                return

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        settings = self._settings

        consumer = AIOKafkaConsumer(
            *[str(t) for t in self._topics],
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=f'{settings.KAFKA_CLIENT_ID}-consumer',
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
        )
        producer = EventProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=f'{settings.KAFKA_CLIENT_ID}-dlq',
            request_timeout_ms=settings.KAFKA_REQUEST_TIMEOUT_MS,
        )

        await consumer.start()
        await producer.start()
        if self._idempotency is not None:
            await self._idempotency.start()
        logger.info(
            'consumer.started',
            service=self._service,
            topics=[str(t) for t in self._topics],
            group_id=self._group_id,
        )

        try:
            while not stop_event.is_set():
                batch = await consumer.getmany(timeout_ms=1000, max_records=50)
                for records in batch.values():
                    for msg in records:
                        await self._process(producer, msg)
                        await consumer.commit()
                    if stop_event.is_set():
                        break
        finally:
            if self._idempotency is not None:
                await self._idempotency.stop()
            await producer.stop()
            await consumer.stop()
            logger.info('consumer.stopped', service=self._service)

    async def _process(self, producer: RawSender, msg: object) -> None:
        raw_value: bytes = getattr(msg, 'value', b'') or b''
        key: bytes | None = getattr(msg, 'key', None)
        source_topic: str = str(getattr(msg, 'topic', ''))

        try:
            envelope = EventEnvelope.from_json(raw_value)
        except (ValidationError, ValueError):
            logger.exception(
                'consumer.invalid_envelope', source_topic=source_topic
            )
            self._count(
                event_type='unknown',
                topic=source_topic,
                outcome='invalid',
            )
            await self._to_dlq(
                producer,
                source_topic=source_topic,
                value=raw_value,
                key=key,
                reason='invalid_envelope',
            )
            return

        if str(envelope.producer) in self._skip_producers:
            self._count(
                event_type=str(envelope.event_type),
                topic=source_topic,
                outcome='skipped_own',
            )
            logger.debug(
                'consumer.skipped_own_event',
                event_id=str(envelope.event_id),
                producer=str(envelope.producer),
            )
            return

        await self._handle_with_retries(
            producer,
            envelope,
            source_topic=source_topic,
            raw_value=raw_value,
            key=key,
        )


async def run_consumer(consumer: EventConsumer) -> None:
    """Запускает консьюмер с обработкой SIGINT/SIGTERM."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    await consumer.run(stop_event)
