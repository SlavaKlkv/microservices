"""Поведение общего консьюмера: DLQ, повторы, дубли, свои события.

Здесь проверяется самая неприятная часть системы — та, что решает, что
делать с сообщением, которое не получается обработать. Ошибка в этой
логике либо теряет события, либо намертво заклинивает партицию.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from ms_events import EventEnvelope, EventType, Producer, Topic
from ms_events.consumer import EventConsumer
from ms_events.settings import KafkaSettings, RedisSettings


class FakeProducer:
    """Продюсер, который только запоминает отправленное."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, str | None]] = []

    async def send_raw(
        self, topic: str, value: bytes, *, key: str | None = None
    ) -> None:
        self.sent.append((topic, value, key))


class FakeMessage:
    def __init__(
        self, topic: str, value: bytes, key: bytes | None = None
    ) -> None:
        self.topic = topic
        self.value = value
        self.key = key


def make_envelope(
    producer: Producer = Producer.NOTIFICATION,
    event_type: EventType = EventType.ORDER_NOTIFIED,
) -> EventEnvelope:
    return EventEnvelope(
        event_type=event_type,
        saga_id=uuid4(),
        producer=producer,
        aggregate_id='42',
        payload={'order_id': 42, 'user_id': 7},
    )


def build_consumer(
    handler: Any,
    *,
    max_attempts: int = 3,
    skip_producers: tuple[Producer, ...] = (),
) -> EventConsumer:
    return EventConsumer(
        service='test-consumer',
        topics=(Topic.NOTIFICATIONS,),
        group_id='test-group',
        settings=KafkaSettings(
            CONSUMER_MAX_ATTEMPTS=max_attempts,
            # Повторы в тестах должны быть мгновенными.
            CONSUMER_BACKOFF_BASE_SEC=0.0,
            CONSUMER_BACKOFF_CAP_SEC=0.0,
        ),
        handler=handler,
        skip_producers=skip_producers,
    )


async def test_good_message_reaches_handler() -> None:
    seen: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        seen.append(envelope)

    envelope = make_envelope()
    producer = FakeProducer()

    await build_consumer(handler)._process(
        producer,
        FakeMessage(str(Topic.NOTIFICATIONS), envelope.to_json()),
    )

    assert [e.event_id for e in seen] == [envelope.event_id]
    assert producer.sent == []


async def test_unparsable_message_goes_straight_to_dlq() -> None:
    # «Отравленная таблетка»: повторы бессмысленны, а без DLQ консьюмер
    # зациклится на ней навсегда.
    calls = 0

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        nonlocal calls
        calls += 1

    producer = FakeProducer()

    await build_consumer(handler)._process(
        producer,
        FakeMessage(str(Topic.NOTIFICATIONS), b'{"broken": true}'),
    )

    assert calls == 0
    assert len(producer.sent) == 1
    assert producer.sent[0][0] == str(Topic.NOTIFICATIONS_DLQ)


async def test_handler_failure_is_retried_then_dlq() -> None:
    attempts = 0

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError('database is down')

    producer = FakeProducer()

    await build_consumer(handler, max_attempts=3)._process(
        producer,
        FakeMessage(str(Topic.NOTIFICATIONS), make_envelope().to_json()),
    )

    assert attempts == 3
    assert len(producer.sent) == 1
    assert producer.sent[0][0] == str(Topic.NOTIFICATIONS_DLQ)


async def test_transient_failure_recovers_without_dlq() -> None:
    attempts = 0

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError('temporary')

    producer = FakeProducer()

    await build_consumer(handler, max_attempts=3)._process(
        producer,
        FakeMessage(str(Topic.NOTIFICATIONS), make_envelope().to_json()),
    )

    assert attempts == 2
    assert producer.sent == []


async def test_own_events_are_skipped() -> None:
    # Сервис не должен реагировать на то, что сам же и опубликовал:
    # подписка на собственный топик замкнула бы сагу в цикл.
    seen: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        seen.append(envelope)

    envelope = make_envelope(producer=Producer.ORDERS)
    producer = FakeProducer()

    consumer = build_consumer(handler, skip_producers=(Producer.ORDERS,))
    await consumer._process(
        producer, FakeMessage(str(Topic.NOTIFICATIONS), envelope.to_json())
    )

    assert seen == []
    assert producer.sent == []


async def test_foreign_events_still_pass_the_filter() -> None:
    seen: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        seen.append(envelope)

    consumer = build_consumer(handler, skip_producers=(Producer.ORDERS,))
    await consumer._process(
        FakeProducer(),
        FakeMessage(
            str(Topic.NOTIFICATIONS),
            make_envelope(producer=Producer.NOTIFICATION).to_json(),
        ),
    )

    assert len(seen) == 1


async def test_duplicate_is_dropped_by_fast_path() -> None:
    seen: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        seen.append(envelope)

    consumer = EventConsumer(
        service='test-consumer',
        topics=(Topic.NOTIFICATIONS,),
        group_id='test-group',
        settings=KafkaSettings(),
        handler=handler,
        redis_settings=RedisSettings(REDIS_ENABLED=True),
    )
    consumer._idempotency._client = _AlwaysTakenRedis()  # type: ignore[union-attr]

    await consumer._process(
        FakeProducer(),
        FakeMessage(str(Topic.NOTIFICATIONS), make_envelope().to_json()),
    )

    assert seen == []


class _AlwaysTakenRedis:
    async def set(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def delete(self, *args: Any) -> int:
        return 0


async def test_failed_handler_releases_the_claim() -> None:
    # Иначе повтор события будет отброшен как дубль, а в Postgres так
    # ничего и не появится — событие потеряно молча.
    released: list[str] = []

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        raise RuntimeError('boom')

    consumer = build_consumer(handler, max_attempts=1)

    class TrackingCache:
        async def claim(self, event_id: str) -> bool:
            return True

        async def release(self, event_id: str) -> None:
            released.append(event_id)

    consumer._idempotency = TrackingCache()  # type: ignore[assignment]

    envelope = make_envelope()
    await consumer._process(
        FakeProducer(),
        FakeMessage(str(Topic.NOTIFICATIONS), envelope.to_json()),
    )

    assert released == [str(envelope.event_id)]


@pytest.mark.parametrize(
    ('source', 'expected'),
    [
        (str(Topic.ORDERS), str(Topic.ORDERS_DLQ)),
        (str(Topic.NOTIFICATIONS), str(Topic.NOTIFICATIONS_DLQ)),
        (str(Topic.ORDERS_DLQ), str(Topic.ORDERS_DLQ)),
    ],
)
def test_dlq_topic_resolution(source: str, expected: str) -> None:
    assert EventConsumer._dlq_topic(source) == expected


def test_unknown_topic_has_no_dlq() -> None:
    assert EventConsumer._dlq_topic('some.random.topic') is None
