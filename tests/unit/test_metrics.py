"""Метрики консьюмера: без них асинхронная часть системы не видна.

Проверяем не сами счётчики, а то, что консьюмер размечает исходы —
именно по метке outcome в проде отличают «дубль» от «упало».
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from prometheus_client import REGISTRY

from ms_events import EventEnvelope, EventType, Producer, Topic
from ms_events.consumer import EventConsumer
from ms_events.settings import KafkaSettings
from tests.unit.test_consumer import FakeMessage, FakeProducer

SERVICE = 'metrics-test'


def counter(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return 0.0 if value is None else value


def events_total(outcome: str, event_type: str) -> float:
    return counter(
        'consumer_events_total',
        service=SERVICE,
        event_type=event_type,
        topic=str(Topic.NOTIFICATIONS),
        outcome=outcome,
    )


def make_consumer(handler: Any, *, max_attempts: int = 1) -> EventConsumer:
    return EventConsumer(
        service=SERVICE,
        topics=(Topic.NOTIFICATIONS,),
        group_id='metrics-group',
        settings=KafkaSettings(
            CONSUMER_MAX_ATTEMPTS=max_attempts,
            CONSUMER_BACKOFF_BASE_SEC=0.0,
            CONSUMER_BACKOFF_CAP_SEC=0.0,
        ),
        handler=handler,
    )


def make_envelope(event_type: EventType) -> EventEnvelope:
    return EventEnvelope(
        event_type=event_type,
        saga_id=uuid4(),
        producer=Producer.NOTIFICATION,
        aggregate_id='42',
        payload={'order_id': 42},
    )


async def test_processed_event_is_counted() -> None:
    event_type = str(EventType.ORDER_NOTIFIED)
    before = events_total('processed', event_type)

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        return None

    await make_consumer(handler)._process(
        FakeProducer(),
        FakeMessage(
            str(Topic.NOTIFICATIONS),
            make_envelope(EventType.ORDER_NOTIFIED).to_json(),
        ),
    )

    assert events_total('processed', event_type) == before + 1


async def test_failed_event_is_counted_and_lands_in_dlq() -> None:
    event_type = str(EventType.ORDER_NOTIFICATION_FAILED)
    before_failed = events_total('failed', event_type)
    before_dlq = counter(
        'consumer_dlq_total',
        service=SERVICE,
        topic=str(Topic.NOTIFICATIONS_DLQ),
        reason='max_attempts_exhausted',
    )

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        raise RuntimeError('boom')

    await make_consumer(handler)._process(
        FakeProducer(),
        FakeMessage(
            str(Topic.NOTIFICATIONS),
            make_envelope(EventType.ORDER_NOTIFICATION_FAILED).to_json(),
        ),
    )

    assert events_total('failed', event_type) == before_failed + 1
    assert (
        counter(
            'consumer_dlq_total',
            service=SERVICE,
            topic=str(Topic.NOTIFICATIONS_DLQ),
            reason='max_attempts_exhausted',
        )
        == before_dlq + 1
    )


async def test_invalid_envelope_is_counted_separately() -> None:
    before = events_total('invalid', 'unknown')

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        return None

    await make_consumer(handler)._process(
        FakeProducer(),
        FakeMessage(str(Topic.NOTIFICATIONS), b'not an envelope'),
    )

    assert events_total('invalid', 'unknown') == before + 1


async def test_handler_duration_is_observed() -> None:
    name = 'consumer_handler_duration_seconds_count'
    labels = {'service': SERVICE, 'event_type': str(EventType.ORDER_NOTIFIED)}
    before = REGISTRY.get_sample_value(name, labels) or 0.0

    async def handler(envelope: EventEnvelope, topic: str) -> None:
        return None

    await make_consumer(handler)._process(
        FakeProducer(),
        FakeMessage(
            str(Topic.NOTIFICATIONS),
            make_envelope(EventType.ORDER_NOTIFIED).to_json(),
        ),
    )

    assert (REGISTRY.get_sample_value(name, labels) or 0.0) == before + 1
