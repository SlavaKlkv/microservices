"""Строка outbox и конверт события обязаны быть взаимно обратимы.

Outbox — это то, что переживает падение процесса. Если из строки нельзя
восстановить ровно то событие, которое собирались отправить, гарантия
«ничего не потеряется» ничего не стоит.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from ms_events import EventEnvelope, EventType, OutboxStatus, Producer, Topic
from ms_events.db import OutboxMixin, outbox_values


class FakeOutboxRow(OutboxMixin):
    """Строка outbox без привязки к БД — нужен только маппинг колонок."""

    def __init__(self, **values: object) -> None:
        for key, value in values.items():
            setattr(self, key, value)


def make_envelope() -> EventEnvelope:
    return EventEnvelope(
        event_type=EventType.ORDER_CREATED,
        saga_id=uuid4(),
        correlation_id=uuid4(),
        causation_id=uuid4(),
        producer=Producer.ORDERS,
        aggregate_id='42',
        payload={'order_id': 42, 'total_price': '10.00'},
    )


def test_outbox_values_maps_envelope() -> None:
    envelope = make_envelope()

    values = outbox_values(envelope, topic=str(Topic.ORDERS), max_attempts=7)

    assert values['event_id'] == str(envelope.event_id)
    assert values['event_type'] == str(envelope.event_type)
    assert values['saga_id'] == str(envelope.saga_id)
    assert values['correlation_id'] == str(envelope.correlation_id)
    assert values['causation_id'] == str(envelope.causation_id)
    assert values['producer'] == str(envelope.producer)
    assert values['topic'] == str(Topic.ORDERS)
    assert values['aggregate_id'] == 42
    assert values['payload'] == envelope.payload
    assert values['status'] is OutboxStatus.NEW
    assert values['attempts'] == 0
    assert values['max_attempts'] == 7


def test_optional_trace_fields_survive_being_empty() -> None:
    envelope = make_envelope()
    envelope.correlation_id = None
    envelope.causation_id = None

    values = outbox_values(envelope, topic=str(Topic.ORDERS))

    assert values['correlation_id'] is None
    assert values['causation_id'] is None


def test_row_restores_the_same_envelope() -> None:
    envelope = make_envelope()
    created_at = datetime.now(timezone.utc)

    row = FakeOutboxRow(
        **outbox_values(envelope, topic=str(Topic.ORDERS)),
        created_at=created_at,
    )
    restored = row.to_envelope()

    assert restored.event_id == envelope.event_id
    assert restored.event_type == envelope.event_type
    assert restored.saga_id == envelope.saga_id
    assert restored.correlation_id == envelope.correlation_id
    assert restored.causation_id == envelope.causation_id
    assert restored.producer == envelope.producer
    assert restored.aggregate_id == envelope.aggregate_id
    assert restored.payload == envelope.payload


def test_restored_envelope_is_serializable() -> None:
    envelope = make_envelope()
    row = FakeOutboxRow(
        **outbox_values(envelope, topic=str(Topic.ORDERS)),
        created_at=datetime.now(timezone.utc),
    )

    assert EventEnvelope.from_json(row.to_envelope().to_json()) is not None


def test_dead_status_exists_for_exhausted_rows() -> None:
    # Воркер переводит строку в DEAD и больше её не трогает — без этого
    # статуса безнадёжное событие крутилось бы вечно.
    assert OutboxStatus.DEAD.value == 'DEAD'
