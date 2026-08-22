"""Конверт события — единственный контракт между сервисами.

Ломать его молча нельзя: подписчик, который не разберёт сообщение,
отправит его в DLQ, а не упадёт заметно.
"""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ms_events import CURRENT_EVENT_VERSION, EventEnvelope, EventType, Producer


def make_envelope(**overrides: object) -> EventEnvelope:
    data: dict[str, object] = {
        'event_type': EventType.ORDER_CREATED,
        'saga_id': uuid4(),
        'producer': Producer.ORDERS,
        'aggregate_id': '42',
        'payload': {'order_id': 42, 'user_id': 7},
    }
    data.update(overrides)
    return EventEnvelope(**data)  # type: ignore[arg-type]


def test_roundtrip_preserves_all_fields() -> None:
    envelope = make_envelope(correlation_id=uuid4())

    restored = EventEnvelope.from_json(envelope.to_json())

    assert restored == envelope


def test_version_is_stamped_by_default() -> None:
    assert make_envelope().event_version == CURRENT_EVENT_VERSION


def test_event_ids_are_unique_per_envelope() -> None:
    assert make_envelope().event_id != make_envelope().event_id


def test_caused_by_carries_saga_trace() -> None:
    source = make_envelope(correlation_id=uuid4())

    result = EventEnvelope.caused_by(
        source,
        event_type=EventType.ORDER_NOTIFIED,
        producer=Producer.NOTIFICATION,
        payload={'order_id': 42},
    )

    # saga_id и correlation_id тянутся через всю цепочку, causation_id
    # указывает на непосредственную причину — так восстанавливается порядок.
    assert result.saga_id == source.saga_id
    assert result.correlation_id == source.correlation_id
    assert result.causation_id == source.event_id
    assert result.event_id != source.event_id
    assert result.producer == Producer.NOTIFICATION
    assert result.aggregate_id == source.aggregate_id


def test_caused_by_defaults_payload_to_empty() -> None:
    result = EventEnvelope.caused_by(
        make_envelope(),
        event_type=EventType.ORDER_CONFIRMED,
        producer=Producer.ORDERS,
    )

    assert result.payload == {}


def test_unknown_field_is_rejected() -> None:
    # extra='forbid': опечатка в имени поля должна падать здесь, а не
    # тихо теряться по дороге к подписчику.
    raw = json.loads(make_envelope().to_json())
    raw['whatever'] = 1

    with pytest.raises(ValidationError):
        EventEnvelope.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    'raw',
    [b'', b'not json at all', b'{"event_type": "order.created"}'],
)
def test_broken_message_raises(raw: bytes) -> None:
    # Консьюмер ловит это исключение и уводит сообщение в DLQ.
    with pytest.raises((ValidationError, ValueError)):
        EventEnvelope.from_json(raw)


def test_unknown_event_type_is_rejected() -> None:
    raw = json.loads(make_envelope().to_json())
    raw['event_type'] = 'order.teleported'

    with pytest.raises(ValidationError):
        EventEnvelope.from_json(json.dumps(raw))
