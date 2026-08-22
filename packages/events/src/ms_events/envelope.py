"""Конверт события — единый контракт обмена между сервисами."""

from datetime import datetime, timezone
from typing import Any, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from ms_events.types import EventType, Producer

#: Текущая версия схемы конверта.
CURRENT_EVENT_VERSION = 1


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventEnvelope(BaseModel):
    """Оболочка любого события в шине.

    Поля трассировки:
    * ``saga_id`` — общий идентификатор всей цепочки саги;
    * ``correlation_id`` — идентификатор исходного HTTP-запроса;
    * ``causation_id`` — ``event_id`` события-причины.
    """

    model_config = ConfigDict(extra='forbid')

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    event_version: int = CURRENT_EVENT_VERSION
    saga_id: UUID
    correlation_id: UUID | None = None
    causation_id: UUID | None = None
    occurred_at: datetime = Field(default_factory=utcnow)
    producer: Producer
    aggregate_type: str = 'order'
    aggregate_id: str
    payload: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def caused_by(
        cls,
        source: 'EventEnvelope',
        *,
        event_type: EventType,
        producer: Producer,
        payload: dict[str, Any] | None = None,
    ) -> Self:
        """Следующее событие саги с трассировкой от исходного."""
        return cls(
            event_type=event_type,
            saga_id=source.saga_id,
            correlation_id=source.correlation_id,
            causation_id=source.event_id,
            producer=producer,
            aggregate_type=source.aggregate_type,
            aggregate_id=source.aggregate_id,
            payload=payload or {},
        )

    def to_json(self) -> bytes:
        return self.model_dump_json().encode('utf-8')

    @classmethod
    def from_json(cls, raw: bytes | str) -> Self:
        """Разбирает сообщение из Kafka.

        Бросает ``pydantic.ValidationError`` — вызывающий консьюмер обязан
        трактовать это как невосстановимую ошибку и отправить сообщение в DLQ.
        """
        return cls.model_validate_json(raw)
