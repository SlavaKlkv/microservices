"""Общие SQLAlchemy-миксины: outbox и журнал обработанных событий.

Каждый сервис держит свою БД и свой ``Base``, поэтому переиспользуется
не готовая модель, а миксин с набором колонок и конвертацией в конверт
события. Так outbox у orders и у notification устроены одинаково.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    DateTime,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ms_events.envelope import EventEnvelope
from ms_events.types import EventType, Producer

#: Длина строкового представления UUID.
UUID_LENGTH = 36


class OutboxStatus(str, enum.Enum):
    """Состояние строки outbox.

    ``DEAD`` — попытки публикации исчерпаны, событие ушло в DLQ и больше
    не будет разбираться воркером.
    """

    NEW = 'NEW'
    SENT = 'SENT'
    ERROR = 'ERROR'
    DEAD = 'DEAD'


class OutboxMixin:
    """Колонки таблицы ``outbox``, общие для всех сервисов-издателей."""

    __tablename__ = 'outbox'

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    event_id: Mapped[str] = mapped_column(
        String(UUID_LENGTH),
        nullable=False,
        unique=True,
        index=True,
        comment='UUID бизнес-события для идемпотентности',
    )
    event_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
        comment='Тип события, например order.created',
    )
    saga_id: Mapped[str] = mapped_column(
        String(UUID_LENGTH),
        nullable=False,
        index=True,
        comment='Идентификатор саги, общий для всей цепочки',
    )
    correlation_id: Mapped[str | None] = mapped_column(
        String(UUID_LENGTH),
        nullable=True,
        comment='X-Request-ID исходного HTTP-запроса',
    )
    causation_id: Mapped[str | None] = mapped_column(
        String(UUID_LENGTH),
        nullable=True,
        comment='event_id события-причины',
    )
    producer: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment='Сервис-издатель события',
    )
    topic: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        index=True,
        comment='Kafka-топик, в который публикуется событие',
    )
    aggregate_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        index=True,
        comment='Тип агрегата, например order',
    )
    aggregate_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        index=True,
        comment='ID агрегата (order_id) — он же ключ партиционирования',
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment='Полезная нагрузка события',
    )
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus, name='outbox_status'),
        nullable=False,
        default=OutboxStatus.NEW,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment='Сделано попыток отправки'
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=10,
        comment='Предел попыток, после которого строка становится DEAD',
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment='Время следующей попытки',
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment='Последняя ошибка публикации'
    )

    def to_envelope(self) -> EventEnvelope:
        """Восстанавливает конверт события из строки outbox."""
        return EventEnvelope(
            event_id=UUID(self.event_id),
            event_type=EventType(self.event_type),
            saga_id=UUID(self.saga_id),
            correlation_id=(
                UUID(self.correlation_id) if self.correlation_id else None
            ),
            causation_id=(
                UUID(self.causation_id) if self.causation_id else None
            ),
            occurred_at=self.created_at,
            producer=Producer(self.producer),
            aggregate_type=self.aggregate_type,
            aggregate_id=str(self.aggregate_id),
            payload=self.payload,
        )


def outbox_values(
    envelope: EventEnvelope,
    *,
    topic: str,
    max_attempts: int = 10,
) -> dict[str, Any]:
    """Готовит значения строки outbox из конверта события."""
    return {
        'event_id': str(envelope.event_id),
        'event_type': str(envelope.event_type),
        'saga_id': str(envelope.saga_id),
        'correlation_id': (
            str(envelope.correlation_id) if envelope.correlation_id else None
        ),
        'causation_id': (
            str(envelope.causation_id) if envelope.causation_id else None
        ),
        'producer': str(envelope.producer),
        'topic': topic,
        'aggregate_type': envelope.aggregate_type,
        'aggregate_id': int(envelope.aggregate_id),
        'payload': envelope.payload,
        'status': OutboxStatus.NEW,
        'attempts': 0,
        'max_attempts': max_attempts,
    }


class ProcessedEventMixin:
    """Журнал обработанных событий — источник истины идемпотентности."""

    __tablename__ = 'processed_event'

    event_id: Mapped[str] = mapped_column(
        String(UUID_LENGTH), primary_key=True
    )
    event_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
