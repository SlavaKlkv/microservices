from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    String,
    Text,
    func,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import Mapped, mapped_column

from ms_events import OutboxMixin, ProcessedEventMixin
from notification.core.db import Base


class NotificationStatus(str, enum.Enum):
    """Результат попытки доставить уведомление."""

    SENT = 'SENT'
    FAILED = 'FAILED'


class Notification(Base):
    """Одно уведомление, порождённое событием саги."""

    __tablename__ = 'notification'

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    event_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        unique=True,
        index=True,
        comment='event_id входящего события — гарантия «одно на событие»',
    )
    saga_id: Mapped[str] = mapped_column(
        String(36), nullable=False, index=True
    )
    order_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, index=True
    )
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    channel: Mapped[str] = mapped_column(
        String(16), nullable=False, default='email'
    )
    recipient: Mapped[str | None] = mapped_column(String(320), nullable=True)
    subject: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)

    status: Mapped[NotificationStatus] = mapped_column(
        SAEnum(NotificationStatus, name='notification_status'),
        nullable=False,
        index=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ProcessedEvent(ProcessedEventMixin, Base):
    """Идемпотентность консьюмера уведомлений."""


class OutboxEvent(OutboxMixin, Base):
    """Outbox сервиса уведомлений: результат саги публикуется отсюда."""
