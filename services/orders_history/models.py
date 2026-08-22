from __future__ import annotations

from datetime import datetime
from typing import Any

from ms_events import ProcessedEventMixin
from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from orders_history.core.db import Base


class OrderHistory(Base):
    """Журнал событий заказов — по одной строке на событие."""

    __tablename__ = 'order_history'
    __table_args__ = (
        UniqueConstraint('event_id', name='uq_order_history_event_id'),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    order_id: Mapped[int] = mapped_column(
        BigInteger, index=True, nullable=False
    )
    user_id: Mapped[int] = mapped_column(
        BigInteger, index=True, nullable=False
    )

    event_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)

    saga_id: Mapped[str | None] = mapped_column(
        String(36),
        nullable=True,
        index=True,
        comment='Идентификатор саги, к которой относится событие',
    )

    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProcessedEvent(ProcessedEventMixin, Base):
    """Идемпотентность консьюмера: event_id уже здесь — событие обработано."""
