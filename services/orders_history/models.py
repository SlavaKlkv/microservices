from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class OrderHistory(Base):
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

    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProcessedEvent(Base):
    """
    Таблица для идемпотентности consumer'а.
    Если event_id уже есть здесь — событие уже обработано.
    """

    __tablename__ = 'processed_event'

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
