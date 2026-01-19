import enum
from typing import Any

from orders.core.constants import (
    ORDER_TOTAL_PRICE_MIN,
    ORDER_TOTAL_PRICE_PRECISION,
    ORDER_TOTAL_PRICE_SCALE,
    EVENT_ID_LENGTH,
)
from orders.core.db import Base
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    Text,
    func,
    String,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from uuid import uuid4


class OutboxStatus(str, enum.Enum):
    NEW = 'NEW'
    SENT = 'SENT'
    ERROR = 'ERROR'


class OrderStatus(str, enum.Enum):
    PENDING = 'PENDING'
    CONFIRMED = 'CONFIRMED'
    CANCELLED = 'CANCELLED'


class Order(Base):
    __tablename__ = 'orders'
    __table_args__ = (
        CheckConstraint(
            f'total_price >= {ORDER_TOTAL_PRICE_MIN}',
            name='ck_orders_total_price_non_negative',
        ),
        CheckConstraint(
            'user_id >= 1',
            name='ck_orders_user_id_positive',
        ),
    )

    id = mapped_column(Integer, primary_key=True)

    user_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        index=True,
    )

    status: Mapped[OrderStatus] = mapped_column(
        SAEnum(OrderStatus, name='order_status'),
        default=OrderStatus.PENDING,
        nullable=False,
        index=True,
    )

    total_price = mapped_column(
        Numeric(ORDER_TOTAL_PRICE_PRECISION, ORDER_TOTAL_PRICE_SCALE),
        nullable=False,
        comment='Итоговая стоимость заказа',
    )

    created_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment='Дата создания заказа',
    )

    updated_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment='Дата последнего обновления заказа',
    )


class OutboxEvent(Base):
    __tablename__ = 'outbox'

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    event_id: Mapped[str] = mapped_column(
        String(EVENT_ID_LENGTH),
        nullable=False,
        unique=True,
        index=True,
        default=lambda: str(uuid4()),
        comment='UUID бизнес-события для идемпотентности'
    )

    aggregate_type: Mapped[str] = mapped_column(
        nullable=False,
        index=True,
        comment='Тип агрегата (например: order)',
    )

    aggregate_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        index=True,
        comment='ID агрегата (order_id)',
    )

    event_type: Mapped[str] = mapped_column(
        nullable=False,
        index=True,
        comment='Тип события (например: OrderCreated)',
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment='JSON payload события',
    )

    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus, name='outbox_status'),
        nullable=False,
        default=OutboxStatus.NEW,
        index=True,
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment='Количество попыток отправки',
    )

    next_retry_at = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment='Дата следующей попытки отправки',
    )

    created_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    sent_at = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='Последняя ошибка при отправке',
    )
