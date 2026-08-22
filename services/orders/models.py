import enum
from datetime import datetime

from ms_events import OutboxMixin, ProcessedEventMixin
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import Mapped, mapped_column

from orders.core.constants import (
    ORDER_TOTAL_PRICE_MIN,
    ORDER_TOTAL_PRICE_PRECISION,
    ORDER_TOTAL_PRICE_SCALE,
)
from orders.core.db import Base


class OrderStatus(str, enum.Enum):
    PENDING = 'PENDING'
    CONFIRMED = 'CONFIRMED'
    CANCELLED = 'CANCELLED'


class SagaState(enum.StrEnum):
    """Шаги саги заказа — хранится строкой, чтобы не плодить enum в БД."""

    STARTED = 'STARTED'
    NOTIFIED = 'NOTIFIED'
    NOTIFICATION_FAILED = 'NOTIFICATION_FAILED'
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

    saga_id: Mapped[str | None] = mapped_column(
        String(36),
        nullable=True,
        index=True,
        comment='Сага, начатая созданием заказа',
    )

    cancel_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='Причина отмены (для компенсации саги)',
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


class OutboxEvent(OutboxMixin, Base):
    """Транзакционный outbox сервиса заказов.

    Набор колонок и правила публикации общие для всех издателей и живут
    в ``ms_events.db.OutboxMixin``.
    """


class ProcessedEvent(ProcessedEventMixin, Base):
    """Идемпотентность саги-консьюмера сервиса заказов."""


class OrderSaga(Base):
    """Состояние саги по заказу — для наблюдаемости и разбора инцидентов."""

    __tablename__ = 'order_saga'

    saga_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, index=True
    )
    state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        index=True,
        comment='Текущий шаг саги: STARTED, NOTIFIED, CONFIRMED, CANCELLED…',
    )
    last_event_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, comment='Последнее применённое событие'
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
