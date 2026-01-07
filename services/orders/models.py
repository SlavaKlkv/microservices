import enum

from orders.core.constants import (
    ORDER_TOTAL_PRICE_MIN,
    ORDER_TOTAL_PRICE_PRECISION,
    ORDER_TOTAL_PRICE_SCALE,
)
from orders.core.db import Base
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    func,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import Mapped, mapped_column


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
