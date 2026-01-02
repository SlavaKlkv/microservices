from orders.core.constants import (
    ORDER_STATUS_MAX_LENGTH,
    ORDER_TOTAL_PRICE_MIN,
    ORDER_TOTAL_PRICE_PRECISION,
    ORDER_TOTAL_PRICE_SCALE,
)
from orders.core.db import Base
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import mapped_column


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

    user_id = mapped_column(
        Integer,
        index=True,
        nullable=False,
        comment='ID пользователя (владелец заказа)',
    )

    status = mapped_column(
        String(ORDER_STATUS_MAX_LENGTH),
        index=True,
        nullable=False,
        comment='Статус заказа',
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
