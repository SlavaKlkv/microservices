from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from orders.models import OrderStatus


class OrderBase(BaseModel):
    total_price: Decimal = Field(ge=0, description='Итоговая стоимость заказа')


class OrderCreate(OrderBase):
    model_config = ConfigDict(
        json_schema_extra={
            'examples': [
                {
                    'total_price': '199.99',
                }
            ]
        }
    )


class OrderUpdate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            'examples': [
                {
                    'total_price': '199.99',
                }
            ]
        }
    )

    total_price: Decimal | None = Field(
        None, ge=0, description='Обновлённая стоимость заказа'
    )


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: int
    status: OrderStatus
    total_price: Decimal
    created_at: datetime
    updated_at: datetime

    @field_serializer('total_price')
    def serialize_price(self, value: Decimal) -> str:
        return f'{value:.2f}'


class OrderStatusRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: OrderStatus


class OrdersList(BaseModel):
    items: list[OrderRead]
    total: int
    limit: int
    offset: int


class OrderDelete(OrderRead):
    deleted: bool = True
