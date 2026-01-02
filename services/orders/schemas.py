from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class OrderBase(BaseModel):
    user_id: int = Field(description='ID пользователя (владелец заказа)')
    status: str = Field(description='Статус заказа')
    total_price: Decimal = Field(ge=0, description='Итоговая стоимость заказа')


class OrderCreate(OrderBase):
    model_config = ConfigDict(
        json_schema_extra={
            'examples': [
                {
                    'user_id': 1,
                    'status': 'created',
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
                    'status': 'paid',
                    'total_price': '199.99',
                }
            ]
        }
    )

    status: str | None = Field(None, description='Новый статус заказа')
    total_price: Decimal | None = Field(
        None, ge=0, description='Обновлённая стоимость заказа'
    )


class OrderRead(BaseModel):
    id: int
    user_id: int
    status: str
    total_price: Decimal
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

    @field_serializer('total_price')
    def serialize_price(self, value: Decimal) -> str:
        return f'{value:.2f}'


class OrdersList(BaseModel):
    items: list[OrderRead]
    total: int
    limit: int
    offset: int


class OrderDelete(OrderRead):
    deleted: bool = True
