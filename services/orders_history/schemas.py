from __future__ import annotations

from datetime import datetime
from typing import Any

from orders_history.core.constants import (
    EVENT_ID_MAX_LEN,
    EVENT_TYPE_MAX_LEN,
)
from pydantic import BaseModel, ConfigDict, Field


class HistoryEventIn(BaseModel):
    """
    Входная схема для consumer'а:
    одно событие, которое нужно записать в историю.
    """

    event_id: str = Field(
        min_length=1,
        max_length=EVENT_ID_MAX_LEN,
        description='Уникальный идентификатор события',
    )
    event_type: str = Field(
        min_length=1,
        max_length=EVENT_TYPE_MAX_LEN,
        description='Тип события (например, OrderConfirmed)',
    )
    order_id: int = Field(gt=0, description='ID заказа')
    user_id: int = Field(gt=0, description='ID пользователя')
    payload: dict[str, Any] = Field(
        description='Полезная нагрузка события (JSON)'
    )


class OrderHistoryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    order_id: int
    user_id: int
    event_id: str
    event_type: str
    payload: dict[str, Any]
    created_at: datetime


class OrderHistoryList(BaseModel):
    items: list[OrderHistoryRead]
    total: int
    limit: int
    offset: int


class ProcessedEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: str
    processed_at: datetime
