from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from notification.models import NotificationStatus


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: str
    saga_id: str
    order_id: int
    user_id: int | None
    channel: str
    recipient: str | None
    subject: str
    body: str
    status: NotificationStatus
    error: str | None
    created_at: datetime


class NotificationsList(BaseModel):
    items: list[NotificationRead]
    total: int
    limit: int
    offset: int
