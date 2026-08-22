from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from notification.core.constants import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    MAX_LIMIT,
)
from notification.core.db import get_session
from notification.schemas import NotificationsList
from notification.service import NotificationService

notifications_router = APIRouter(
    prefix='/notifications', tags=['notifications']
)


def get_notification_service(
    session: AsyncSession = Depends(get_session),
) -> NotificationService:
    return NotificationService(session)


@notifications_router.get('', response_model=NotificationsList)
async def list_notifications(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(DEFAULT_OFFSET, ge=0),
    order_id: int | None = Query(default=None, ge=1),
    saga_id: str | None = Query(default=None, max_length=36),
    service: NotificationService = Depends(get_notification_service),
) -> NotificationsList:
    """Журнал отправленных уведомлений с фильтрами."""
    return await service.list_notifications(
        limit=limit, offset=offset, order_id=order_id, saga_id=saga_id
    )
