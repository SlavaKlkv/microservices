from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from orders_history.core.constants import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    MAX_LIMIT,
)
from orders_history.core.db import get_session  # type: ignore
from orders_history.schemas import OrderHistoryList
from orders_history.service import HistoryService
from sqlalchemy.ext.asyncio import AsyncSession

orders_history_router = APIRouter(
    prefix='/orders_history', tags=['orders_history']
)


def get_history_service(
    session: AsyncSession = Depends(get_session),
) -> HistoryService:
    return HistoryService(session)


@orders_history_router.get(
    '/history',
    response_model=OrderHistoryList,
)
async def list_orders_history(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(DEFAULT_OFFSET, ge=0),
    order_id: int | None = Query(
        default=None, ge=1, description='Фильтр по ID заказа'
    ),
    user_id: int | None = Query(
        default=None, ge=1, description='Фильтр по ID пользователя'
    ),
    event_type: str | None = Query(
        default=None,
        description='Фильтр по типу события '
                    '(order.created, order.confirmed, '
                    'order.cancelled, order.price_changed)',
    ),
    service: HistoryService = Depends(get_history_service),
) -> OrderHistoryList:
    """Общий журнал истории заказов с фильтрами."""
    return await service.list_history(
        limit=limit,
        offset=offset,
        order_id=order_id,
        user_id=user_id,
        event_type=event_type,
    )
