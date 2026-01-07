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
    '/{order_id}/history',
    response_model=OrderHistoryList,
)
async def get_order_history(
    order_id: int,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(DEFAULT_OFFSET, ge=0),
    service: HistoryService = Depends(get_history_service),
) -> OrderHistoryList:
    return await service.list_history(order_id, limit=limit, offset=offset)
