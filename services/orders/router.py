from fastapi import APIRouter, Depends, Query, status
from orders.core.constants import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    MAX_LIMIT,
)
from orders.core.db import get_session
from orders.schemas import (
    OrderCreate,
    OrderDelete,
    OrderRead,
    OrdersList,
    OrderUpdate,
)
from orders.service import OrderService
from sqlalchemy.ext.asyncio import AsyncSession

orders_router = APIRouter(prefix='/orders', tags=['orders'])


def get_order_service(
    session: AsyncSession = Depends(get_session),
) -> OrderService:
    return OrderService(session=session)


@orders_router.post(
    '',
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_order(
    payload: OrderCreate,
    service: OrderService = Depends(get_order_service),
) -> OrderRead:
    return await service.create(payload)


@orders_router.get(
    '/{order_id}',
    response_model=OrderRead,
)
async def get_order_by_id(
    order_id: int,
    service: OrderService = Depends(get_order_service),
) -> OrderRead:
    return await service.get_by_id(order_id)


@orders_router.get(
    '',
    response_model=OrdersList,
)
async def get_orders(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(DEFAULT_OFFSET, ge=0),
    service: OrderService = Depends(get_order_service),
) -> OrdersList:
    return await service.get_all(limit=limit, offset=offset)


@orders_router.patch(
    '/{order_id}',
    response_model=OrderRead,
)
async def update_order(
    order_id: int,
    payload: OrderUpdate,
    service: OrderService = Depends(get_order_service),
) -> OrderRead:
    return await service.update(order_id, payload)


@orders_router.delete(
    '/{order_id}',
    response_model=OrderDelete,
)
async def delete_order(
    order_id: int,
    service: OrderService = Depends(get_order_service),
) -> OrderDelete:
    return await service.delete(order_id)
