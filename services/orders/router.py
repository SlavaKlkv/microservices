from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from orders.core.constants import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    MAX_LIMIT,
)
from orders.core.db import get_session
from orders.core.dependencies import CurrentUser, get_current_user
from orders.schemas import (
    OrderCreate,
    OrderDelete,
    OrderRead,
    OrdersList,
    OrderStatusRead,
    OrderUpdate,
)
from orders.service import OrderService

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
    user: CurrentUser = Depends(get_current_user),
) -> OrderRead:
    return await service.create(payload, user_id=user.id)


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


@orders_router.post(
    '/{order_id}/confirm',
    response_model=OrderStatusRead,
)
async def confirm_order(
    order_id: int,
    service: OrderService = Depends(get_order_service),
) -> OrderStatusRead:
    return await service.confirm(order_id)


@orders_router.post(
    '/{order_id}/cancel',
    response_model=OrderStatusRead,
)
async def cancel_order(
    order_id: int,
    service: OrderService = Depends(get_order_service),
) -> OrderStatusRead:
    return await service.cancel(order_id)


@orders_router.delete(
    '/{order_id}',
    response_model=OrderDelete,
)
async def delete_order(
    order_id: int,
    service: OrderService = Depends(get_order_service),
) -> OrderDelete:
    return await service.delete(order_id)
