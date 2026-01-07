from __future__ import annotations

from typing import Iterable

from orders.core.exceptions import (
    IntegrityConflictException,
    OrderNotFoundException,
)
from orders.models import Order, OrderStatus
from orders.repository import OrderRepository
from orders.schemas import (
    OrderCreate,
    OrderDelete,
    OrderRead,
    OrdersList,
    OrderStatusRead,
    OrderUpdate,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


class OrderService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repo = OrderRepository(session=session)

    @staticmethod
    def _to_schema(obj) -> OrderRead:
        return OrderRead.model_validate(obj)

    @staticmethod
    def _to_status_schema(obj) -> OrderStatusRead:
        return OrderStatusRead.model_validate(obj)

    @staticmethod
    def _to_list(items: Iterable) -> list[OrderRead]:
        return [OrderRead.model_validate(i) for i in items]

    async def create(self, payload: OrderCreate, *, user_id: int) -> OrderRead:
        order = Order(
            user_id=user_id,
            status=OrderStatus.PENDING,
            total_price=payload.total_price,
        )

        try:
            async with self._session.begin():
                obj = await self._repo.create(order)
        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        await self._session.refresh(obj)
        return self._to_schema(obj)

    async def get_by_id(self, order_id: int) -> OrderRead:
        obj = await self._repo.get_by_id(order_id)
        if obj is None:
            raise OrderNotFoundException(order_id)
        return self._to_schema(obj)

    async def get_all(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> OrdersList:
        items, total = await self._repo.get_all(limit=limit, offset=offset)
        return OrdersList(
            items=self._to_list(items),
            total=total,
            limit=limit,
            offset=offset,
        )

    async def update(self, order_id: int, payload: OrderUpdate) -> OrderRead:
        try:
            async with self._session.begin():
                obj = await self._repo.get_by_id(order_id)
                if obj is None:
                    raise OrderNotFoundException(order_id)

                data = payload.model_dump(exclude_unset=True)
                data.pop('status', None)

                for field, value in data.items():
                    setattr(obj, field, value)

                obj = await self._repo.update(obj)
        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        await self._session.refresh(obj)
        return self._to_schema(obj)

    async def confirm(self, order_id: int) -> OrderStatusRead:
        async with self._session.begin():
            obj = await self._repo.get_by_id_for_update(order_id)
            if obj is None:
                raise OrderNotFoundException(order_id)

            if obj.status == OrderStatus.CONFIRMED:
                return self._to_status_schema(obj)

            if obj.status != OrderStatus.PENDING:
                raise IntegrityConflictException(
                    f'Невозможно подтвердить заказ со статусом {obj.status}'
                )

            obj.status = OrderStatus.CONFIRMED
            obj = await self._repo.update(obj)

        await self._session.refresh(obj)
        return self._to_status_schema(obj)

    async def cancel(self, order_id: int) -> OrderStatusRead:
        async with self._session.begin():
            obj = await self._repo.get_by_id_for_update(order_id)
            if obj is None:
                raise OrderNotFoundException(order_id)

            if obj.status == OrderStatus.CANCELLED:
                return self._to_status_schema(obj)

            if obj.status != OrderStatus.PENDING:
                raise IntegrityConflictException(
                    f'Невозможно отменить заказ со статусом {obj.status}'
                )

            obj.status = OrderStatus.CANCELLED
            obj = await self._repo.update(obj)

        await self._session.refresh(obj)
        return self._to_status_schema(obj)

    async def delete(self, order_id: int) -> OrderDelete:
        try:
            async with self._session.begin():
                obj = await self._repo.get_by_id(order_id)
                if obj is None:
                    raise OrderNotFoundException(order_id)

                deleted = OrderDelete.model_validate(obj)
                await self._repo.delete(obj)
        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        return deleted
