from __future__ import annotations

from typing import Iterable

from orders.core.exceptions import (
    IntegrityConflictException,
    OrderNotFoundException,
)
from orders.models import Order
from orders.repository import OrderRepository
from orders.schemas import (
    OrderCreate,
    OrderDelete,
    OrderRead,
    OrdersList,
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
    def _to_list(items: Iterable) -> list[OrderRead]:
        return [OrderRead.model_validate(i) for i in items]

    async def create(self, payload: OrderCreate) -> OrderRead:
        order = Order(
            user_id=payload.user_id,
            status=payload.status,
            total_price=payload.total_price,
        )

        try:
            obj = await self._repo.create(order)
            await self._session.refresh(obj)
        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        await self._session.commit()
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
        obj = await self._repo.get_by_id(order_id)
        if obj is None:
            raise OrderNotFoundException(order_id)

        data = payload.model_dump(exclude_unset=True)
        for field, value in data.items():
            setattr(obj, field, value)

        try:
            obj = await self._repo.update(obj)
            await self._session.refresh(obj)
        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        await self._session.commit()

        return self._to_schema(obj)

    async def delete(self, order_id: int) -> OrderDelete:
        obj = await self._repo.get_by_id(order_id)
        if obj is None:
            raise OrderNotFoundException(order_id)

        try:
            deleted = await self._repo.delete(obj)
        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        await self._session.commit()

        return deleted
