from __future__ import annotations

from orders_history.models import OrderHistory, ProcessedEvent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class OrderHistoryRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def add_history(self, history: OrderHistory) -> OrderHistory:
        self._session.add(history)
        await self._session.flush()
        return history

    async def list_by_order_id(
        self,
        order_id: int,
        *,
        limit: int,
        offset: int,
    ) -> tuple[list[OrderHistory], int]:
        stmt = (
            select(OrderHistory)
            .where(OrderHistory.order_id == order_id)
            .order_by(OrderHistory.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        items = list(result.scalars().all())

        count_stmt = select(OrderHistory).where(
            OrderHistory.order_id == order_id
        )
        count_result = await self._session.execute(count_stmt)
        total = len(list(count_result.scalars().all()))

        return items, total


class ProcessedEventRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def add(self, event: ProcessedEvent) -> None:
        self._session.add(event)
        await self._session.flush()

    async def exists(self, event_id: str) -> bool:
        stmt = select(ProcessedEvent).where(
            ProcessedEvent.event_id == event_id
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
