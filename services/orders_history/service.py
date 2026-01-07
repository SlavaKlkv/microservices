from __future__ import annotations

from orders_history.models import OrderHistory, ProcessedEvent
from orders_history.repository import (
    OrderHistoryRepository,
    ProcessedEventRepository,
)
from orders_history.schemas import (
    HistoryEventIn,
    OrderHistoryList,
    OrderHistoryRead,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


class HistoryService:
    def __init__(self, session: AsyncSession):
        self._session = session
        self._history_repo = OrderHistoryRepository(session)
        self._processed_repo = ProcessedEventRepository(session)

    async def record_event(
        self, event: HistoryEventIn
    ) -> OrderHistoryRead | None:
        async with self._session.begin():
            # Пытаемся добавить event_id.
            # Если уже есть — это повторная доставка.
            try:
                await self._processed_repo.add(
                    ProcessedEvent(event_id=event.event_id)
                )
            except IntegrityError:
                # event_id уже обработан: ничего не пишем в историю
                return None

            history = OrderHistory(
                order_id=event.order_id,
                user_id=event.user_id,
                event_id=event.event_id,
                event_type=event.event_type,
                payload=event.payload,
            )
            await self._history_repo.add_history(history)

        await self._session.refresh(history)
        return OrderHistoryRead.model_validate(history)

    async def list_history(
        self,
        order_id: int,
        *,
        limit: int,
        offset: int,
    ) -> OrderHistoryList:
        items, total = await self._history_repo.list_by_order_id(
            order_id,
            limit=limit,
            offset=offset,
        )
        return OrderHistoryList(
            items=[OrderHistoryRead.model_validate(x) for x in items],
            total=total,
            limit=limit,
            offset=offset,
        )
