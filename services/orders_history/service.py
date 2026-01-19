from __future__ import annotations

from typing import Any, cast

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
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession


class HistoryService:
    def __init__(self, session: AsyncSession):
        self._session = session
        self._history_repo = OrderHistoryRepository(session)
        self._processed_repo = ProcessedEventRepository(session)

    async def record_event(
        self, event: HistoryEventIn
    ) -> OrderHistoryRead | None:
        """Записывает событие в историю заказов.
        Идемпотентность обеспечивается таблицей
        ProcessedEvent (unique по event_id).
        """
        async with self._session.begin():
            # Идемпотентность по event_id:
            # если событие уже обработано — просто выходим.
            stmt = (
                insert(ProcessedEvent)
                .values(event_id=event.event_id)
                .on_conflict_do_nothing(
                    index_elements=[ProcessedEvent.event_id]
                )
            )
            result = cast(CursorResult[Any], await self._session.execute(stmt))
            if result.rowcount == 0:
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
        *,
        limit: int,
        offset: int,
        order_id: int | None = None,
        user_id: int | None = None,
        event_type: str | None = None,
    ) -> OrderHistoryList:
        """Возвращает историю заказов (общий журнал) с пагинацией и фильтрами.

        Можно фильтровать по:
        - order_id: история конкретного заказа
        - user_id: история по пользователю
        - event_type: тип события (например: order.created)
        """
        stmt = (
            select(OrderHistory)
            .order_by(OrderHistory.id.desc())
            .limit(limit)
            .offset(offset)
        )
        count_stmt = select(func.count()).select_from(OrderHistory)

        if order_id is not None:
            stmt = stmt.where(OrderHistory.order_id == order_id)
            count_stmt = count_stmt.where(OrderHistory.order_id == order_id)

        if user_id is not None:
            stmt = stmt.where(OrderHistory.user_id == user_id)
            count_stmt = count_stmt.where(OrderHistory.user_id == user_id)

        if event_type is not None:
            stmt = stmt.where(OrderHistory.event_type == event_type)
            count_stmt = count_stmt.where(
                OrderHistory.event_type == event_type
            )

        result = await self._session.execute(stmt)
        items = list(result.scalars().all())

        count_result = await self._session.execute(count_stmt)
        total = int(count_result.scalar_one())

        return OrderHistoryList(
            items=[OrderHistoryRead.model_validate(x) for x in items],
            total=total,
            limit=limit,
            offset=offset,
        )
