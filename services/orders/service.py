from __future__ import annotations

from typing import Any, Iterable
from uuid import UUID, uuid4

import structlog
from ms_events import (
    EventEnvelope,
    EventType,
    Producer,
    Topic,
    outbox_values,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from orders.core.exceptions import (
    IntegrityConflictException,
    OrderNotFoundException,
)
from orders.models import Order, OrderStatus, OutboxEvent
from orders.repository import OrderRepository
from orders.schemas import (
    OrderCreate,
    OrderDelete,
    OrderRead,
    OrdersList,
    OrderStatusRead,
    OrderUpdate,
)
from orders.settings import settings


def current_correlation_id() -> UUID | None:
    """X-Request-ID текущего запроса, если он есть в контексте логов."""
    raw = structlog.contextvars.get_contextvars().get('request_id')
    if not raw:
        return None
    try:
        return UUID(str(raw))
    except ValueError:
        return None


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

    def _add_outbox_event(
        self,
        *,
        obj: Order,
        event_type: EventType,
        payload_extra: dict[str, Any] | None = None,
        saga_id: UUID | None = None,
        causation_id: UUID | None = None,
    ) -> EventEnvelope:
        """Кладёт событие в outbox в рамках текущей транзакции.

        Событие всегда упаковано в версионированный конверт: так все
        подписчики читают одну и ту же схему сообщения.
        """
        payload: dict[str, Any] = {
            'order_id': obj.id,
            'user_id': obj.user_id,
            'status': obj.status.value
            if hasattr(obj.status, 'value')
            else str(obj.status),
            'total_price': str(obj.total_price),
        }
        if payload_extra:
            payload.update(payload_extra)

        envelope = EventEnvelope(
            event_type=event_type,
            saga_id=saga_id or uuid4(),
            correlation_id=current_correlation_id(),
            causation_id=causation_id,
            producer=Producer.ORDERS,
            aggregate_type='order',
            aggregate_id=str(obj.id),
            payload=payload,
        )
        self._session.add(
            OutboxEvent(
                **outbox_values(
                    envelope,
                    topic=str(Topic.ORDERS),
                    max_attempts=settings.OUTBOX_MAX_ATTEMPTS,
                )
            )
        )
        return envelope

    async def create(self, payload: OrderCreate, *, user_id: int) -> OrderRead:
        order = Order(
            user_id=user_id,
            status=OrderStatus.PENDING,
            total_price=payload.total_price,
        )

        try:
            async with self._session.begin():
                obj = await self._repo.create(order)

                await self._session.flush()

                # Начало саги: created фиксирует факт, notify_requested
                # просит сервис уведомлений сделать свою часть работы.
                created = self._add_outbox_event(
                    obj=obj, event_type=EventType.ORDER_CREATED
                )
                self._add_outbox_event(
                    obj=obj,
                    event_type=EventType.ORDER_NOTIFY_REQUESTED,
                    saga_id=created.saga_id,
                    causation_id=created.event_id,
                )

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
                obj = await self._repo.get_by_id_for_update(order_id)
                if obj is None:
                    raise OrderNotFoundException(order_id)

                old_total_price = obj.total_price

                data = payload.model_dump(exclude_unset=True)
                data.pop('status', None)

                for field, value in data.items():
                    setattr(obj, field, value)

                obj = await self._repo.update(obj)

                # Публикация доменного события только в случае изменения цены
                if (
                    'total_price' in data
                    and obj.total_price != old_total_price
                ):
                    await self._session.flush()
                    self._add_outbox_event(
                        obj=obj,
                        event_type=EventType.ORDER_PRICE_CHANGED,
                        payload_extra={
                            'old_total_price': str(old_total_price),
                            'new_total_price': str(obj.total_price),
                        },
                    )
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

            await self._session.flush()
            self._add_outbox_event(
                obj=obj, event_type=EventType.ORDER_CONFIRMED
            )

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

            await self._session.flush()
            self._add_outbox_event(
                obj=obj, event_type=EventType.ORDER_CANCELLED
            )

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
