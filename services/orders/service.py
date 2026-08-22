from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, cast
from uuid import UUID, uuid4

import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ms_events import (
    EventEnvelope,
    EventType,
    Producer,
    Topic,
    outbox_values,
    utcnow,
)
from ms_events.metrics import SAGA_COMPLETED, SAGA_DURATION
from orders.core.exceptions import (
    IntegrityConflictException,
    OrderNotFoundException,
)
from orders.models import (
    Order,
    OrderSaga,
    OrderStatus,
    OutboxEvent,
    ProcessedEvent,
    SagaState,
)
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

logger = structlog.get_logger('orders.service')


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

                obj.saga_id = str(created.saga_id)
                self._session.add(
                    OrderSaga(
                        saga_id=str(created.saga_id),
                        order_id=obj.id,
                        state=SagaState.STARTED,
                        last_event_id=str(created.event_id),
                    )
                )

        except IntegrityError as exc:
            raise IntegrityConflictException() from exc

        await self._session.refresh(obj)
        return self._to_schema(obj)

    async def _claim_event(self, envelope: EventEnvelope) -> bool:
        """Отмечает событие обработанным. False — это дубль.

        Вставка идёт в той же транзакции, что и смена статуса заказа,
        поэтому «обработано» и «применено» не могут разъехаться.
        """
        stmt = (
            pg_insert(ProcessedEvent)
            .values(
                event_id=str(envelope.event_id),
                event_type=str(envelope.event_type),
            )
            .on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id])
        )
        result = cast(CursorResult[Any], await self._session.execute(stmt))
        return result.rowcount > 0

    async def _touch_saga(
        self, envelope: EventEnvelope, order: Order, state: SagaState
    ) -> datetime | None:
        """Фиксирует текущий шаг саги (создаёт строку, если её ещё нет).

        Возвращает момент начала саги — по нему считается её длительность.
        """
        stmt = (
            pg_insert(OrderSaga)
            .values(
                saga_id=str(envelope.saga_id),
                order_id=order.id,
                state=str(state),
                last_event_id=str(envelope.event_id),
            )
            .on_conflict_do_update(
                index_elements=[OrderSaga.saga_id],
                set_={
                    'state': str(state),
                    'last_event_id': str(envelope.event_id),
                    'updated_at': utcnow(),
                },
            )
            .returning(OrderSaga.started_at)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    @staticmethod
    def _observe_saga(outcome: str, started_at: datetime | None) -> None:
        """Записывает завершение саги в метрики.

        Вызывается только из ветки реального перехода статуса: она
        отрабатывает ровно один раз на сагу, поэтому гистограмма не
        накручивается повторами событий.
        """
        SAGA_COMPLETED.labels(outcome=outcome).inc()
        if started_at is None:
            return
        SAGA_DURATION.labels(outcome=outcome).observe(
            max((utcnow() - started_at).total_seconds(), 0.0)
        )

    async def apply_notification_result(
        self, envelope: EventEnvelope
    ) -> OrderStatus | None:
        """Завершает сагу по результату уведомления.

        Успех переводит заказ PENDING → CONFIRMED, неудача запускает
        компенсацию PENDING → CANCELLED с событием order.cancelled.
        Повторное событие и заказ, уже находящийся в целевом статусе, —
        no-op: сага обязана быть идемпотентной при at-least-once доставке.
        """
        order_id = int(
            envelope.payload.get('order_id') or envelope.aggregate_id
        )

        async with self._session.begin():
            if not await self._claim_event(envelope):
                return None

            obj = await self._repo.get_by_id_for_update(order_id)
            if obj is None:
                logger.warning(
                    'saga.order_not_found',
                    order_id=order_id,
                    event_id=str(envelope.event_id),
                )
                return None

            succeeded = envelope.event_type == EventType.ORDER_NOTIFIED
            target = (
                OrderStatus.CONFIRMED if succeeded else OrderStatus.CANCELLED
            )

            if obj.status == target:
                await self._touch_saga(
                    envelope,
                    obj,
                    SagaState.CONFIRMED if succeeded else SagaState.CANCELLED,
                )
                return target

            if obj.status != OrderStatus.PENDING:
                # Заказ уже завершён вручную — сага не спорит с оператором.
                logger.info(
                    'saga.transition_skipped',
                    order_id=order_id,
                    status=obj.status.value,
                    event_type=str(envelope.event_type),
                )
                return obj.status

            obj.status = target
            if not succeeded:
                obj.cancel_reason = str(
                    envelope.payload.get('reason') or 'notification failed'
                )
            await self._session.flush()

            self._add_outbox_event(
                obj=obj,
                event_type=(
                    EventType.ORDER_CONFIRMED
                    if succeeded
                    else EventType.ORDER_CANCELLED
                ),
                payload_extra=(
                    {} if succeeded else {'reason': obj.cancel_reason}
                ),
                saga_id=envelope.saga_id,
                causation_id=envelope.event_id,
            )
            started_at = await self._touch_saga(
                envelope,
                obj,
                SagaState.CONFIRMED if succeeded else SagaState.CANCELLED,
            )
            self._observe_saga(
                'completed' if succeeded else 'compensated', started_at
            )

        logger.info(
            'saga.order_transitioned',
            order_id=order_id,
            saga_id=str(envelope.saga_id),
            status=target.value,
        )
        return target

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
