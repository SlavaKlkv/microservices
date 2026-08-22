from __future__ import annotations

from typing import Any, cast

import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from ms_events import (
    EventEnvelope,
    EventType,
    Producer,
    Topic,
    outbox_values,
)
from notification.mailer import NotificationSendError, send_email
from notification.models import (
    Notification,
    NotificationStatus,
    OutboxEvent,
    ProcessedEvent,
)
from notification.schemas import NotificationRead, NotificationsList
from notification.settings import settings

logger = structlog.get_logger('notification.service')


def build_message(envelope: EventEnvelope) -> tuple[str, str]:
    """Тема и текст письма по событию саги."""
    order_id = envelope.payload.get('order_id') or envelope.aggregate_id
    subject = f'Заказ №{order_id} принят'
    body = (
        f'Здравствуйте!\n\n'
        f'Ваш заказ №{order_id} принят в обработку.\n'
        f'Сумма заказа: {envelope.payload.get("total_price", "—")}.\n'
    )
    return subject, body


def resolve_recipient(envelope: EventEnvelope) -> str:
    """Адрес получателя: из события, иначе технический адрес-заглушка."""
    email = envelope.payload.get('user_email')
    if isinstance(email, str) and email:
        return email
    user_id = envelope.payload.get('user_id')
    return f'user-{user_id}@example.invalid'


class NotificationService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def _claim_event(self, envelope: EventEnvelope) -> bool:
        """Отмечает событие обработанным. False — дубль, работать не надо.

        Вставка живёт в той же транзакции, что и бизнес-запись, поэтому
        «обработано» и «записано» не могут разъехаться.
        """
        stmt = (
            insert(ProcessedEvent)
            .values(
                event_id=str(envelope.event_id),
                event_type=str(envelope.event_type),
            )
            .on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id])
        )
        result = cast(CursorResult[Any], await self._session.execute(stmt))
        return result.rowcount > 0

    async def handle_notify_requested(
        self, envelope: EventEnvelope
    ) -> Notification | None:
        """Обрабатывает order.notify_requested.

        Возвращает созданное уведомление либо None, если событие уже было
        обработано раньше.
        """
        async with self._session.begin():
            if not await self._claim_event(envelope):
                return None

            subject, body = build_message(envelope)
            recipient = resolve_recipient(envelope)

            status = NotificationStatus.SENT
            error: str | None = None
            try:
                await send_email(
                    recipient=recipient, subject=subject, body=body
                )
            except NotificationSendError as exc:
                status = NotificationStatus.FAILED
                error = str(exc)
                logger.warning(
                    'notification.send_failed',
                    event_id=str(envelope.event_id),
                    error=error,
                )

            payload = envelope.payload
            notification = Notification(
                event_id=str(envelope.event_id),
                saga_id=str(envelope.saga_id),
                order_id=int(payload.get('order_id') or envelope.aggregate_id),
                user_id=(
                    int(payload['user_id'])
                    if payload.get('user_id') is not None
                    else None
                ),
                channel='email',
                recipient=recipient,
                subject=subject,
                body=body,
                status=status,
                error=error,
            )
            self._session.add(notification)

            # Результат саги уезжает через собственный outbox: запись
            # уведомления и факт его публикации коммитятся вместе.
            self._add_result_event(envelope, notification)
            await self._session.flush()

        await self._session.refresh(notification)
        return notification

    def _add_result_event(
        self, source: EventEnvelope, notification: Notification
    ) -> EventEnvelope:
        """Кладёт в outbox order.notified либо order.notification_failed."""
        succeeded = notification.status is NotificationStatus.SENT
        event_type = (
            EventType.ORDER_NOTIFIED
            if succeeded
            else EventType.ORDER_NOTIFICATION_FAILED
        )
        payload: dict[str, Any] = {
            'order_id': notification.order_id,
            'user_id': notification.user_id,
            'channel': notification.channel,
            'recipient': notification.recipient,
        }
        if not succeeded:
            payload['reason'] = notification.error

        result = EventEnvelope.caused_by(
            source,
            event_type=event_type,
            producer=Producer.NOTIFICATION,
            payload=payload,
        )
        self._session.add(
            OutboxEvent(
                **outbox_values(
                    result,
                    topic=str(Topic.NOTIFICATIONS),
                    max_attempts=settings.OUTBOX_MAX_ATTEMPTS,
                )
            )
        )
        return result

    async def list_notifications(
        self,
        *,
        limit: int,
        offset: int,
        order_id: int | None = None,
        saga_id: str | None = None,
    ) -> NotificationsList:
        stmt = (
            select(Notification)
            .order_by(Notification.id.desc())
            .limit(limit)
            .offset(offset)
        )
        count_stmt = select(func.count()).select_from(Notification)

        if order_id is not None:
            stmt = stmt.where(Notification.order_id == order_id)
            count_stmt = count_stmt.where(Notification.order_id == order_id)
        if saga_id is not None:
            stmt = stmt.where(Notification.saga_id == saga_id)
            count_stmt = count_stmt.where(Notification.saga_id == saga_id)

        items = list((await self._session.execute(stmt)).scalars().all())
        total = int((await self._session.execute(count_stmt)).scalar_one())

        return NotificationsList(
            items=[NotificationRead.model_validate(i) for i in items],
            total=total,
            limit=limit,
            offset=offset,
        )
