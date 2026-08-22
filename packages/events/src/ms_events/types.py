"""Справочник топиков и типов событий проекта."""

from enum import StrEnum


class Topic(StrEnum):
    """Kafka-топики.

    Один топик на сервис-владелец агрегата: так сохраняется порядок событий
    внутри одного заказа (ключ партиционирования — order_id).
    """

    ORDERS = 'orders.events.v1'
    ORDERS_DLQ = 'orders.events.v1.dlq'
    NOTIFICATIONS = 'notifications.events.v1'
    NOTIFICATIONS_DLQ = 'notifications.events.v1.dlq'

    @property
    def dlq(self) -> 'Topic':
        """DLQ-топик, парный данному."""
        if self.name.endswith('_DLQ'):
            return self
        return Topic[f'{self.name}_DLQ']


class EventType(StrEnum):
    """Типы событий саги и сопутствующих доменных фактов."""

    ORDER_CREATED = 'order.created'
    ORDER_NOTIFY_REQUESTED = 'order.notify_requested'
    ORDER_NOTIFIED = 'order.notified'
    ORDER_NOTIFICATION_FAILED = 'order.notification_failed'
    ORDER_CONFIRMED = 'order.confirmed'
    ORDER_CANCELLED = 'order.cancelled'
    ORDER_PRICE_CHANGED = 'order.price_changed'


class Producer(StrEnum):
    """Сервисы-издатели событий."""

    ORDERS = 'orders'
    NOTIFICATION = 'notification'


#: Какой сервис в какой топик публикует.
TOPIC_BY_PRODUCER: dict[Producer, Topic] = {
    Producer.ORDERS: Topic.ORDERS,
    Producer.NOTIFICATION: Topic.NOTIFICATIONS,
}
