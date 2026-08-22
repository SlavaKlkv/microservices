# orders

Владелец заказов. Начинает сагу при создании заказа и завершает её,
когда приходит результат уведомления.

## Роль в саге

```
POST /orders  →  заказ PENDING + order.created + order.notify_requested
                 (одной транзакцией через outbox)

order.notified            →  PENDING → CONFIRMED  →  order.confirmed
order.notification_failed →  PENDING → CANCELLED  →  order.cancelled
```

Переходы идемпотентны: событие на заказ, уже находящийся в целевом
статусе, — тихий no-op, а не 409. Заказ, завершённый вручную через API,
сага не трогает — оператор главнее.

## Процессы

| Команда | Что делает |
|---|---|
| `uvicorn orders.main:app` | HTTP API |
| `python -m orders.outbox_worker` | публикует накопленные события в Kafka |
| `python -m orders.consumer` | слушает `notifications.events.v1`, завершает сагу |

Консьюмер подписан **только** на топик уведомлений: подписка на
собственный топик замкнула бы сагу в бесконечный цикл.

## API

| Метод | Путь | |
|---|---|---|
| POST | `/api/v1/orders` | создать заказ (нужен Bearer-токен) |
| GET | `/api/v1/orders/{id}` | заказ со статусом |
| GET | `/api/v1/orders` | список с пагинацией |
| PATCH | `/api/v1/orders/{id}` | изменить цену |
| POST | `/api/v1/orders/{id}/confirm` | подтвердить вручную |
| POST | `/api/v1/orders/{id}/cancel` | отменить вручную |
| DELETE | `/api/v1/orders/{id}` | удалить |

Плюс `/health`, `/ready` и `/metrics`.

## Таблицы

| Таблица | Зачем |
|---|---|
| `orders` | заказ, его статус и `cancel_reason` |
| `outbox` | события, ждущие публикации (ADR-0002) |
| `processed_event` | идемпотентность консьюмера (ADR-0003) |
| `order_saga` | текущий шаг саги, для разбора инцидентов и метрики длительности |

## Настройки

Полный список — в `.env.docker.example`. Существенное:
`AUTH_SERVICE_URL` для проверки токена, `OUTBOX_*` для воркера,
`CONSUMER_*` и `REDIS_*` для консьюмера.
