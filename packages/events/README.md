# ms-events

Общий контракт событий и всё, что обязано быть одинаковым во всех
сервисах саги. Почему это отдельный пакет, а не копия в каждом сервисе —
[ADR-0005](../../docs/adr/0005-shared-events-package.md).

## Что внутри

| Модуль | Что даёт |
|---|---|
| `envelope` | `EventEnvelope` — конверт события с трассировкой саги |
| `types` | справочники `Topic`, `EventType`, `Producer` |
| `db` | миксины таблиц `outbox` и `processed_event`, `outbox_values` |
| `outbox` | `OutboxWorker` — публикация накопленных событий с backoff |
| `consumer` | `EventConsumer` — чтение топиков, DLQ, ограниченные повторы |
| `producer` | `EventProducer` и узкий протокол `RawSender` |
| `idempotency` | `IdempotencyCache` — быстрый путь на Redis |
| `metrics` | метрики outbox, консьюмеров, DLQ и длительности саги |
| `retry` | `backoff_seconds` — экспоненциальная задержка с потолком |
| `logging` | `setup_logging` — структурированный вывод |
| `middleware` | `RequestIdMiddleware` — сквозной `X-Request-ID` |
| `settings` | базовые `ServiceSettings`, `DBSettings`, `KafkaSettings`, `RedisSettings` |

Переиспользуются миксины, а не готовые модели: у каждого сервиса своя
база и свой `Base`.

## Конверт события

```python
EventEnvelope(
    event_type=EventType.ORDER_CREATED,
    saga_id=saga_id,          # общий на всю цепочку
    correlation_id=req_id,    # X-Request-ID исходного запроса
    causation_id=parent_id,   # event_id события-причины
    producer=Producer.ORDERS,
    aggregate_id='42',        # он же ключ партиционирования
    payload={...},
)
```

Схема закрыта (`extra='forbid'`): опечатка в имени поля падает при
разборе, а не теряется молча по дороге к подписчику.

## Подключение

```toml
dependencies = ["ms-events"]

[tool.uv.sources]
ms-events = { workspace = true }
```
