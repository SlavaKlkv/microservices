# Handoff: состояние работ по саге-хореографии

Документ для продолжения работы из любой сессии и с любого устройства.
Обновляется по мере закрытия этапов.

## Что за работа

Проект доводится от «микросервисы обмениваются одним событием» до полноценной
саги-хореографии с идемпотентностью, DLQ, тестами, CI и наблюдаемостью.

Целевая цепочка саги:

```
order.created → order.notify_requested → order.notified          → order.confirmed
                                       ↘ order.notification_failed → order.cancelled (компенсация)
```

Топики: `orders.events.v1`, `notifications.events.v1` и парные `*.dlq`.
Ключ партиционирования — `order_id`: так сохраняется порядок событий одного заказа.
`orders` не подписывается на собственный топик (иначе цикл), плюс в базовом
консьюмере есть фильтр `producer != self`.

## Договорённости

- Работа ведётся в ветке `develop`, PR не создаются, в `main` не вливаем.
- Коммиты: Conventional Commits, **заголовок на английском**, **тело на русском**
  (что и зачем). Указывать AI соавтором запрещено — никаких `Co-Authored-By`
  и footer'ов вида «Generated with…».
- Один этап = один коммит + push. После каждого этапа ruff и mypy зелёные.
- Код и докстринги комментируются по-русски; стиль: одинарные кавычки,
  длина строки 79 (конфиг ruff — в корневом `pyproject.toml`).
- В репозиторий попадают только `*.example`, реальные `.env*` игнорируются;
  в примерах — плейсхолдеры `change-me`.

## Ключевые архитектурные решения

- **Общий контракт вместо копипасты.** Репозиторий переведён в uv workspace:
  корневой `pyproject.toml`, общий пакет `packages/events` (`ms_events`) с
  `EventEnvelope`, справочником топиков и типов событий, `setup_logging` и
  middleware `X-Request-ID`. Один `uv.lock` и один конфиг линтеров на весь репо.
  Build-контекст Docker — корень репозитория, иначе общий пакет не попадает в образ.
- **Конверт события** несёт трассировку саги: `event_id`, `saga_id`,
  `correlation_id`, `causation_id`, `occurred_at`, `producer`, `payload`.
- **Outbox у каждого продюсера**: событие пишется в одной транзакции с бизнес-записью,
  отдельный воркер публикует его в Kafka с экспоненциальным backoff.
- **Идемпотентность** — таблица `processed_event` в Postgres, вставка
  `ON CONFLICT DO NOTHING` в той же транзакции, что и бизнес-запись.
  Redis (этап 9) добавляется только как быстрый путь перед Postgres,
  источником истины не становится.
- **Redis сознательно не заменяет Postgres**: отметка «обработано» обязана
  коммититься атомарно с бизнес-записью, иначе событие теряется при падении.

## Сделано (ветка `develop`)

| Коммит | Содержание |
|---|---|
| `style: apply ruff formatting…` | единый стиль в orders и orders_history |
| `chore: add docker env examples` | шаблоны `.env.docker`, `!.env*.example`, секреты → `change-me` |
| `fix(auth): log exceptions via structlog` | обработчики исключений пишут в общий структурированный вывод |
| `feat(notification): add initial notification service` | первичный вариант сервиса |
| `fix(orders): repair outbox worker and history consumer defects` | enum-статусы вместо строк, один продюсер на цикл, structlog-поля, graceful shutdown |
| `refactor: extract shared events package into uv workspace` | пакет `ms-events`, корневые конфиги, `/ready`, Docker-контекст из корня |
| `refactor: unify settings and add kafka configuration` | общие `DBSettings`/`KafkaSettings`/`RedisSettings`, `os.getenv` убран из воркеров |
| `feat(orders): publish events with a versioned envelope` | `EventEnvelope` в outbox, `saga_id`/`topic`/`max_attempts`, статус `DEAD`, DLQ |
| `feat(orders-history): consume envelopes with dlq and idempotency` | разбор конверта, DLQ вместо вечного poison pill, подписка на оба топика |
| `refactor(notification): rewrite service on fastapi and sqlalchemy` | вместо голого asyncpg и рантайм-DDL — FastAPI, SQLAlchemy, Alembic, SMTP из настроек |
| `feat(notification): publish saga result events` | дедуп, запись уведомления и outbox-строка `order.notified`/`order.notification_failed` в одной транзакции; свой outbox-воркер |

## Осталось

8. `feat(orders): complete saga with compensation` — консьюмер на
   `notifications.events.v1`, переходы `PENDING→CONFIRMED/CANCELLED`, таблица
   `order_saga`, идемпотентные переходы (повтор — no-op, не 409).
9. `feat(redis): add idempotency fast path` — `SET NX EX` перед Postgres,
   деградация при недоступности Redis.
10. `feat(infra): run all workers and notification service in compose` —
    `postgres-notifications`, все воркеры и консьюмеры отдельными сервисами,
    one-shot `*-migrate` (сейчас `alembic upgrade head` в command API-сервисов
    даёт гонку миграций), `kafka-init` с явными топиками, `/notifications/` в nginx.
11. `feat(observability): fix grafana provisioning and add saga metrics` —
    datasource-манифест вместо ошибочного scrape-конфига, монтирование
    provisioning, метрики outbox/консьюмеров/DLQ/длительности саги.
12. `test: cover services and saga with pytest` — unit, integration на
    testcontainers, e2e под маркером `e2e`.
13. `ci: add github actions pipeline` — lint, typecheck, test, migrations, build, e2e.
14. `docs: document saga architecture and commands` — полноценный корневой
    README с mermaid-диаграммой саги, README сервисов, `docs/adr/`.

## Открытые риски

- ~~Сборка образов после перехода на uv workspace не проверена~~ — **снято**:
  `docker compose build` на коммите `b8c859c` прошёл для auth, orders и
  orders-history, образ notification собран отдельной командой
  `docker build -f services/notification/Dockerfile .`. Правки Dockerfile'ов
  не потребовались.
- Миграции orders, orders_history и notification прогнаны на живом
  PostgreSQL 16 (`upgrade head`, `alembic check`, `downgrade base`) — расхождений
  моделей и миграций нет.
- `ALTER TYPE … ADD VALUE` для статуса `DEAD` необратим: downgrade миграции
  не возвращает enum в прежний вид.
- Дубли уведомлений при at-least-once: отправка письма идёт после коммита
  дедупликации, то есть возможен сценарий «записали, но не отправили».
  Выбор осознанный — лучше не отправить, чем отправить дважды.

## Как проверять

```bash
uv sync --all-packages
uv run ruff check services packages && uv run ruff format --check services packages
docker compose build && docker compose up -d --wait
```

E2E-сценарий саги: `POST /api/v1/orders` с токеном → в течение нескольких секунд
`GET /api/v1/orders/{id}` показывает `CONFIRMED`, а в истории заказа лежат четыре
события с одним `saga_id`. Failure-путь включается настройкой
`NOTIFICATION_FAIL_RATE=1.0` — заказ должен уйти в `CANCELLED`.
