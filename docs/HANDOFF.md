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
- **Миграции — отдельные one-shot job'ы** `*-migrate`, а не `alembic upgrade
  head` в `command` API-сервиса. Иначе несколько процессов одной БД (API,
  воркер, консьюмер) стартуют одновременно и дерутся за `alembic_version`.
  Всё, что ходит в БД, ждёт `service_completed_successfully` своей миграции.
- **Топики заводит `kafka-init`**, автосоздание в брокере выключено: иначе
  первый подписавшийся консьюмер создаёт топик на одну партицию, и
  параллелизм по `order_id` теряется навсегда. Основные топики — по
  `KAFKA_TOPIC_PARTITIONS` партиций, DLQ — одна.
- **Метрики воркеров живут на отдельном порту.** API-сервисы отдают
  `/metrics` через instrumentator FastAPI, а воркеры и консьюмеры HTTP не
  слушают вообще — им поднимается отдельный сервер на `METRICS_PORT`.
  Иначе вся асинхронная половина системы, ровно та, где копятся отказы,
  остаётся невидимой для Prometheus.
- **Три уровня тестов включаются разными условиями**, и недоступность
  окружения даёт skip, а не падение: unit работают всегда; integration
  берут базу из `TEST_POSTGRES_DSN`, иначе поднимают testcontainers,
  иначе пропускаются; e2e включаются только при заданном `E2E_BASE_URL`.
  Сломанный код и ненастроенная машина обязаны выглядеть по-разному.
- **Alembic в тестах запускается подпроцессом**, как job `*-migrate` в
  compose: `env.py` читает настройки сервиса при импорте, подменить их
  в уже загруженном процессе нельзя.
- **`known-first-party` зафиксирован явно** в конфиге ruff: пакеты сервисов
  лежат в `services/<name>`, а импортируются коротким именем, и разные
  версии ruff классифицируют их по-разному.

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
| `feat(orders): complete saga with compensation` | сага-консьюмер на `notifications.events.v1`, `PENDING→CONFIRMED/CANCELLED`, `order_saga`, идемпотентные переходы |
| `feat(redis): add idempotency fast path` | `SET NX EX 24h` перед Postgres во всех консьюмерах, снятие заявки при неудаче, деградация без Redis |
| `style: pin isort first-party packages` | `known-first-party` в конфиге ruff, порядок импортов приведён к нему во всём репо |
| `feat(infra): run all workers and notification service in compose` | `postgres-notifications`, notification-service, 5 воркеров и консьюмеров отдельными сервисами, one-shot `*-migrate` и `kafka-init`, mailhog, `/notifications/` в nginx |
| `feat(observability): fix grafana provisioning and add saga metrics` | datasource-манифест вместо scrape-конфига, монтирование provisioning, метрики outbox/консьюмеров/DLQ/длительности саги, дашборд саги |
| `test: cover services and saga with pytest` | 92 теста: unit на конверт/DLQ/идемпотентность/метрики, integration на живом Postgres (сага, компенсация, миграции), e2e под маркером |
| `ci: add github actions pipeline` | lint/mypy, тесты на postgres-сервисе, сборка четырёх образов матрицей, e2e на поднятом compose |

## Осталось

14. `docs: document saga architecture and commands` — полноценный корневой
    README с mermaid-диаграммой саги, README сервисов, `docs/adr/`.

## Открытые риски

- ~~Сборка образов после перехода на uv workspace не проверена~~ — **снято**:
  `docker compose build` на коммите `b8c859c` прошёл для auth, orders и
  orders-history, образ notification собран отдельной командой
  `docker build -f services/notification/Dockerfile .`. Правки Dockerfile'ов
  не потребовались.
- Миграции orders, orders_history и notification прогнаны на живом
  PostgreSQL 16 (`upgrade head`, `alembic check`, `downgrade base`, повторный
  `upgrade head`) — расхождений моделей и миграций нет. Попутно исправлено:
  downgrade миграции `5bfab7e344e7` не удалял тип `outbox_status`, из-за чего
  повторный upgrade падал на `CREATE TYPE`.
- Сага проверена вживую на PostgreSQL: создание заказа пишет `order.created`
  и `order.notify_requested`, `order.notified` переводит заказ в `CONFIRMED`,
  повтор того же события — no-op, `order.notification_failed` даёт `CANCELLED`
  с `cancel_reason` и событием `order.cancelled`. Kafka в этой проверке не
  участвовала — сервисный слой вызывался напрямую.
- `ALTER TYPE … ADD VALUE` для статуса `DEAD` необратим: downgrade миграции
  не возвращает enum в прежний вид.
- Redis-путь проверен на живом контейнере: повторный `claim` возвращает
  `False`, после `release` снова `True`, при недоступном Redis консьюмер
  переходит в деградированный режим и продолжает работать через Postgres.
- Сквозная проверка через настоящую Kafka **не делалась** и на этапе 12:
  e2e-тесты написаны, но без docker-демона их не на чем прогнать. Они
  пропускаются, пока не задан `E2E_BASE_URL`. Всё остальное — 92 теста —
  проходит, включая сагу и компенсацию на живом PostgreSQL 16.
- Новый `docker-compose.yml` **не поднимался вживую**: в сессии, где он
  писался, не было docker-демона. Проверено только `docker compose config`
  (25 сервисов рендерятся, якоря и `depends_on` разворачиваются верно) и
  импорт всех точек входа воркеров и консьюмеров. Первый реальный
  `up -d --wait` — обязательная часть этапа 12.
- `orders_history` тянул `psycopg2` из исходников, остальные сервисы —
  `psycopg2-binary`. Из-за этого `uv sync --all-packages` падал на машинах
  без `libpq-dev`. Выровнено на `psycopg2-binary`, `uv.lock` пересобран.
- Дашборд Grafana и scrape-конфиг Prometheus **не проверены на живых
  контейнерах** — тот же docker-демон. Проверено: YAML обоих файлов
  разбирается, datasource-манифест имеет корректную структуру
  (`apiVersion: 1` + `datasources`), дашборд — валидный JSON на 11
  панелей, метрики регистрируются и отдаются через `/metrics`
  (дымовой прогон `generate_latest` + поднятый сервер).
- Пайплайн CI **не прогонялся**: workflow нельзя запустить из сессии без
  docker и без пуша в GitHub. Проверено то, что проверяемо: YAML
  разбирается, четыре job'а на месте, а шаги lint и test — это ровно те
  команды, которые здесь отработали зелёными, включая
  `UV_FROZEN=1 uv sync --all-packages`. Первый реальный прогон job'ов
  build и e2e произойдёт на первом же пуше.
- Дубли уведомлений при at-least-once: отправка письма идёт после коммита
  дедупликации, то есть возможен сценарий «записали, но не отправили».
  Выбор осознанный — лучше не отправить, чем отправить дважды.

## Как проверять

```bash
cp .env.example .env
for s in auth orders orders_history notification; do
  cp services/$s/.env.docker.example services/$s/.env.docker
done

uv sync --all-packages
uv run ruff check services packages && uv run ruff format --check services packages
uv run mypy packages services tests

# unit — всегда; integration — на своей базе или через testcontainers
uv run pytest tests/unit
TEST_POSTGRES_DSN=postgresql://postgres:postgres@localhost:5432/postgres \
  uv run pytest tests

docker compose build && docker compose up -d --wait

# e2e — по поднятому стеку
E2E_BASE_URL=http://localhost uv run pytest tests/e2e
```

Наблюдаемость: Prometheus на `${PROMETHEUS_PORT}` (таргеты — `api` и
`workers`), Grafana на `${GRAFANA_PORT}`, дашборд «Microservices Saga
Overview» появляется сам через провижининг. Ключевая панель — «В DLQ за
сутки»: в норме там ноль.

`docker compose config --quiet` валидирует файл без запущенного демона.
`*-migrate` и `kafka-init` — one-shot job'ы: в `docker compose ps` они
показаны как `exited (0)`, это норма, а не падение. Список топиков виден
в логах `kafka-init` и в kafka-ui на `:8080`. Письма сервиса уведомлений —
в mailhog на `${MAILHOG_HTTP_PORT}` (реальная отправка включается
`NOTIFICATION_ENABLED=true`).

E2E-сценарий саги: `POST /api/v1/orders` с токеном → в течение нескольких секунд
`GET /api/v1/orders/{id}` показывает `CONFIRMED`, а в истории заказа лежат четыре
события с одним `saga_id`. Failure-путь включается настройкой
`NOTIFICATION_FAIL_RATE=1.0` — заказ должен уйти в `CANCELLED`.

Через nginx (`:${NGINX_HTTP_PORT}`) сервисы доступны как `/auth/`, `/orders/`,
`/history/` и `/notifications/`.
