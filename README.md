# Микросервисы заказов: сага-хореография на Kafka

[![CI](https://img.shields.io/github/actions/workflow/status/SlavaKlkv/microservices/ci.yml?style=flat-square&label=CI)](../../actions)

Четыре сервиса обмениваются событиями через Kafka и доводят заказ до
терминального статуса без оркестратора: каждый сервис сам решает, как
реагировать на чужое событие. Проект собран вокруг трёх вещей, которые
обычно и ломаются в распределённых системах, — доставки событий,
идемпотентности и наблюдаемости.

## Сага

```mermaid
sequenceDiagram
    autonumber
    participant C as Клиент
    participant O as orders
    participant K as Kafka
    participant N as notification
    participant H as orders-history

    C->>O: POST /api/v1/orders
    Note over O: заказ PENDING + два события<br/>в outbox одной транзакцией
    O-->>C: 201 Created

    O->>K: order.created
    O->>K: order.notify_requested
    K->>N: order.notify_requested

    alt письмо отправлено
        N->>K: order.notified
        K->>O: order.notified
        Note over O: PENDING → CONFIRMED
        O->>K: order.confirmed
    else отправить не удалось
        N->>K: order.notification_failed
        K->>O: order.notification_failed
        Note over O: PENDING → CANCELLED<br/>(компенсация)
        O->>K: order.cancelled
    end

    K->>H: все события саги
```

Цепочка целиком:

```
order.created → order.notify_requested → order.notified            → order.confirmed
                                       ↘ order.notification_failed → order.cancelled
```

Все события одной саги связаны общим `saga_id`, а `causation_id`
указывает на непосредственную причину — по ним восстанавливается порядок
даже когда события пришли вразнобой.

## Как это устроено

| Решение | Зачем | Подробно |
|---|---|---|
| Хореография, без оркестратора | нет единой точки отказа и «бога», знающего про все сервисы | [ADR-0001](docs/adr/0001-choreography-over-orchestration.md) |
| Транзакционный outbox | событие не может потеряться между коммитом в БД и отправкой в брокер | [ADR-0002](docs/adr/0002-transactional-outbox.md) |
| `processed_event` в Postgres, Redis — быстрый путь | доставка at-least-once, дубли неизбежны | [ADR-0003](docs/adr/0003-idempotency.md) |
| DLQ и ограниченные повторы | «отравленная таблетка» не должна намертво занимать партицию | [ADR-0004](docs/adr/0004-dlq-and-retries.md) |
| Общий пакет `ms-events` в uv workspace | контракт событий один на всех, а не скопирован четырежды | [ADR-0005](docs/adr/0005-shared-events-package.md) |
| Миграции — отдельные one-shot job'ы | несколько процессов на одну БД дерутся за `alembic_version` | [ADR-0006](docs/adr/0006-migrations-as-jobs.md) |
| Retention в outbox-воркере | обе служебные таблицы растут на каждое событие саги | [ниже](#уборка) |

Ключ партиционирования — `order_id`: события одного заказа всегда
попадают в одну партицию и сохраняют порядок. `orders` не подписан на
собственный топик (иначе цикл), плюс в базовом консьюмере стоит фильтр
по `producer`.

## Сервисы

| Сервис | Порт | Роль | README |
|---|---|---|---|
| `auth` | 8001 | пользователи и JWT | [services/auth](services/auth/README.md) |
| `orders` | 8002 | заказы, старт и завершение саги | [services/orders](services/orders/README.md) |
| `orders-history` | 8003 | журнал всех событий саги | [services/orders_history](services/orders_history/README.md) |
| `notification` | 8004 | уведомления и результат саги | [services/notification](services/notification/README.md) |

Плюс фоновые процессы: два outbox-воркера и три консьюмера. Общий код
контракта — в [`packages/events`](packages/events/README.md).

## Запуск

```bash
cp .env.example .env
for s in auth orders orders_history notification; do
  cp "services/$s/.env.docker.example" "services/$s/.env.docker"
done

docker compose build
docker compose up -d --wait
```

`*-migrate` и `kafka-init` — одноразовые job'ы: в `docker compose ps`
они показаны как `exited (0)`, это норма, а не падение.

Через nginx на `:80` сервисы доступны как `/auth/`, `/orders/`,
`/history/` и `/notifications/`.

| Что | Где |
|---|---|
| Swagger сервиса | `http://localhost:8002/docs` (и другие порты) |
| Kafka UI | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Письма (mailhog) | `http://localhost:8025` |

## Проверить сагу руками

```bash
BASE=http://localhost

# регистрация и токен
curl -sX POST $BASE/auth/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"demo","email":"demo@example.com","password":"Passw0rd1"}'

TOKEN=$(curl -sX POST $BASE/auth/api/v1/auth/login_json \
  -H 'Content-Type: application/json' \
  -d '{"login":"demo","password":"Passw0rd1"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["tokens"]["access_token"])')

# заказ создаётся в PENDING
ORDER=$(curl -sX POST $BASE/orders/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"total_price":"199.99"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# через пару секунд — CONFIRMED, никто не вмешивался
curl -s $BASE/orders/api/v1/orders/$ORDER

# в истории — четыре события с одним saga_id
curl -s "$BASE/history/api/v1/orders_history/history?order_id=$ORDER"
```

Ветка компенсации включается `NOTIFICATION_FAIL_RATE=1.0` в
`services/notification/.env.docker` — заказ должен уйти в `CANCELLED`
с заполненным `cancel_reason`.

## Разработка

```bash
uv sync --all-packages

uv run ruff check tests services packages
uv run ruff format --check tests services packages
uv run mypy packages services tests
```

Тесты разложены на три уровня и включаются разными условиями —
недоступность окружения даёт skip, а не падение:

```bash
# unit — работают всегда
uv run pytest tests/unit

# integration — своя база или, если её нет, testcontainers
TEST_POSTGRES_DSN=postgresql://postgres:postgres@localhost:5432/postgres \
  uv run pytest tests

# e2e — по поднятому compose
E2E_BASE_URL=http://localhost uv run pytest tests/e2e
```

Миграции сервиса:

```bash
PYTHONPATH=services uv run alembic -c services/orders/alembic.ini upgrade head
```

## Уборка

`outbox` и `processed_event` пополняются на каждое событие саги, поэтому
раз в `OUTBOX_CLEANUP_INTERVAL_SEC` (по умолчанию час) outbox-воркер
удаляет то, что уже сделало свою работу:

| Что удаляется | Через сколько | Почему можно |
|---|---|---|
| строки `outbox` в статусе `SENT` | `OUTBOX_RETENTION_DAYS`, по умолчанию 7 | событие уже в брокере, строка — только след публикации |
| отметки в `processed_event` | `PROCESSED_EVENT_RETENTION_DAYS`, по умолчанию 30 | повтор доставки через месяц после события практически невозможен |

Не удаляется ничего, что ещё нужно: строки `NEW` и `ERROR` ждут отправки,
а `DEAD` — единственный след события, ушедшего в DLQ, и разбирают его
руками. Ноль в любой из переменных отключает соответствующую уборку.

## Наблюдаемость

API-сервисы отдают `/metrics` сами, воркерам и консьюмерам поднимается
отдельный сервер на `METRICS_PORT` — иначе вся асинхронная половина
системы, ровно та, где копятся отказы, остаётся невидимой.

| Метрика | О чём говорит |
|---|---|
| `outbox_events_published_total` | события доезжают до брокера |
| `outbox_events_dead_total` | попытки исчерпаны, событие в DLQ |
| `consumer_events_total{outcome}` | processed / duplicate / failed / invalid |
| `consumer_dlq_total` | **в норме ноль** — единственная метрика под алерт |
| `outbox_rows_pruned_total` | сколько строк забрала уборка (`table`: outbox / processed_event) |
| `saga_duration_seconds` | сколько живёт сага до терминального статуса |

Дашборд «Microservices Saga Overview» появляется в Grafana сам через
провижининг.

## Известные ограничения

Честный список того, что осознанно не сделано или сделано с оговоркой.

* **Разбор DLQ — ручная работа.** Никакой автоматики переигрывания нет.
  Поэтому `consumer_dlq_total` и `outbox_events_dead_total` — метрики
  под алерт: в норме там ноль.
* **Уведомление может быть записано, но не отправлено.** Письмо уходит
  после коммита дедупликации, так что падение между этими шагами теряет
  письмо. Выбор осознанный: лучше не отправить, чем отправить дважды.
* **Downgrade миграции `a1c4f2e7b301` необратим не полностью.**
  `ALTER TYPE … ADD VALUE` для статуса `DEAD` в PostgreSQL не
  откатывается: при откате строки переводятся в `ERROR`, но само
  значение остаётся в типе.
* **Сквозной прогон саги через настоящую Kafka выполняется только в
  CI.** Локально его закрывают job `e2e` и `tests/e2e`; на машине без
  docker эти тесты пропускаются, а не падают.

## Соглашения

* код и докстринги — по-русски, одинарные кавычки, длина строки 79;
* заголовок коммита по Conventional Commits на английском, тело — на
  русском: что сделано и зачем;
* в репозиторий попадают только `*.example`, реальные `.env*`
  игнорируются, секреты в примерах — плейсхолдеры `change-me`.
