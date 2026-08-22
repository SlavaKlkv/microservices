from __future__ import annotations

import asyncio
import json
import os
import signal
import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText
from typing import Any

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer

logger = structlog.get_logger()


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic: str
    group_id: str

    database_url: str
    consumer_log_level: str

    @classmethod
    def from_env(cls) -> 'Settings':
        in_docker = os.getenv('IN_DOCKER', '').lower() in {
            '1',
            'true',
            'yes',
        }

        bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        if not bootstrap:
            bootstrap = 'kafka:9092' if in_docker else 'localhost:9092'

        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            db_host = os.getenv('DB_HOST') or (
                'postgres' if in_docker else 'localhost'
            )
            db_port = os.getenv('DB_PORT') or '5432'
            db_user = os.getenv('DB_USER') or 'app'
            db_password = os.getenv('DB_PASSWORD') or 'app'
            db_name = os.getenv('DB_NAME') or 'notification_db'
            db_url = (
                f'postgresql://{db_user}:{db_password}'
                f'@{db_host}:{db_port}/{db_name}'
            )

        return cls(
            bootstrap_servers=bootstrap,
            topic=os.getenv('KAFKA_TOPIC', 'orders.events'),
            group_id=os.getenv('KAFKA_GROUP_ID', 'notification-service'),
            database_url=db_url,
            consumer_log_level=os.getenv('LOG_LEVEL', 'INFO'),
        )


def _parse_message(value: bytes) -> dict[str, Any]:
    """Парсит сообщение из Kafka.

    Ожидаем JSON-объект (dict). Payload может лежать как на верхнем уровне,
    так и в поле `payload` (как у outbox-паттерна).
    """
    try:
        raw = value.decode('utf-8')
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError('message is not a JSON object')
        return data
    except Exception as exc:
        raise ValueError(f'cannot parse message as JSON: {exc}') from exc


def _extract_event(data: dict[str, Any]) -> dict[str, Any]:
    """Нормализует событие под единый формат для нотификаций."""
    payload = (
        data.get('payload') if isinstance(data.get('payload'), dict) else {}
    )

    event_type = (
        data.get('event_type') or data.get('type') or payload.get('event_type')
    )

    # event_id: предпочтительно уникальный UUID/строка из outbox; иначе fallback.
    event_id = (
        data.get('event_id')
        or payload.get('event_id')
        or data.get('id')
        or payload.get('id')
    )

    order_id = data.get('order_id') or payload.get('order_id')
    user_id = data.get('user_id') or payload.get('user_id')

    # Полезно хранить «сырой» payload, чтобы потом расширять виды уведомлений.
    normalized_payload = payload if payload else data

    return {
        'event_id': event_id,
        'event_type': event_type,
        'order_id': order_id,
        'user_id': user_id,
        'payload': normalized_payload,
    }


async def _ensure_schema(conn: asyncpg.Connection) -> None:
    """Создаёт таблицу нотификаций, если её ещё нет.

    Для проекта с Alembic это можно убрать и держать миграции отдельно.
    Но локально/на старте контейнера это сильно упрощает запуск.
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_db (
            id BIGSERIAL PRIMARY KEY,
            event_id TEXT UNIQUE NOT NULL,
            event_type TEXT NOT NULL,
            order_id BIGINT NULL,
            user_id BIGINT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS ix_notifications_user_id ON notification_db (user_id);
        CREATE INDEX IF NOT EXISTS ix_notifications_order_id ON notification_db (order_id);
        CREATE INDEX IF NOT EXISTS ix_notifications_event_type ON notification_db (event_type);
        """
    )


# Функция отправки email уведомлений
async def send_email_notification(user_email: str, message: str) -> None:
    msg = MIMEText(message)
    msg['Subject'] = 'Your Notification'
    msg['From'] = 'noreply@example.com'
    msg['To'] = user_email

    with smtplib.SMTP('smtp.example.com') as server:
        server.login('user', 'password')
        server.sendmail('noreply@example.com', user_email, msg.as_string())


async def _save_notification(
    pool: asyncpg.Pool, event: dict[str, Any]
) -> bool:
    """Сохраняет уведомление в БД.

    Возвращает True, если вставка произошла, и False если событие уже было обработано.
    """
    event_id = event.get('event_id')
    if not event_id:
        raise ValueError('event_id is required for idempotency')

    async with pool.acquire() as conn:
        # ON CONFLICT — ключевой момент: consumer может получать одно событие повторно.
        res = await conn.execute(
            """
            INSERT INTO notification_db (event_id, event_type, order_id, user_id, payload)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            str(event_id),
            str(event.get('event_type') or 'Unknown'),
            event.get('order_id'),
            event.get('user_id'),
            json.dumps(event.get('payload') or {}, ensure_ascii=False),
        )

    # Если вставка успешна, отправляем уведомление
    if res.endswith(' 1'):
        user_email = event.get('user_email')
        if user_email:
            message = f'Your order {event.get("order_id")} has been processed successfully.'
            await send_email_notification(user_email, message)

    # asyncpg возвращает строку вида "INSERT 0 1" или "INSERT 0 0"
    return res.endswith(' 1')


async def run_consumer(settings: Settings) -> None:
    stop_event = asyncio.Event()

    def _request_stop(*_: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            # На некоторых платформах/средах add_signal_handler может быть недоступен.
            pass

    # DB
    pool: asyncpg.Pool | None = None
    consumer: AIOKafkaConsumer | None = None

    try:
        pool = await asyncpg.create_pool(
            settings.database_url, min_size=1, max_size=5
        )
        async with pool.acquire() as conn:
            await _ensure_schema(conn)

        # Kafka
        consumer = AIOKafkaConsumer(
            settings.topic,
            bootstrap_servers=settings.bootstrap_servers,
            group_id=settings.group_id,
            enable_auto_commit=True,
            auto_offset_reset='earliest',
            value_deserializer=lambda b: b,
        )

        await consumer.start()
        logger.info(
            'notification_consumer_started',
            bootstrap=settings.bootstrap_servers,
            topic=settings.topic,
            group_id=settings.group_id,
            database_url=_mask_db_url(settings.database_url),
        )

        while not stop_event.is_set():
            try:
                msg = await consumer.getone()
            except Exception:
                logger.exception('notification_consumer_poll_failed')
                await asyncio.sleep(1)
                continue

            try:
                data = _parse_message(msg.value)
                event = _extract_event(data)

                saved = await _save_notification(pool, event)

                logger.info(
                    'notification_event_processed',
                    saved=saved,
                    event_type=event.get('event_type'),
                    event_id=event.get('event_id'),
                    order_id=event.get('order_id'),
                    user_id=event.get('user_id'),
                    offset=msg.offset,
                    partition=msg.partition,
                )

            except Exception:
                logger.exception(
                    'notification_event_processing_failed',
                    offset=getattr(msg, 'offset', None),
                    partition=getattr(msg, 'partition', None),
                )

    finally:
        if consumer is not None:
            try:
                await consumer.stop()
            except Exception:
                logger.exception('notification_consumer_stop_failed')
            else:
                logger.info('notification_consumer_stopped')

        if pool is not None:
            try:
                await pool.close()
            except Exception:
                logger.exception('notification_db_pool_close_failed')


def _mask_db_url(url: str) -> str:
    # Логировать пароль не надо.
    # Поддерживаем самые частые форматы: postgresql://user:pass@host:port/db
    try:
        if '@' not in url or '://' not in url:
            return url
        scheme, rest = url.split('://', 1)
        creds, tail = rest.split('@', 1)
        if ':' in creds:
            user = creds.split(':', 1)[0]
            return f'{scheme}://{user}:***@{tail}'
        return url
    except Exception:
        return '***'


def main() -> None:
    settings = Settings.from_env()
    asyncio.run(run_consumer(settings))


if __name__ == '__main__':
    main()
