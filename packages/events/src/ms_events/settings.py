"""Базовые классы настроек, общие для всех микросервисов.

Каждый сервис наследует нужные ему миксины и добавляет свои поля,
поэтому имена переменных окружения одинаковы во всём проекте.
"""

from urllib.parse import quote

from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceSettings(BaseSettings):
    """Общие параметры процесса: логирование и порт метрик."""

    model_config = SettingsConfigDict(extra='ignore')

    LOG_LEVEL: str = 'INFO'
    #: Порт, на котором воркер/консьюмер поднимает /metrics.
    METRICS_PORT: int = 9100


class DBSettings(BaseSettings):
    """Подключение к PostgreSQL сервиса."""

    model_config = SettingsConfigDict(extra='ignore')

    DB_HOST: str = 'localhost'
    DB_PORT: int = 5432
    DB_USER: str = 'app'
    DB_PASSWORD: str = 'app'
    DB_NAME: str = 'app_db'
    DB_ECHO: bool = False

    def _dsn(self, driver: str) -> str:
        user = quote(self.DB_USER)
        password = quote(self.DB_PASSWORD)
        hostport = (
            f'{self.DB_HOST}:{self.DB_PORT}' if self.DB_PORT else self.DB_HOST
        )
        return f'{driver}://{user}:{password}@{hostport}/{self.DB_NAME}'

    @property
    def db_connection_url(self) -> str:
        """Асинхронный URL (используется приложением)."""
        return self._dsn('postgresql+asyncpg')

    @property
    def db_connection_url_sync(self) -> str:
        """Синхронный URL (Alembic и инструменты)."""
        return self._dsn('postgresql')


class KafkaSettings(BaseSettings):
    """Параметры брокера, outbox-воркера и консьюмеров."""

    model_config = SettingsConfigDict(extra='ignore')

    KAFKA_BOOTSTRAP_SERVERS: str = 'localhost:9092'
    KAFKA_CLIENT_ID: str = 'microservices-local'
    KAFKA_GROUP_ID: str = 'default'
    KAFKA_AUTO_OFFSET_RESET: str = 'earliest'
    KAFKA_REQUEST_TIMEOUT_MS: int = 10_000

    #: Outbox-воркер.
    OUTBOX_POLL_INTERVAL_SEC: float = 1.0
    OUTBOX_BATCH_SIZE: int = 50
    OUTBOX_BACKOFF_BASE_SEC: float = 1.0
    OUTBOX_BACKOFF_CAP_SEC: float = 60.0
    #: После стольких неудачных попыток строка уходит в статус DEAD и DLQ.
    OUTBOX_MAX_ATTEMPTS: int = 10

    #: Retention. Отправленные строки outbox и отметки об обработанных
    #: событиях нужны недолго: первые — как след публикации, вторые — пока
    #: возможен повтор доставки. Держать их вечно значит растить таблицы
    #: без ограничения. 0 отключает уборку.
    OUTBOX_RETENTION_DAYS: int = 7
    PROCESSED_EVENT_RETENTION_DAYS: int = 30
    #: Как часто воркер занимается уборкой.
    OUTBOX_CLEANUP_INTERVAL_SEC: float = 3600.0

    #: Консьюмеры.
    CONSUMER_MAX_ATTEMPTS: int = 5
    CONSUMER_BACKOFF_BASE_SEC: float = 0.5
    CONSUMER_BACKOFF_CAP_SEC: float = 30.0


class RedisSettings(BaseSettings):
    """Redis как быстрый путь идемпотентности (не источник истины)."""

    model_config = SettingsConfigDict(extra='ignore')

    REDIS_ENABLED: bool = False
    REDIS_URL: str = 'redis://localhost:6379/0'
    #: TTL ключа идемпотентности, по умолчанию сутки.
    IDEMPOTENCY_TTL_SEC: int = 86_400
