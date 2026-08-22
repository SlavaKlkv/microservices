from pathlib import Path

from pydantic_settings import SettingsConfigDict

from ms_events import DBSettings, KafkaSettings, RedisSettings, ServiceSettings

SERVICE_DIR = Path(__file__).resolve().parent


class Settings(
    ServiceSettings,
    DBSettings,
    KafkaSettings,
    RedisSettings,
):
    """Настройки сервиса заказов."""

    model_config = SettingsConfigDict(
        env_file=SERVICE_DIR / '.env', extra='ignore'
    )

    DB_NAME: str = 'orders_db'
    KAFKA_GROUP_ID: str = 'orders-saga'

    #: Базовый URL сервиса авторизации для проверки токена.
    AUTH_SERVICE_URL: str = 'http://127.0.0.1:8000'


settings = Settings()
