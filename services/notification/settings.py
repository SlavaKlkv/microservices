from pathlib import Path

from ms_events import DBSettings, KafkaSettings, RedisSettings, ServiceSettings
from pydantic_settings import SettingsConfigDict

SERVICE_DIR = Path(__file__).resolve().parent


class Settings(
    ServiceSettings,
    DBSettings,
    KafkaSettings,
    RedisSettings,
):
    """Настройки сервиса уведомлений."""

    model_config = SettingsConfigDict(
        env_file=SERVICE_DIR / '.env', extra='ignore'
    )

    DB_NAME: str = 'notification_db'
    KAFKA_GROUP_ID: str = 'notification'

    #: Реальная отправка писем выключена по умолчанию: иначе тесты и CI
    #: будут висеть на недоступном SMTP-хосте.
    NOTIFICATION_ENABLED: bool = False
    SMTP_HOST: str = 'localhost'
    SMTP_PORT: int = 1025
    SMTP_USER: str = ''
    SMTP_PASSWORD: str = ''
    SMTP_FROM: str = 'noreply@example.com'
    SMTP_USE_TLS: bool = False
    SMTP_TIMEOUT_SEC: float = 10.0

    #: Доля искусственных отказов отправки (0.0..1.0) — нужна, чтобы
    #: проверять компенсирующую ветку саги без настоящего SMTP.
    NOTIFICATION_FAIL_RATE: float = 0.0


settings = Settings()
