from pathlib import Path

from pydantic_settings import SettingsConfigDict

from ms_events import DBSettings, ServiceSettings

SERVICE_DIR = Path(__file__).resolve().parent


class Settings(ServiceSettings, DBSettings):
    """Настройки сервиса авторизации."""

    model_config = SettingsConfigDict(
        env_file=SERVICE_DIR / '.env', extra='ignore'
    )

    DB_NAME: str = 'auth_db'

    AUTH_JWT_SECRET: str

    ALGORITHM: str = 'HS256'
    ACCESS_TTL_MIN: int = 15
    REFRESH_TTL_DAYS: int = 7


settings = Settings()  # type: ignore[call-arg]
