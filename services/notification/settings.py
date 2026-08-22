from pathlib import Path
from urllib.parse import quote

from pydantic_settings import BaseSettings, SettingsConfigDict

SERVICE_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    DB_HOST: str = 'localhost'
    DB_PORT: int = 5432
    DB_USER: str = 'app'
    DB_PASSWORD: str = 'app'
    DB_NAME: str = 'notification_db'
    DB_ECHO: bool = False

    ALGORITHM: str = 'HS256'
    ACCESS_TTL_MIN: int = 15
    REFRESH_TTL_DAYS: int = 7

    model_config = SettingsConfigDict(
        env_file=SERVICE_DIR / '.env', extra='ignore'
    )

    @property
    def db_connection_url(self) -> str:
        """Асинхронный URL (используется приложением)."""
        user = quote(self.DB_USER)
        password = quote(self.DB_PASSWORD)
        hostport = (
            f'{self.DB_HOST}:{self.DB_PORT}' if self.DB_PORT else self.DB_HOST
        )
        return (
            f'postgresql+asyncpg://{user}:{password}@{hostport}/{self.DB_NAME}'
        )

    @property
    def db_connection_url_sync(self) -> str:
        """Синхронный URL (Alembic / инструменты)."""
        user = quote(self.DB_USER)
        password = quote(self.DB_PASSWORD)
        hostport = (
            f'{self.DB_HOST}:{self.DB_PORT}' if self.DB_PORT else self.DB_HOST
        )
        return f'postgresql://{user}:{password}@{hostport}/{self.DB_NAME}'


settings = Settings()
