from __future__ import annotations

from collections.abc import AsyncGenerator

from notification.settings import settings
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


engine: AsyncEngine = create_async_engine(
    settings.db_connection_url,
    echo=settings.DB_ECHO,
    pool_pre_ping=True,
)

SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


async def dispose_engine() -> None:
    """
    Корректно закрыть соединения (например, при завершении приложения).
    """
    await engine.dispose()
