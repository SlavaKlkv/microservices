from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from notification.settings import settings


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


async def db_ping() -> bool:
    """Быстрая проверка готовности БД для /ready."""
    try:
        async with SessionLocal() as session:
            await session.execute(text('SELECT 1'))
        return True
    except Exception:
        return False


async def dispose_engine() -> None:
    """
    Корректно закрыть соединения (например, при завершении приложения).
    """
    await engine.dispose()
