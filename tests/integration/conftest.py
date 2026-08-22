"""Интеграционные тесты: живой PostgreSQL.

Источник базы выбирается в таком порядке:

1. ``TEST_POSTGRES_DSN`` — уже поднятый сервер (CI, локальный postgres);
2. testcontainers — контейнер поднимается на время сессии;
3. если нет ни того, ни другого — тесты пропускаются.

Каждый сервис получает свою базу: схемы у них разные и специально не
пересекаются, а общая база спрятала бы ошибку в миграциях.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

#: Сервис -> (каталог с alembic.ini, имя тестовой базы).
SERVICES = {
    'orders': ('services/orders/alembic.ini', 'test_orders'),
    'orders_history': (
        'services/orders_history/alembic.ini',
        'test_orders_history',
    ),
    'notification': (
        'services/notification/alembic.ini',
        'test_notification',
    ),
}


@dataclass(frozen=True)
class PostgresServer:
    """Координаты сервера, на котором заводятся тестовые базы."""

    host: str
    port: int
    user: str
    password: str

    def dsn(self, database: str, *, driver: str = 'postgresql') -> str:
        return (
            f'{driver}://{self.user}:{self.password}'
            f'@{self.host}:{self.port}/{database}'
        )


def _from_env() -> PostgresServer | None:
    dsn = os.getenv('TEST_POSTGRES_DSN')
    if not dsn:
        return None
    url = sa.engine.make_url(dsn)
    return PostgresServer(
        host=url.host or 'localhost',
        port=url.port or 5432,
        user=url.username or 'postgres',
        password=url.password or 'postgres',
    )


@pytest.fixture(scope='session')
def postgres() -> Iterator[PostgresServer]:
    """Сервер PostgreSQL для интеграционных тестов."""
    server = _from_env()
    if server is not None:
        yield server
        return

    try:
        # Модуль переехал в testcontainers.community, но старый путь ещё
        # жив в предыдущих версиях пакета — поддерживаем оба.
        try:
            from testcontainers.community.postgres import (  # type: ignore
                PostgresContainer,
            )
        except ImportError:
            from testcontainers.postgres import (  # type: ignore
                PostgresContainer,
            )
    except ImportError:  # pragma: no cover - зависит от окружения
        pytest.skip('нет ни TEST_POSTGRES_DSN, ни testcontainers')

    try:
        container = PostgresContainer('postgres:16-alpine')
        container.start()
    except Exception as exc:  # pragma: no cover - зависит от окружения
        pytest.skip(f'не удалось поднять контейнер postgres: {exc}')

    try:
        yield PostgresServer(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
            user=container.username,
            password=container.password,
        )
    finally:
        container.stop()


def _recreate_database(server: PostgresServer, name: str) -> None:
    """Пересоздаёт базу начисто — тесты не должны видеть чужой мусор."""
    engine = sa.create_engine(
        server.dsn('postgres'), isolation_level='AUTOCOMMIT'
    )
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                'WHERE datname = :name AND pid <> pg_backend_pid()'
            ),
            {'name': name},
        )
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}"'))
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    engine.dispose()


def run_alembic(
    server: PostgresServer, service: str, *args: str
) -> subprocess.CompletedProcess[str]:
    """Запускает alembic так же, как это делает job *-migrate в compose.

    Именно подпроцессом: env.py читает настройки сервиса из окружения при
    импорте, и подменить их в уже загруженном процессе нельзя.
    """
    ini, database = SERVICES[service]
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO_ROOT / 'services'),
        'DB_HOST': server.host,
        'DB_PORT': str(server.port),
        'DB_USER': server.user,
        'DB_PASSWORD': server.password,
        'DB_NAME': database,
    }
    return subprocess.run(
        [sys.executable, '-m', 'alembic', '-c', ini, *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope='session')
def migrated(postgres: PostgresServer) -> dict[str, str]:
    """Накатывает миграции всех сервисов, возвращает async-DSN каждого."""
    dsns: dict[str, str] = {}
    for service, (_, database) in SERVICES.items():
        _recreate_database(postgres, database)
        result = run_alembic(postgres, service, 'upgrade', 'head')
        assert result.returncode == 0, (
            f'миграции {service} не накатились:\n'
            f'{result.stdout}\n{result.stderr}'
        )
        dsns[service] = postgres.dsn(database, driver='postgresql+asyncpg')
    return dsns


@pytest.fixture
async def session_factory(
    migrated: dict[str, str], request: pytest.FixtureRequest
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Фабрика сессий сервиса, указанного маркером ``service``."""
    marker = request.node.get_closest_marker('service')
    service = marker.args[0] if marker else 'orders'

    engine = create_async_engine(migrated[service], pool_pre_ping=True)
    try:
        yield async_sessionmaker(bind=engine, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
async def clean_tables(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    """Чистит таблицы перед каждым тестом, оставляя схему на месте."""
    async with session_factory() as session:
        tables = (
            (
                await session.execute(
                    sa.text(
                        'SELECT tablename FROM pg_tables '
                        "WHERE schemaname = 'public' "
                        "AND tablename <> 'alembic_version'"
                    )
                )
            )
            .scalars()
            .all()
        )
        if tables:
            names = ', '.join(f'"{t}"' for t in tables)
            await session.execute(
                sa.text(f'TRUNCATE {names} RESTART IDENTITY CASCADE')
            )
            await session.commit()
    yield
