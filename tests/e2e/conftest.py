"""E2E: сага целиком, через настоящие HTTP, Kafka и все контейнеры.

Включаются только когда задан ``E2E_BASE_URL`` — адрес nginx поднятого
``docker compose`` (обычно ``http://localhost``). Без него тесты
пропускаются: e2e не должен краснеть просто потому, что стек не поднят.

    docker compose up -d --wait
    E2E_BASE_URL=http://localhost uv run pytest tests/e2e -m e2e
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

#: Сколько ждём, пока сага дойдёт до терминального статуса.
SAGA_TIMEOUT_SEC = float(os.getenv('E2E_SAGA_TIMEOUT_SEC', '60'))


@pytest.fixture(scope='session')
def base_url() -> str:
    url = os.getenv('E2E_BASE_URL')
    if not url:
        pytest.skip('E2E_BASE_URL не задан: стек docker compose не поднят')
    return url.rstrip('/')


@pytest.fixture
async def client(base_url: str) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        yield client


@pytest.fixture
async def token(client: httpx.AsyncClient) -> str:
    """Регистрирует одноразового пользователя и возвращает access-токен."""
    suffix = uuid.uuid4().hex[:10]
    credentials = {
        'username': f'e2e{suffix}',
        'email': f'e2e{suffix}@example.com',
        'password': 'E2Epassw0rd',
    }

    registered = await client.post(
        '/auth/api/v1/auth/register', json=credentials
    )
    assert registered.status_code == 201, registered.text

    logged_in = await client.post(
        '/auth/api/v1/auth/login_json',
        json={
            'login': credentials['username'],
            'password': credentials['password'],
        },
    )
    assert logged_in.status_code == 200, logged_in.text
    return str(logged_in.json()['tokens']['access_token'])


@pytest.fixture
def auth_headers(token: str) -> dict[str, str]:
    return {'Authorization': f'Bearer {token}'}
