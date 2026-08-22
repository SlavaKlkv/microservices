"""Общие настройки тестов.

Тесты разложены по трём уровням и включаются разными условиями:

* ``tests/unit`` — чистая логика, работают всегда и нигде не нуждаются;
* ``tests/integration`` — нужен живой PostgreSQL. Берётся из
  ``TEST_POSTGRES_DSN``, иначе поднимается контейнером через
  testcontainers, иначе тесты пропускаются;
* ``tests/e2e`` — нужен поднятый ``docker compose``; включаются только
  когда задан ``E2E_BASE_URL``.

Пропуск, а не падение: в CI и на машине разработчика доступно разное, и
недоступность docker не должна выглядеть как сломанный код.
"""

from __future__ import annotations

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope='session')
def repo_root() -> pathlib.Path:
    return REPO_ROOT
