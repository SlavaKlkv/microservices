"""Миграции: накатываются, откатываются и совпадают с моделями.

Расхождение моделей и миграций — тихая поломка: код работает у
разработчика с create_all и падает в проде на отсутствующей колонке.
"""

from __future__ import annotations

import pytest

from tests.integration.conftest import SERVICES, PostgresServer, run_alembic

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('service', sorted(SERVICES))
def test_models_match_migrations(
    postgres: PostgresServer, migrated: dict[str, str], service: str
) -> None:
    # alembic check падает, если автогенерация нашла бы новые изменения.
    result = run_alembic(postgres, service, 'check')

    assert result.returncode == 0, (
        f'модели {service} разошлись с миграциями:\n'
        f'{result.stdout}\n{result.stderr}'
    )


@pytest.mark.parametrize('service', sorted(SERVICES))
def test_downgrade_and_upgrade_again(
    postgres: PostgresServer, migrated: dict[str, str], service: str
) -> None:
    # Повторный upgrade после полного отката ловит миграции, которые
    # забыли удалить свой enum или индекс: CREATE TYPE упадёт вторым разом.
    down = run_alembic(postgres, service, 'downgrade', 'base')
    assert down.returncode == 0, f'{down.stdout}\n{down.stderr}'

    up = run_alembic(postgres, service, 'upgrade', 'head')
    assert up.returncode == 0, f'{up.stdout}\n{up.stderr}'


@pytest.mark.parametrize('service', sorted(SERVICES))
def test_single_head(
    postgres: PostgresServer, migrated: dict[str, str], service: str
) -> None:
    # Две головы означают разъехавшиеся ветки миграций: upgrade head
    # в такой ситуации падает уже в проде.
    result = run_alembic(postgres, service, 'heads')

    assert result.returncode == 0, result.stderr
    heads = [ln for ln in result.stdout.splitlines() if ln.strip()]
    assert len(heads) == 1, f'у {service} несколько голов: {heads}'
