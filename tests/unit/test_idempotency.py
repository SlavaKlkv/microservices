"""Быстрый путь идемпотентности на Redis.

Ключевое требование: Redis — это оптимизация, а не источник истины.
Любая его неисправность обязана приводить к «обработай через Postgres»,
а не к потере события.
"""

from __future__ import annotations

from typing import Any

import pytest

from ms_events.idempotency import IdempotencyCache
from ms_events.settings import RedisSettings


class FakeRedis:
    """Минимальный Redis: SET NX EX и DELETE."""

    def __init__(self, *, fail: bool = False) -> None:
        self.store: dict[str, str] = {}
        self.fail = fail

    async def set(
        self, key: str, value: str, *, nx: bool = False, ex: int | None = None
    ) -> bool | None:
        if self.fail:
            raise ConnectionError('redis is down')
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        if self.fail:
            raise ConnectionError('redis is down')
        return int(self.store.pop(key, None) is not None)

    async def aclose(self) -> None:
        return None


def make_cache(client: Any | None) -> IdempotencyCache:
    cache = IdempotencyCache(
        RedisSettings(REDIS_ENABLED=True), namespace='test'
    )
    cache._client = client
    return cache


async def test_first_claim_wins_second_is_duplicate() -> None:
    cache = make_cache(FakeRedis())

    assert await cache.claim('event-1') is True
    assert await cache.claim('event-1') is False


async def test_release_allows_reprocessing() -> None:
    # Обработка упала до коммита в Postgres: заявку надо снять, иначе
    # повтор события будет отброшен как дубль, а записи так и не появится.
    cache = make_cache(FakeRedis())
    await cache.claim('event-1')

    await cache.release('event-1')

    assert await cache.claim('event-1') is True


async def test_different_events_do_not_collide() -> None:
    cache = make_cache(FakeRedis())

    assert await cache.claim('event-1') is True
    assert await cache.claim('event-2') is True


async def test_namespace_isolates_consumer_groups() -> None:
    # У каждой группы свой префикс: одно и то же событие читают разные
    # сервисы, и дубль для одного не должен молчаливо пропасть у другого.
    shared = FakeRedis()
    orders = IdempotencyCache(
        RedisSettings(REDIS_ENABLED=True), namespace='orders'
    )
    history = IdempotencyCache(
        RedisSettings(REDIS_ENABLED=True), namespace='history'
    )
    orders._client = shared
    history._client = shared

    assert await orders.claim('event-1') is True
    assert await history.claim('event-1') is True


async def test_broken_redis_degrades_to_postgres() -> None:
    cache = make_cache(FakeRedis(fail=True))

    # True — «обрабатывай»: решение о дубле примет processed_event.
    assert await cache.claim('event-1') is True
    assert cache.degraded is True


async def test_broken_redis_release_does_not_raise() -> None:
    cache = make_cache(FakeRedis(fail=True))

    await cache.release('event-1')


async def test_disabled_cache_always_allows() -> None:
    cache = IdempotencyCache(
        RedisSettings(REDIS_ENABLED=False), namespace='test'
    )

    await cache.start()

    assert cache.degraded is True
    assert await cache.claim('event-1') is True
    assert await cache.claim('event-1') is True


@pytest.mark.parametrize('event_id', ['a', 'b'])
async def test_no_client_means_process(event_id: str) -> None:
    cache = IdempotencyCache(
        RedisSettings(REDIS_ENABLED=True), namespace='test'
    )

    assert await cache.claim(event_id) is True
