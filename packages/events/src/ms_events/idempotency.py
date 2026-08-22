"""Быстрый путь идемпотентности на Redis.

Источник истины — таблица ``processed_event`` в Postgres: только она
коммитится в одной транзакции с бизнес-записью. Redis нужен, чтобы не
ходить в базу за очевидными дублями, которых при at-least-once доставке
много. Поэтому:

* ключ ставится через ``SET NX EX`` перед обработкой — это заявка;
* если транзакция не удалась, заявка снимается, иначе повтор события
  был бы отброшен «как дубль», а в Postgres записи так и не появилось бы;
* если Redis недоступен, консьюмер молча деградирует в путь через
  Postgres и продолжает работать.
"""

from __future__ import annotations

import structlog

from ms_events.settings import RedisSettings

logger = structlog.get_logger('ms_events.idempotency')


class IdempotencyCache:
    """Кэш обработанных event_id поверх Redis."""

    def __init__(self, settings: RedisSettings, *, namespace: str) -> None:
        self._settings = settings
        self._namespace = namespace
        self._client: object | None = None
        self._degraded = not settings.REDIS_ENABLED

    def _key(self, event_id: str) -> str:
        return f'idem:{self._namespace}:{event_id}'

    async def start(self) -> None:
        if not self._settings.REDIS_ENABLED:
            logger.info('idempotency.disabled')
            return
        try:
            from redis.asyncio import Redis

            client = Redis.from_url(
                self._settings.REDIS_URL, decode_responses=True
            )
            await client.ping()
        except Exception:
            self._degraded = True
            logger.warning(
                'idempotency.redis_unavailable', url=self._settings.REDIS_URL
            )
            return

        self._client = client
        self._degraded = False
        logger.info('idempotency.redis_connected')

    async def stop(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            await client.aclose()  # type: ignore[attr-defined]
        except Exception:
            logger.debug('idempotency.redis_close_failed')

    async def claim(self, event_id: str) -> bool:
        """Пытается занять event_id.

        ``True`` — событие надо обрабатывать (в том числе когда Redis
        недоступен: решение примет Postgres). ``False`` — это заведомый
        дубль, обработку можно пропустить.
        """
        if self._client is None:
            return True
        try:
            ok = await self._client.set(  # type: ignore[attr-defined]
                self._key(event_id),
                '1',
                nx=True,
                ex=self._settings.IDEMPOTENCY_TTL_SEC,
            )
        except Exception:
            self._degraded = True
            logger.warning('idempotency.claim_failed', event_id=event_id)
            return True
        return bool(ok)

    async def release(self, event_id: str) -> None:
        """Снимает заявку — обработка не дошла до коммита в Postgres."""
        if self._client is None:
            return
        try:
            await self._client.delete(self._key(event_id))  # type: ignore[attr-defined]
        except Exception:
            logger.warning('idempotency.release_failed', event_id=event_id)

    @property
    def degraded(self) -> bool:
        """True, если работаем без Redis, только через Postgres."""
        return self._degraded
