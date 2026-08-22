"""Тонкая обёртка над AIOKafkaProducer, работающая с конвертами событий."""

from __future__ import annotations

from types import TracebackType
from typing import Any, Self

from aiokafka import AIOKafkaProducer

from ms_events.envelope import EventEnvelope


class EventProducer:
    """Публикует ``EventEnvelope`` в Kafka.

    Ключ партиционирования — идентификатор агрегата (order_id): так все
    события одного заказа попадают в одну партицию и сохраняют порядок.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
        request_timeout_ms: int = 10_000,
    ) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            request_timeout_ms=request_timeout_ms,
            acks='all',
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def send(self, topic: str, envelope: EventEnvelope) -> None:
        """Отправляет событие и дожидается подтверждения от брокера."""
        await self._producer.send_and_wait(
            topic,
            envelope.to_json(),
            key=envelope.aggregate_id.encode('utf-8'),
        )

    async def send_raw(
        self, topic: str, value: bytes, *, key: str | None = None
    ) -> Any:
        """Отправляет уже сериализованное сообщение (используется для DLQ)."""
        return await self._producer.send_and_wait(
            topic,
            value,
            key=key.encode('utf-8') if key else None,
        )
