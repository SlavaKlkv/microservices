"""Outbox-воркер сервиса заказов.

Вся логика публикации общая и живёт в ``ms_events.outbox``; здесь только
привязка к модели и сессиям сервиса.
"""

import asyncio

from ms_events import (
    OutboxWorker,
    run_worker,
    setup_logging,
    start_metrics_server,
)
from orders.core.db import SessionLocal
from orders.models import OutboxEvent
from orders.settings import settings


def build_worker() -> OutboxWorker:
    return OutboxWorker(
        service='orders',
        model=OutboxEvent,
        session_factory=SessionLocal,
        settings=settings,
    )


async def run_outbox_loop(stop_event: asyncio.Event | None = None) -> None:
    await build_worker().run(stop_event)


def main() -> None:
    setup_logging('orders-outbox-worker', level=settings.LOG_LEVEL)
    start_metrics_server(settings.METRICS_PORT, service='orders-outbox-worker')
    try:
        asyncio.run(run_worker(build_worker()))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
