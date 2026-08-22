"""Outbox-воркер сервиса уведомлений.

Тот же общий воркер, что и у orders: отличаются только модель, сессии и
топик, записанный в строке outbox.
"""

import asyncio

from ms_events import OutboxWorker, run_worker, setup_logging
from notification.core.db import SessionLocal
from notification.models import OutboxEvent
from notification.settings import settings


def build_worker() -> OutboxWorker:
    return OutboxWorker(
        service='notification',
        model=OutboxEvent,
        session_factory=SessionLocal,
        settings=settings,
    )


async def run_outbox_loop(stop_event: asyncio.Event | None = None) -> None:
    await build_worker().run(stop_event)


def main() -> None:
    setup_logging('notification-outbox-worker', level=settings.LOG_LEVEL)
    try:
        asyncio.run(run_worker(build_worker()))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
