"""Отправка писем.

Используется aiosmtplib: блокирующий smtplib в асинхронном консьюмере
останавливал бы весь event loop на время разговора с SMTP-сервером.
Реальная отправка выключена по умолчанию (``NOTIFICATION_ENABLED=false``),
иначе тесты и CI ждали бы недоступный SMTP-хост.
"""

from __future__ import annotations

import random
from email.message import EmailMessage

import aiosmtplib
import structlog

from notification.settings import settings

logger = structlog.get_logger('notification.mailer')


class NotificationSendError(RuntimeError):
    """Уведомление доставить не удалось."""


def _maybe_inject_failure() -> None:
    """Искусственный отказ для проверки компенсирующей ветки саги."""
    rate = settings.NOTIFICATION_FAIL_RATE
    if rate > 0 and random.random() < rate:
        raise NotificationSendError(
            'injected failure (NOTIFICATION_FAIL_RATE)'
        )


async def send_email(*, recipient: str, subject: str, body: str) -> None:
    """Отправляет письмо; бросает NotificationSendError при неудаче."""
    _maybe_inject_failure()

    if not settings.NOTIFICATION_ENABLED:
        logger.info(
            'notification.send_skipped',
            reason='NOTIFICATION_ENABLED=false',
            recipient=recipient,
        )
        return

    message = EmailMessage()
    message['From'] = settings.SMTP_FROM
    message['To'] = recipient
    message['Subject'] = subject
    message.set_content(body)

    try:
        await aiosmtplib.send(
            message,
            hostname=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
            username=settings.SMTP_USER or None,
            password=settings.SMTP_PASSWORD or None,
            start_tls=settings.SMTP_USE_TLS,
            timeout=settings.SMTP_TIMEOUT_SEC,
        )
    except Exception as exc:
        raise NotificationSendError(f'{type(exc).__name__}: {exc}') from exc
