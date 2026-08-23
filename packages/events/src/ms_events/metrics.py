"""Метрики саги для Prometheus.

API-сервисы отдают ``/metrics`` через instrumentator FastAPI, а воркеры и
консьюмеры HTTP не слушают вообще — им поднимается отдельный крошечный
сервер на ``METRICS_PORT``. Без этого вся асинхронная половина системы
(ровно та, где копятся отказы) остаётся невидимой.

Что меряем и зачем:

* ``outbox_*`` — доехали ли события до брокера. Растущий
  ``outbox_pending_events`` означает, что воркер не справляется или Kafka
  недоступна, а ненулевой ``outbox_events_dead_total`` — что события
  окончательно потеряны для основного потока и лежат в DLQ;
* ``consumer_events_total`` с меткой ``outcome`` — сколько событий
  обработано, сколько отброшено как дубли и сколько упало;
* ``consumer_dlq_total`` — единственная метрика, по которой стоит
  заводить алерт: в DLQ ничего не должно попадать в норме;
* ``saga_duration_seconds`` — сколько живёт сага от создания заказа до
  терминального статуса. Хвост этой гистограммы показывает застрявшие
  саги раньше, чем о них сообщит пользователь.
"""

from __future__ import annotations

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = structlog.get_logger('ms_events.metrics')

OUTBOX_PUBLISHED = Counter(
    'outbox_events_published_total',
    'Событий успешно опубликовано из outbox в Kafka',
    ['service', 'event_type', 'topic'],
)

OUTBOX_FAILED = Counter(
    'outbox_publish_failures_total',
    'Неудачных попыток публикации из outbox',
    ['service', 'event_type'],
)

OUTBOX_DEAD = Counter(
    'outbox_events_dead_total',
    'Событий, у которых исчерпаны попытки публикации (ушли в DLQ)',
    ['service', 'event_type'],
)

OUTBOX_BATCH = Gauge(
    'outbox_batch_size',
    'Размер последней выбранной воркером пачки строк outbox',
    ['service'],
)

OUTBOX_PRUNED = Counter(
    'outbox_rows_pruned_total',
    'Строк удалено уборкой: отправленный outbox и журнал обработанных',
    ['service', 'table'],
)

CONSUMER_EVENTS = Counter(
    'consumer_events_total',
    'События, прошедшие через консьюмер',
    ['service', 'event_type', 'topic', 'outcome'],
)

CONSUMER_DLQ = Counter(
    'consumer_dlq_total',
    'Сообщений отправлено в DLQ',
    ['service', 'topic', 'reason'],
)

CONSUMER_HANDLER_DURATION = Histogram(
    'consumer_handler_duration_seconds',
    'Длительность обработки одного события консьюмером',
    ['service', 'event_type'],
)

SAGA_COMPLETED = Counter(
    'saga_completed_total',
    'Саги, дошедшие до терминального состояния',
    ['outcome'],
)

SAGA_DURATION = Histogram(
    'saga_duration_seconds',
    'Длительность саги от создания заказа до терминального статуса',
    ['outcome'],
    # Сага — это две сетевые пересылки и две транзакции: интересен
    # диапазон секунд, а не миллисекунд.
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 300, 900),
)


def start_metrics_server(port: int, *, service: str) -> None:
    """Поднимает ``/metrics`` для процесса без своего HTTP-сервера.

    Ошибку не поднимаем наверх: занятый порт метрик не повод ронять
    воркер, который делает полезную работу.
    """
    try:
        start_http_server(port)
    except OSError:
        logger.warning(
            'metrics.server_start_failed', service=service, port=port
        )
    else:
        logger.info('metrics.server_started', service=service, port=port)
