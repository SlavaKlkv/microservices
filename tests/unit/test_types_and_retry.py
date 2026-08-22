"""Справочник топиков и расчёт задержек между повторами."""

from __future__ import annotations

import pytest

from ms_events import TOPIC_BY_PRODUCER, Producer, Topic, backoff_seconds


@pytest.mark.parametrize(
    ('topic', 'expected'),
    [
        (Topic.ORDERS, Topic.ORDERS_DLQ),
        (Topic.NOTIFICATIONS, Topic.NOTIFICATIONS_DLQ),
    ],
)
def test_dlq_is_paired_with_main_topic(topic: Topic, expected: Topic) -> None:
    assert topic.dlq is expected


@pytest.mark.parametrize('topic', [Topic.ORDERS_DLQ, Topic.NOTIFICATIONS_DLQ])
def test_dlq_of_dlq_is_itself(topic: Topic) -> None:
    # Иначе сообщение, упавшее при разборе уже в DLQ, ушло бы в
    # несуществующий топик *.dlq.dlq.
    assert topic.dlq is topic


def test_every_producer_has_a_topic() -> None:
    assert set(TOPIC_BY_PRODUCER) == set(Producer)


def test_backoff_grows_and_is_capped() -> None:
    base, cap = 1.0, 10.0

    delays = [backoff_seconds(attempt, base, cap) for attempt in range(1, 9)]

    assert delays == sorted(delays)
    assert delays[0] >= base
    assert max(delays) <= cap


def test_backoff_first_attempt_is_not_instant() -> None:
    assert backoff_seconds(1, 0.5, 30.0) > 0
