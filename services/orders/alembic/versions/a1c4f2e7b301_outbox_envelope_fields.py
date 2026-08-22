"""Outbox: envelope fields, topic routing and DEAD status

Revision ID: a1c4f2e7b301
Revises: b99c21106a78
Create Date: 2026-08-22 16:10:00.000000

ВНИМАНИЕ: миграция добавляет значение DEAD в тип outbox_status через
ALTER TYPE ... ADD VALUE. PostgreSQL не умеет удалять значение из enum,
поэтому downgrade необратим в этой части: DEAD останется в типе, а сами
строки со статусом DEAD будут переведены в ERROR. Полный откат
потребовал бы пересоздания типа и таблицы.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = 'a1c4f2e7b301'
down_revision: Union[str, Sequence[str], None] = 'b99c21106a78'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Топик по умолчанию для уже накопленных строк.
ORDERS_TOPIC = 'orders.events.v1'


def upgrade() -> None:
    # ALTER TYPE ... ADD VALUE до PostgreSQL 12 нельзя было выполнять
    # внутри транзакции; autocommit_block снимает вопрос на любых версиях.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE outbox_status ADD VALUE IF NOT EXISTS 'DEAD'")

    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')

    op.add_column(
        'outbox',
        sa.Column(
            'saga_id',
            sa.String(length=36),
            nullable=True,
            comment='Идентификатор саги, общий для всей цепочки',
        ),
    )
    op.add_column(
        'outbox',
        sa.Column(
            'correlation_id',
            sa.String(length=36),
            nullable=True,
            comment='X-Request-ID исходного HTTP-запроса',
        ),
    )
    op.add_column(
        'outbox',
        sa.Column(
            'causation_id',
            sa.String(length=36),
            nullable=True,
            comment='event_id события-причины',
        ),
    )
    op.add_column(
        'outbox',
        sa.Column(
            'producer',
            sa.String(length=32),
            nullable=True,
            comment='Сервис-издатель события',
        ),
    )
    op.add_column(
        'outbox',
        sa.Column(
            'topic',
            sa.String(length=128),
            nullable=True,
            comment='Kafka-топик, в который публикуется событие',
        ),
    )
    op.add_column(
        'outbox',
        sa.Column(
            'max_attempts',
            sa.Integer(),
            nullable=True,
            comment='Предел попыток, после которого строка становится DEAD',
        ),
    )

    # Бэкфилл существующих строк: у них своей саги не было.
    op.execute(
        sa.text(
            """
            UPDATE outbox
            SET saga_id = COALESCE(saga_id, gen_random_uuid()::text),
                producer = COALESCE(producer, 'orders'),
                topic = COALESCE(topic, :topic),
                max_attempts = COALESCE(max_attempts, 10)
            """
        ).bindparams(topic=ORDERS_TOPIC)
    )

    op.alter_column('outbox', 'saga_id', nullable=False)
    op.alter_column('outbox', 'producer', nullable=False)
    op.alter_column('outbox', 'topic', nullable=False)
    op.alter_column('outbox', 'max_attempts', nullable=False)

    op.create_index(op.f('ix_outbox_saga_id'), 'outbox', ['saga_id'])
    op.create_index(op.f('ix_outbox_topic'), 'outbox', ['topic'])

    # Приводим длины строковых колонок к тем, что объявлены в OutboxMixin.
    op.alter_column(
        'outbox',
        'event_type',
        type_=sa.String(length=64),
        existing_type=sa.String(),
        existing_nullable=False,
        comment='Тип события, например order.created',
        existing_comment='Тип события (например: OrderCreated)',
    )
    op.alter_column(
        'outbox',
        'aggregate_type',
        type_=sa.String(length=32),
        existing_type=sa.String(),
        existing_nullable=False,
        comment='Тип агрегата, например order',
        existing_comment='Тип агрегата (например: order)',
    )
    op.alter_column(
        'outbox',
        'aggregate_id',
        existing_type=sa.BigInteger(),
        existing_nullable=False,
        comment='ID агрегата (order_id) — он же ключ партиционирования',
        existing_comment='ID агрегата (order_id)',
    )
    op.alter_column(
        'outbox',
        'payload',
        existing_type=sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        comment='Полезная нагрузка события',
        existing_comment='JSON payload события',
    )
    op.alter_column(
        'outbox',
        'attempts',
        existing_type=sa.Integer(),
        existing_nullable=False,
        comment='Сделано попыток отправки',
        existing_comment='Количество попыток отправки',
    )
    op.alter_column(
        'outbox',
        'next_retry_at',
        existing_type=sa.DateTime(timezone=True),
        existing_nullable=True,
        comment='Время следующей попытки',
        existing_comment='Дата следующей попытки отправки',
    )
    op.alter_column(
        'outbox',
        'last_error',
        existing_type=sa.Text(),
        existing_nullable=True,
        comment='Последняя ошибка публикации',
        existing_comment='Последняя ошибка при отправке',
    )


def downgrade() -> None:
    # Значение DEAD из типа outbox_status убрать нельзя (см. докстринг),
    # поэтому лишь возвращаем такие строки в ERROR.
    op.execute("UPDATE outbox SET status = 'ERROR' WHERE status = 'DEAD'")

    op.alter_column(
        'outbox',
        'last_error',
        existing_type=sa.Text(),
        existing_nullable=True,
        comment='Последняя ошибка при отправке',
        existing_comment='Последняя ошибка публикации',
    )
    op.alter_column(
        'outbox',
        'next_retry_at',
        existing_type=sa.DateTime(timezone=True),
        existing_nullable=True,
        comment='Дата следующей попытки отправки',
        existing_comment='Время следующей попытки',
    )
    op.alter_column(
        'outbox',
        'attempts',
        existing_type=sa.Integer(),
        existing_nullable=False,
        comment='Количество попыток отправки',
        existing_comment='Сделано попыток отправки',
    )
    op.alter_column(
        'outbox',
        'payload',
        existing_type=sa.dialects.postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        comment='JSON payload события',
        existing_comment='Полезная нагрузка события',
    )
    op.alter_column(
        'outbox',
        'aggregate_id',
        existing_type=sa.BigInteger(),
        existing_nullable=False,
        comment='ID агрегата (order_id)',
        existing_comment='ID агрегата (order_id) — он же ключ партиционирования',
    )
    op.alter_column(
        'outbox',
        'aggregate_type',
        type_=sa.String(),
        existing_type=sa.String(length=32),
        existing_nullable=False,
        comment='Тип агрегата (например: order)',
        existing_comment='Тип агрегата, например order',
    )
    op.alter_column(
        'outbox',
        'event_type',
        type_=sa.String(),
        existing_type=sa.String(length=64),
        existing_nullable=False,
        comment='Тип события (например: OrderCreated)',
        existing_comment='Тип события, например order.created',
    )

    op.drop_index(op.f('ix_outbox_topic'), table_name='outbox')
    op.drop_index(op.f('ix_outbox_saga_id'), table_name='outbox')

    op.drop_column('outbox', 'max_attempts')
    op.drop_column('outbox', 'topic')
    op.drop_column('outbox', 'producer')
    op.drop_column('outbox', 'causation_id')
    op.drop_column('outbox', 'correlation_id')
    op.drop_column('outbox', 'saga_id')
