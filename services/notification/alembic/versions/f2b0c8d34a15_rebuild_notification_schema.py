"""Rebuild notification schema: notification, processed_event, outbox

Revision ID: f2b0c8d34a15
Revises: 8383bb3afdb9
Create Date: 2026-08-22 17:10:00.000000

Прежняя таблица notifications была слепком «сырого» события и никем не
использовалась (сервис в рантайме создавал свою notification_db), поэтому
она удаляется, а вместо неё появляется нормальная схема сервиса.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = 'f2b0c8d34a15'
down_revision: Union[str, Sequence[str], None] = '8383bb3afdb9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table('notifications')

    op.create_table(
        'notification',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            'event_id',
            sa.String(length=36),
            nullable=False,
            comment='event_id входящего события — гарантия «одно на событие»',
        ),
        sa.Column('saga_id', sa.String(length=36), nullable=False),
        sa.Column('order_id', sa.BigInteger(), nullable=False),
        sa.Column('user_id', sa.BigInteger(), nullable=True),
        sa.Column('channel', sa.String(length=16), nullable=False),
        sa.Column('recipient', sa.String(length=320), nullable=True),
        sa.Column('subject', sa.String(length=255), nullable=False),
        sa.Column('body', sa.Text(), nullable=False),
        sa.Column(
            'status',
            sa.Enum('SENT', 'FAILED', name='notification_status'),
            nullable=False,
        ),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_notification_event_id'),
        'notification',
        ['event_id'],
        unique=True,
    )
    op.create_index(
        op.f('ix_notification_saga_id'), 'notification', ['saga_id']
    )
    op.create_index(
        op.f('ix_notification_order_id'), 'notification', ['order_id']
    )
    op.create_index(
        op.f('ix_notification_status'), 'notification', ['status']
    )

    op.create_table(
        'processed_event',
        sa.Column('event_id', sa.String(length=36), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=True),
        sa.Column(
            'processed_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('event_id'),
    )

    op.create_table(
        'outbox',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            'event_id',
            sa.String(length=36),
            nullable=False,
            comment='UUID бизнес-события для идемпотентности',
        ),
        sa.Column(
            'event_type',
            sa.String(length=64),
            nullable=False,
            comment='Тип события, например order.created',
        ),
        sa.Column(
            'saga_id',
            sa.String(length=36),
            nullable=False,
            comment='Идентификатор саги, общий для всей цепочки',
        ),
        sa.Column(
            'correlation_id',
            sa.String(length=36),
            nullable=True,
            comment='X-Request-ID исходного HTTP-запроса',
        ),
        sa.Column(
            'causation_id',
            sa.String(length=36),
            nullable=True,
            comment='event_id события-причины',
        ),
        sa.Column(
            'producer',
            sa.String(length=32),
            nullable=False,
            comment='Сервис-издатель события',
        ),
        sa.Column(
            'topic',
            sa.String(length=128),
            nullable=False,
            comment='Kafka-топик, в который публикуется событие',
        ),
        sa.Column(
            'aggregate_type',
            sa.String(length=32),
            nullable=False,
            comment='Тип агрегата, например order',
        ),
        sa.Column(
            'aggregate_id',
            sa.BigInteger(),
            nullable=False,
            comment='ID агрегата (order_id) — он же ключ партиционирования',
        ),
        sa.Column(
            'payload',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            comment='Полезная нагрузка события',
        ),
        sa.Column(
            'status',
            sa.Enum('NEW', 'SENT', 'ERROR', 'DEAD', name='outbox_status'),
            nullable=False,
        ),
        sa.Column(
            'attempts',
            sa.Integer(),
            nullable=False,
            comment='Сделано попыток отправки',
        ),
        sa.Column(
            'max_attempts',
            sa.Integer(),
            nullable=False,
            comment='Предел попыток, после которого строка становится DEAD',
        ),
        sa.Column(
            'next_retry_at',
            sa.DateTime(timezone=True),
            nullable=True,
            comment='Время следующей попытки',
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'last_error',
            sa.Text(),
            nullable=True,
            comment='Последняя ошибка публикации',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_outbox_event_id'), 'outbox', ['event_id'], unique=True
    )
    op.create_index(op.f('ix_outbox_event_type'), 'outbox', ['event_type'])
    op.create_index(op.f('ix_outbox_saga_id'), 'outbox', ['saga_id'])
    op.create_index(op.f('ix_outbox_topic'), 'outbox', ['topic'])
    op.create_index(
        op.f('ix_outbox_aggregate_type'), 'outbox', ['aggregate_type']
    )
    op.create_index(
        op.f('ix_outbox_aggregate_id'), 'outbox', ['aggregate_id']
    )
    op.create_index(op.f('ix_outbox_status'), 'outbox', ['status'])


def downgrade() -> None:
    op.drop_table('outbox')
    op.execute('DROP TYPE IF EXISTS outbox_status')
    op.drop_table('processed_event')
    op.drop_table('notification')
    op.execute('DROP TYPE IF EXISTS notification_status')

    op.create_table(
        'notifications',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('event_id', sa.String(), nullable=False),
        sa.Column('event_type', sa.String(), nullable=False),
        sa.Column('order_id', sa.BigInteger(), nullable=True),
        sa.Column('user_id', sa.BigInteger(), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column(
            'created_at',
            sa.TIMESTAMP(),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('event_id'),
    )
