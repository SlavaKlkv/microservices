"""Add event_id to outbox

Revision ID: b99c21106a78
Revises: 5bfab7e344e7
Create Date: 2026-01-19 12:52:34.036910
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = 'b99c21106a78'
down_revision: Union[str, Sequence[str], None] = '5bfab7e344e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # gen_random_uuid() находится в расширении pgcrypto
    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')

    # 1) Сначала добавляем колонку как nullable, чтобы миграция прошла на существующих строках.
    op.add_column(
        'outbox',
        sa.Column(
            'event_id',
            sa.String(length=36),
            nullable=True,
            comment='UUID бизнес-события для идемпотентности',
        ),
    )

    # 2) Заполняем event_id для уже существующих записей.
    op.execute(
        sa.text(
            """
            UPDATE outbox
            SET event_id = gen_random_uuid()::text
            WHERE event_id IS NULL
            """
        )
    )

    # 3) Делаем NOT NULL и задаём дефолт для новых строк.
    op.alter_column(
        'outbox',
        'event_id',
        nullable=False,
        server_default=sa.text('gen_random_uuid()::text'),
    )

    # 4) Уникальность для идемпотентности.
    op.create_index(
        op.f('ix_outbox_event_id'), 'outbox', ['event_id'], unique=True
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_outbox_event_id'), table_name='outbox')
    op.alter_column('outbox', 'event_id', server_default=None)
    op.drop_column('outbox', 'event_id')
