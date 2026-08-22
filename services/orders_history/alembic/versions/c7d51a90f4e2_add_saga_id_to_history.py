"""Add saga_id to order_history and event_type to processed_event

Revision ID: c7d51a90f4e2
Revises: 336334812c27
Create Date: 2026-08-22 16:40:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = 'c7d51a90f4e2'
down_revision: Union[str, Sequence[str], None] = '336334812c27'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'order_history',
        sa.Column(
            'saga_id',
            sa.String(length=36),
            nullable=True,
            comment='Идентификатор саги, к которой относится событие',
        ),
    )
    op.create_index(
        op.f('ix_order_history_saga_id'), 'order_history', ['saga_id']
    )

    op.add_column(
        'processed_event',
        sa.Column('event_type', sa.String(length=64), nullable=True),
    )
    # event_id всегда UUID, 64 символа были запасом «на всякий случай».
    op.alter_column(
        'processed_event',
        'event_id',
        type_=sa.String(length=36),
        existing_type=sa.String(length=64),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        'processed_event',
        'event_id',
        type_=sa.String(length=64),
        existing_type=sa.String(length=36),
        existing_nullable=False,
    )
    op.drop_column('processed_event', 'event_type')
    op.drop_index(
        op.f('ix_order_history_saga_id'), table_name='order_history'
    )
    op.drop_column('order_history', 'saga_id')
