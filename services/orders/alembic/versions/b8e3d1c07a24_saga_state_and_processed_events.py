"""Saga state: orders.saga_id, cancel_reason, processed_event, order_saga

Revision ID: b8e3d1c07a24
Revises: a1c4f2e7b301
Create Date: 2026-08-22 18:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = 'b8e3d1c07a24'
down_revision: Union[str, Sequence[str], None] = 'a1c4f2e7b301'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'orders',
        sa.Column(
            'saga_id',
            sa.String(length=36),
            nullable=True,
            comment='Сага, начатая созданием заказа',
        ),
    )
    op.add_column(
        'orders',
        sa.Column(
            'cancel_reason',
            sa.Text(),
            nullable=True,
            comment='Причина отмены (для компенсации саги)',
        ),
    )
    op.create_index(op.f('ix_orders_saga_id'), 'orders', ['saga_id'])

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
        'order_saga',
        sa.Column('saga_id', sa.String(length=36), nullable=False),
        sa.Column('order_id', sa.BigInteger(), nullable=False),
        sa.Column(
            'state',
            sa.String(length=32),
            nullable=False,
            comment=(
                'Текущий шаг саги: STARTED, NOTIFIED, CONFIRMED, CANCELLED…'
            ),
        ),
        sa.Column(
            'last_event_id',
            sa.String(length=36),
            nullable=True,
            comment='Последнее применённое событие',
        ),
        sa.Column(
            'started_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('saga_id'),
    )
    op.create_index(
        op.f('ix_order_saga_order_id'), 'order_saga', ['order_id']
    )
    op.create_index(op.f('ix_order_saga_state'), 'order_saga', ['state'])


def downgrade() -> None:
    op.drop_index(op.f('ix_order_saga_state'), table_name='order_saga')
    op.drop_index(op.f('ix_order_saga_order_id'), table_name='order_saga')
    op.drop_table('order_saga')
    op.drop_table('processed_event')
    op.drop_index(op.f('ix_orders_saga_id'), table_name='orders')
    op.drop_column('orders', 'cancel_reason')
    op.drop_column('orders', 'saga_id')
