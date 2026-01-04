"""orders pending confirm cancel

Revision ID: 9377a7524d09
Revises: d1f66ccd60bc
Create Date: 2026-01-04 17:40:08.717507

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '9377a7524d09'
down_revision: Union[str, Sequence[str], None] = 'd1f66ccd60bc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    order_status = postgresql.ENUM(
        'PENDING',
        'CONFIRMED',
        'CANCELLED',
        name='order_status',
    )
    # Create enum type if it doesn't exist yet
    order_status.create(op.get_bind(), checkfirst=True)

    # Convert VARCHAR -> ENUM with explicit cast
    op.alter_column(
        'orders',
        'status',
        existing_type=sa.VARCHAR(length=32),
        type_=order_status,
        postgresql_using="""
        CASE lower(status)
          WHEN 'pending' THEN 'PENDING'
          WHEN 'created' THEN 'PENDING'
          WHEN 'paid' THEN 'CONFIRMED'
          WHEN 'confirmed' THEN 'CONFIRMED'
          WHEN 'cancelled' THEN 'CANCELLED'
          WHEN 'canceled' THEN 'CANCELLED'
        ELSE 'PENDING'
        END::order_status
        """,
        existing_comment='Статус заказа',
        existing_nullable=False,
    )
def downgrade() -> None:
    """Downgrade schema."""
    order_status = postgresql.ENUM(
        'PENDING',
        'CONFIRMED',
        'CANCELLED',
        name='order_status',
    )

    # Convert ENUM -> VARCHAR with explicit cast
    op.alter_column(
        'orders',
        'status',
        existing_type=order_status,
        type_=sa.VARCHAR(length=32),
        postgresql_using='status::text',
        existing_comment='Статус заказа',
        existing_nullable=False,
    )

    # Drop enum type if it exists
    order_status.drop(op.get_bind(), checkfirst=True)
