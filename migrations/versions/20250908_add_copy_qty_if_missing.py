"""Ensure copy_qty column exists"""

revision = '20250908_add_copy_qty_if_missing'
down_revision = '20250728_replace_multiplier_with_copy_qty'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


def upgrade():
    conn = op.get_bind()
    inspector = inspect(conn)
    columns = [c['name'] for c in inspector.get_columns('account')]
    if 'copy_qty' not in columns:
        op.add_column('account', sa.Column('copy_qty', sa.Integer(), nullable=True))


def downgrade():
    conn = op.get_bind()
    inspector = inspect(conn)
    columns = [c['name'] for c in inspector.get_columns('account')]
    if 'copy_qty' in columns:
        op.drop_column('account', 'copy_qty')
