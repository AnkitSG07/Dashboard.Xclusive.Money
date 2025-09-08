"""Add copy_qty to Account and drop multiplier"""

revision = '20250728_replace_multiplier_with_copy_qty'
down_revision = '20250727_add_copy_value_limit'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('account', sa.Column('copy_qty', sa.Integer(), nullable=True))
    op.drop_column('account', 'multiplier')


def downgrade():
    op.add_column('account', sa.Column('multiplier', sa.Float(), server_default='1'))
    op.drop_column('account', 'copy_qty')
