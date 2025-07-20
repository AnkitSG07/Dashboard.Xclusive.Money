"""Add account_id column to strategy"""

revision = '20250722_add_account_to_strategy'
down_revision = '20250721_add_activation_fields_to_strategy'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('strategy', sa.Column('account_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'strategy', 'account', ['account_id'], ['id'])


def downgrade():
    op.drop_constraint(None, 'strategy', type_='foreignkey')
    op.drop_column('strategy', 'account_id')
