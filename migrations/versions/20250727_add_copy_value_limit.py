"""Add value limit fields to Account"""

revision = '20250727_add_copy_value_limit'
down_revision = '20250726_add_brokers_and_master_accounts'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('account', sa.Column('copy_value_limit', sa.Float()))
    op.add_column('account', sa.Column('copied_value', sa.Float(), server_default='0'))


def downgrade():
    op.drop_column('account', 'copied_value')
    op.drop_column('account', 'copy_value_limit')
