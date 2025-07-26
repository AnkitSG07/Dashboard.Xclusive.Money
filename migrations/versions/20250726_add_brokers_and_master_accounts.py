"""Add brokers and master_accounts to Strategy"""

revision = '20250726_add_brokers_and_master_accounts'
down_revision = '20250724_add_public_icon_and_subscriptions'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('strategy', sa.Column('brokers', sa.Text()))
    op.add_column('strategy', sa.Column('master_accounts', sa.Text()))


def downgrade():
    op.drop_column('strategy', 'master_accounts')
    op.drop_column('strategy', 'brokers')
