"""Add activation fields to strategy"""

revision = '20250721_add_activation_fields_to_strategy'
down_revision = '20250720_add_user_id_to_strategy'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('strategy', sa.Column('is_active', sa.Boolean(), server_default=sa.text('0')))
    op.add_column('strategy', sa.Column('last_run_at', sa.DateTime()))


def downgrade():
    op.drop_column('strategy', 'last_run_at')
    op.drop_column('strategy', 'is_active')
