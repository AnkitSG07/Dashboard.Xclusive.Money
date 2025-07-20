"""Add user_id column to strategy"""

revision = '20250720_add_user_id_to_strategy'
down_revision = '20250719_add_strategy_table'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('strategy', sa.Column('user_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'strategy', 'user', ['user_id'], ['id'])


def downgrade():
    op.drop_constraint(None, 'strategy', type_='foreignkey')
    op.drop_column('strategy', 'user_id')
