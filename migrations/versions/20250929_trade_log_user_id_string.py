"""Change trade_log.user_id to String for broker identifiers"""

from alembic import op
import sqlalchemy as sa


revision = "20250929_trade_log_user_id_string"
down_revision = "20250916_activate_master_copy_status"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('trade_log') as batch:
        batch.alter_column(
            'user_id',
            existing_type=sa.Integer(),
            type_=sa.String(length=64),
            existing_nullable=True,
        )


def downgrade():
    with op.batch_alter_table('trade_log') as batch:
        batch.alter_column(
            'user_id',
            existing_type=sa.String(length=64),
            type_=sa.Integer(),
            existing_nullable=True,
        )
