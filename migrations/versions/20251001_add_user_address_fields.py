"""Add address fields to user table"""

from alembic import op
import sqlalchemy as sa


revision = "20251001_add_user_address_fields"
down_revision = "20250929_trade_log_user_id_string"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("user", sa.Column("address_line1", sa.String(length=255), nullable=True))
    op.add_column("user", sa.Column("address_line2", sa.String(length=255), nullable=True))
    op.add_column("user", sa.Column("state", sa.String(length=120), nullable=True))
    op.add_column("user", sa.Column("zip_code", sa.String(length=20), nullable=True))
    op.add_column("user", sa.Column("gstin", sa.String(length=20), nullable=True))


def downgrade():
    op.drop_column("user", "gstin")
    op.drop_column("user", "zip_code")
    op.drop_column("user", "state")
    op.drop_column("user", "address_line2")
    op.drop_column("user", "address_line1")  
