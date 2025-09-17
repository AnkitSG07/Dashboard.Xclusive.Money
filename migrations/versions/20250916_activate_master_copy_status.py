"""Ensure master accounts default to copy trading"""

revision = '20250916_activate_master_copy_status'
down_revision = '20250908_add_copy_qty_if_missing'
branch_labels = None
depends_on = None

from alembic import op


def upgrade():
    op.execute(
        """
        UPDATE account
        SET copy_status = 'On'
        WHERE role = 'master'
          AND (copy_status IS NULL OR TRIM(copy_status) = '' OR lower(copy_status) <> 'on')
        """
    )


def downgrade():
    op.execute(
        """
        UPDATE account
        SET copy_status = 'Off'
        WHERE role = 'master'
        """
    )
