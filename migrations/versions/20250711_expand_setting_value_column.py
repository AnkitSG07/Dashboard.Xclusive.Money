"""Expand Setting.value column to Text

Revision ID: 20250711_expand_setting_value_column
Revises: 20250710_uuid_primary_keys
Create Date: 2025-07-11 00:00:00
"""

from alembic import op
import sqlalchemy as sa

revision = '20250711_expand_setting_value_column'
down_revision = '20250710_uuid_primary_keys'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('setting') as batch:
        batch.alter_column('value', type_=sa.Text(), existing_type=sa.String(length=255))


def downgrade():
    with op.batch_alter_table('setting') as batch:
        batch.alter_column('value', type_=sa.String(length=255), existing_type=sa.Text())
