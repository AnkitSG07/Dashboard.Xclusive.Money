"""Change user.profile_image to Text for data URLs

Revision ID: 20250712_profile_image_text
Revises: 20250711_expand_setting_value_column
Create Date: 2025-07-12 00:00:00
"""
from alembic import op
import sqlalchemy as sa

revision = '20250712_profile_image_text'
down_revision = '20250711_expand_setting_value_column'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('user') as batch:
        batch.alter_column('profile_image', type_=sa.Text(), existing_type=sa.String(length=120))


def downgrade():
    with op.batch_alter_table('user') as batch:
        batch.alter_column('profile_image', type_=sa.String(length=120), existing_type=sa.Text())
