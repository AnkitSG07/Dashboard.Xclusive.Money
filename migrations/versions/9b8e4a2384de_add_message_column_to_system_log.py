"""add message column to system_log

Revision ID: 9b8e4a2384de
Revises: 69a08f36e333
Create Date: 2025-07-10 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '9b8e4a2384de'
down_revision = '69a08f36e333'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE system_log ADD COLUMN IF NOT EXISTS message TEXT")


def downgrade():
    op.execute("ALTER TABLE system_log DROP COLUMN IF EXISTS message")
