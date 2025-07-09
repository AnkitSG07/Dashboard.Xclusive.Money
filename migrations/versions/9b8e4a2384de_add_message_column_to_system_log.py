"""Add message column to system_log table

Revision ID: fix_system_log_20250709
Revises: 69a08f36e333
Create Date: 2025-07-09 08:54:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fix_system_log_20250709'
down_revision = '69a08f36e333'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('system_log', sa.Column('message', sa.Text()))

def downgrade():
    op.drop_column('system_log', 'message')
