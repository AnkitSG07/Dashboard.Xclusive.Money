"""add message column to system_log

Revision ID: 20250709_add_message_column_to_system_log
Revises: 
Create Date: 2025-07-09 10:20:00

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20250709_add_message_column_to_system_log'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('system_log', sa.Column('message', sa.Text(), nullable=True))

def downgrade():
    op.drop_column('system_log', 'message')
