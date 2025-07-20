"""Add Strategy table"

Revision ID: 20250719_add_strategy_table
Revises: 20250712_profile_image_text
Create Date: 2025-07-19 00:00:00
"""

from alembic import op
import sqlalchemy as sa

revision = '20250719_add_strategy_table'
down_revision = '20250712_profile_image_text'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'strategy',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(length=120), nullable=False),
        sa.Column('description', sa.Text()),
        sa.Column('asset_class', sa.String(length=50), nullable=False),
        sa.Column('style', sa.String(length=50), nullable=False),
        sa.Column('allow_auto_submit', sa.Boolean(), server_default=sa.text('1')),
        sa.Column('allow_live_trading', sa.Boolean(), server_default=sa.text('1')),
        sa.Column('allow_any_ticker', sa.Boolean(), server_default=sa.text('1')),
        sa.Column('allowed_tickers', sa.Text()),
        sa.Column('notification_emails', sa.Text()),
        sa.Column('notify_failures_only', sa.Boolean(), server_default=sa.text('0')),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
    )


def downgrade():
    op.drop_table('strategy')
