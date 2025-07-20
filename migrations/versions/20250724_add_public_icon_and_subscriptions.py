"""Add public and icon fields to Strategy and subscription table

Revision ID: 20250724_add_public_icon_and_subscriptions
Revises: 20250723_add_strategy_extra_fields
Create Date: 2025-07-24 00:00:00
"""

from alembic import op
import sqlalchemy as sa

revision = '20250724_add_public_icon_and_subscriptions'
down_revision = '20250723_add_strategy_extra_fields'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('strategy', sa.Column('is_public', sa.Boolean(), server_default=sa.text('0'), index=True))
    op.add_column('strategy', sa.Column('icon', sa.Text()))

    op.create_table(
        'strategy_subscription',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('strategy_id', sa.Integer(), sa.ForeignKey('strategy.id'), nullable=False, index=True),
        sa.Column('subscriber_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=False, index=True),
        sa.Column('approved', sa.Boolean(), server_default=sa.text('0')),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.UniqueConstraint('strategy_id', 'subscriber_id', name='uq_strategy_subscriber')
    )


def downgrade():
    op.drop_table('strategy_subscription')
    op.drop_column('strategy', 'icon')
    op.drop_column('strategy', 'is_public')
