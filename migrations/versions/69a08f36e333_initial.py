"""initial schema

Revision ID: 69a08f36e333
Revises: 
Create Date: 2025-07-08 10:28:41.551572
"""
from alembic import op
import sqlalchemy as sa

revision = '69a08f36e333'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'user',
       sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('email', sa.String(length=120), nullable=False, unique=True),
        sa.Column('password_hash', sa.String(length=128)),
        sa.Column('name', sa.String(length=120)),
        sa.Column('phone', sa.String(length=20)),
        sa.Column('webhook_token', sa.String(length=64), unique=True),
        sa.Column('profile_image', sa.String(length=120)),
        sa.Column('plan', sa.String(length=20), server_default='Free'),
        sa.Column('last_login', sa.DateTime()),
        sa.Column('subscription_start', sa.DateTime()),
        sa.Column('subscription_end', sa.DateTime()),
        sa.Column('payment_status', sa.String(length=32)),
        sa.Column('is_admin', sa.Boolean(), server_default=sa.text('false')),
    )

    op.create_table(
        'account',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('user.id'), nullable=False),
        sa.Column('broker', sa.String(length=50)),
        sa.Column('client_id', sa.String(length=50), nullable=False),
        sa.Column('username', sa.String(length=100)),
        sa.Column('auto_login', sa.Boolean(), server_default=sa.text('true')),
        sa.Column('last_login_time', sa.DateTime()),
        sa.Column('device_number', sa.String(length=50)),
        sa.Column('token_expiry', sa.DateTime()),
        sa.Column('status', sa.String(length=20)),
        sa.Column('role', sa.String(length=20)),
        sa.Column('linked_master_id', sa.String(length=50)),
        sa.Column('copy_status', sa.String(length=10), server_default='Off'),
        sa.Column('multiplier', sa.Float(), server_default='1'),
        sa.Column('credentials', sa.JSON()),
        sa.Column('last_copied_trade_id', sa.String(length=50)),
    )
    op.create_index('idx_user_client', 'account', ['user_id', 'client_id'])
    op.create_index('idx_role_status', 'account', ['role', 'copy_status'])
    op.create_index('idx_master_children', 'account', ['linked_master_id', 'role'])
    op.create_unique_constraint('uq_user_client', 'account', ['user_id', 'client_id'])

    op.create_table(
        'trade',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('user.id'), nullable=False),
        sa.Column('symbol', sa.String(length=50)),
        sa.Column('action', sa.String(length=10)),
        sa.Column('qty', sa.Integer()),
        sa.Column('price', sa.Float()),
        sa.Column('status', sa.String(length=20)),
        sa.Column('timestamp', sa.DateTime()),
        sa.Column('broker', sa.String(length=50)),
        sa.Column('order_id', sa.String(length=50)),
        sa.Column('client_id', sa.String(length=50)),
    )
    op.create_index('idx_user_timestamp', 'trade', ['user_id', 'timestamp'])
    op.create_index('idx_symbol_action', 'trade', ['symbol', 'action'])

    op.create_table(
        'order_mapping',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('master_order_id', sa.String(length=50), nullable=False),
        sa.Column('child_order_id', sa.String(length=50)),
        sa.Column('master_client_id', sa.String(length=50), nullable=False),
        sa.Column('master_broker', sa.String(length=50)),
        sa.Column('child_client_id', sa.String(length=50), nullable=False),
        sa.Column('child_broker', sa.String(length=50)),
        sa.Column('symbol', sa.String(length=50)),
        sa.Column('status', sa.String(length=20), server_default='ACTIVE'),
        sa.Column('timestamp', sa.DateTime()),
        sa.Column('child_timestamp', sa.DateTime()),
        sa.Column('remarks', sa.Text()),
        sa.Column('multiplier', sa.Float(), server_default='1'),
        sa.Column('action', sa.String(length=10)),
        sa.Column('quantity', sa.Integer()),
        sa.Column('price', sa.Float()),
    )
    op.create_index('idx_master_order_status', 'order_mapping', ['master_order_id', 'status'])

    op.create_table(
        'trade_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('timestamp', sa.DateTime()),
        sa.Column('user_id', sa.Integer),
        sa.Column('symbol', sa.String(length=50)),
        sa.Column('action', sa.String(length=10)),
        sa.Column('quantity', sa.Integer()),
        sa.Column('status', sa.String(length=20)),
        sa.Column('response', sa.Text()),
        sa.Column('broker', sa.String(length=50)),
        sa.Column('client_id', sa.String(length=50)),
        sa.Column('order_id', sa.String(length=50)),
        sa.Column('price', sa.Float()),
        sa.Column('error_code', sa.String(length=50)),
    )


def downgrade():
    op.drop_table('trade_log')
    op.drop_table('order_mapping')
    op.drop_index('idx_symbol_action', table_name='trade')
    op.drop_index('idx_user_timestamp', table_name='trade')
    op.drop_table('trade')
    op.drop_constraint('uq_user_client', 'account', type_='unique')
    op.drop_index('idx_master_children', table_name='account')
    op.drop_index('idx_role_status', table_name='account')
    op.drop_index('idx_user_client', table_name='account')
    op.drop_table('account')
    op.drop_table('user')
