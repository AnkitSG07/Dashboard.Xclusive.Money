"""Add extended strategy fields and logs"""

revision = '20250723_add_strategy_extra_fields'
down_revision = '20250722_add_account_to_strategy'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('strategy', sa.Column('signal_source', sa.String(length=100)))
    op.add_column('strategy', sa.Column('risk_max_positions', sa.Integer()))
    op.add_column('strategy', sa.Column('risk_max_allocation', sa.Float()))
    op.add_column('strategy', sa.Column('schedule', sa.String(length=120)))
    op.add_column('strategy', sa.Column('webhook_secret', sa.String(length=120)))
    op.add_column('strategy', sa.Column('track_performance', sa.Boolean(), server_default=sa.text('0')))
    op.add_column('strategy', sa.Column('log_retention_days', sa.Integer(), server_default='30'))

    op.create_table(
        'strategy_log',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('strategy_id', sa.Integer(), sa.ForeignKey('strategy.id'), nullable=False, index=True),
        sa.Column('timestamp', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('level', sa.String(length=20), server_default='INFO'),
        sa.Column('message', sa.Text()),
        sa.Column('performance', sa.JSON()),
    )


def downgrade():
    op.drop_table('strategy_log')
    op.drop_column('strategy', 'log_retention_days')
    op.drop_column('strategy', 'track_performance')
    op.drop_column('strategy', 'webhook_secret')
    op.drop_column('strategy', 'schedule')
    op.drop_column('strategy', 'risk_max_allocation')
    op.drop_column('strategy', 'risk_max_positions')
    op.drop_column('strategy', 'signal_source')
