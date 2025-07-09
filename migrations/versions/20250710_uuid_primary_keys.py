"""Update tables to use UUID primary keys"""

from alembic import op
import sqlalchemy as sa

revision = '20250710_uuid_primary_keys'
down_revision = '69a08f36e333'
branch_labels = None
depends_on = None


def upgrade():
    # Example migration converting integer IDs to UUID strings
    with op.batch_alter_table('user') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('account') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
        batch.alter_column('user_id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('trade') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
        batch.alter_column('user_id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('webhook_log') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
        batch.alter_column('user_id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('system_log') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
        batch.alter_column('user_id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('setting') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('group') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
        batch.alter_column('user_id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('order_mapping') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
    with op.batch_alter_table('trade_log') as batch:
        batch.alter_column('id', existing_type=sa.Integer(), type_=sa.String(length=36))
        batch.alter_column('user_id', existing_type=sa.Integer(), type_=sa.String(length=36))


def downgrade():
    with op.batch_alter_table('trade_log') as batch:
        batch.alter_column('user_id', type_=sa.Integer())
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('order_mapping') as batch:
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('group') as batch:
        batch.alter_column('user_id', type_=sa.Integer())
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('setting') as batch:
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('system_log') as batch:
        batch.alter_column('user_id', type_=sa.Integer())
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('webhook_log') as batch:
        batch.alter_column('user_id', type_=sa.Integer())
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('trade') as batch:
        batch.alter_column('user_id', type_=sa.Integer())
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('account') as batch:
        batch.alter_column('user_id', type_=sa.Integer())
        batch.alter_column('id', type_=sa.Integer())
    with op.batch_alter_table('user') as batch:
        batch.alter_column('id', type_=sa.Integer())
