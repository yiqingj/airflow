"""add task

Revision ID: 240237d38303
Revises: 9f61a8f866b4
Create Date: 2016-03-07 20:42:37.078997

"""

# revision identifiers, used by Alembic.
revision = '240237d38303'
down_revision = '1968acfc09e3'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects.postgresql import ARRAY
from airflow import settings


def upgrade():
    inspector = Inspector.from_engine(settings.engine)
    tables = inspector.get_table_names()
    if 'task' not in tables:
        op.create_table(
            'task',
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('operator', sa.String(length=50), nullable=False),
            sa.Column('code', sa.TEXT, nullable=True),
            sa.Column('upstreams', ARRAY(sa.String, dimensions=1), nullable=True),
            sa.Column('downstreams', ARRAY(sa.String, dimensions=1), nullable=True),
            sa.PrimaryKeyConstraint('task_id', 'dag_id')
        )

    if 'dagbag' not in tables:
        op.create_table(
            'dagbag',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('name', sa.String(length=100), nullable=False),
            sa.Column('url', sa.String(length=500), nullable=False),
            sa.Column('branch', sa.String(length=100), nullable=False),
            sa.Column('folder', sa.String(length=500), nullable=False),
            sa.Column('disabled', sa.Boolean(), server_default='false'),
            sa.Column('description', sa.TEXT, nullable=True),
            sa.PrimaryKeyConstraint('id')
        )

    op.add_column('dag', sa.Column('health', sa.String(length=50), nullable=True))
    op.add_column('dag', sa.Column('schedule', sa.String(length=100), nullable=True))
    op.add_column('dag', sa.Column('params', sa.TEXT, nullable=True))
    op.add_column('dag_run', sa.Column('version', sa.Integer(), server_default='0'))
    op.add_column('dag_run', sa.Column('queue', sa.String(50)))
    op.add_column('task_instance', sa.Column('version', sa.Integer(), server_default='0'))
    op.add_column('task_instance', sa.Column('expired', sa.Boolean(), server_default='false'))
    op.add_column('task_instance',
                  sa.Column('upstreams', ARRAY(sa.String, dimensions=1), nullable=True))
    op.add_column('task_instance',
                  sa.Column('downstreams', ARRAY(sa.String, dimensions=1), nullable=True))
    op.drop_constraint('task_instance_pkey', 'task_instance')
    op.create_primary_key('task_instance_pkey', 'task_instance',
                          ['task_id', 'dag_id', 'execution_date', 'version'])


def downgrade():
    op.drop_table('task')
    op.drop_table('dagbag')
    op.drop_column('dag', 'health')
    op.drop_column('dag', 'schedule')
    op.drop_column('dag', 'params')
    op.drop_column('dag_run', 'version')
    op.drop_column('dag_run', 'queue')
    op.drop_column('task_instance', 'expired')
    op.drop_column('task_instance', 'upstreams')
    op.drop_column('task_instance', 'downstreams')

    op.drop_constraint('task_instance_pkey', 'task_instance')
    op.create_primary_key('task_instance_pkey', 'task_instance',
                          ['task_id', 'dag_id', 'execution_date'])
    op.drop_column('task_instance', 'version')
