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
    op.add_column('dag', sa.Column('health_status', sa.String(length=20), nullable=True))


def downgrade():
    op.drop_table('task')
    op.drop_column('dag', 'health_status')
