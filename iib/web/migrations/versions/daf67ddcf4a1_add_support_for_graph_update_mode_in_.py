"""Add support for graph_update_mode in Add endpoint.

Revision ID: daf67ddcf4a1
Revises: 8d50f82f0be9
Create Date: 2023-07-27 15:37:59.568914

"""
from alembic import op
import sqlalchemy as sa


revision = 'daf67ddcf4a1'
down_revision = '8d50f82f0be9'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.add_column(sa.Column('graph_update_mode', sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.drop_column('graph_update_mode')
