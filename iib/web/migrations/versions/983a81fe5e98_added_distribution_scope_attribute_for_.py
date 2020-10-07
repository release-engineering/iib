"""Added distribution scope attribute for RequestMergeIndexImage.

Revision ID: 983a81fe5e98
Revises: 4c9db41195ec
Create Date: 2020-10-08 13:25:25.662595

"""
from alembic import op
import sqlalchemy as sa


revision = '983a81fe5e98'
down_revision = '4c9db41195ec'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_merge_index_image', schema=None) as batch_op:
        batch_op.add_column(sa.Column('distribution_scope', sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table('request_merge_index_image', schema=None) as batch_op:
        batch_op.drop_column('distribution_scope')
