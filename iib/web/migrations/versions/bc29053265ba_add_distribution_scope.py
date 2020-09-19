"""Add distribution scope.

Revision ID: bc29053265ba
Revises: 2ab3d4558cb6
Create Date: 2020-09-20 02:26:45.531336

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bc29053265ba'
down_revision = '2ab3d4558cb6'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.add_column(sa.Column('distribution_scope', sa.String(), nullable=True))

    with op.batch_alter_table('request_rm', schema=None) as batch_op:
        batch_op.add_column(sa.Column('distribution_scope', sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table('request_rm', schema=None) as batch_op:
        batch_op.drop_column('distribution_scope')

    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.drop_column('distribution_scope')
