"""Extending RequestAdd for omps_operator_version.

Revision ID: 2ab3d4558cb6
Revises: 71c998c1c210
Create Date: 2020-09-01 13:19:32.267607

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2ab3d4558cb6'
down_revision = '71c998c1c210'
branch_labels = None
depends_on = None


def upgrade():

    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.add_column(sa.Column('omps_operator_version', sa.String(), nullable=True))


def downgrade():

    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.drop_column('omps_operator_version')
