"""Adding ignore_bundle_ocp_version.

Revision ID: 1920ad83d0ab
Revises: 9e9d4f9730c8
Create Date: 2023-11-22 12:03:50.711489

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1920ad83d0ab'
down_revision = '9e9d4f9730c8'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_merge_index_image', schema=None) as batch_op:
        batch_op.add_column(sa.Column('ignore_bundle_ocp_version', sa.Boolean(), nullable=True))


def downgrade():
    with op.batch_alter_table('request_merge_index_image', schema=None) as batch_op:
        batch_op.drop_column('ignore_bundle_ocp_version')
