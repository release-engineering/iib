"""Add check_related_images flag.

Revision ID: 7346beaff092
Revises: daf67ddcf4a1
Create Date: 2023-08-09 23:48:37.624078

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7346beaff092'
down_revision = 'daf67ddcf4a1'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.add_column(sa.Column('check_related_images', sa.BOOLEAN(), nullable=True))


def downgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.drop_column('check_related_images')
