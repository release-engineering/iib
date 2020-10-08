"""Make binary_image optional.

Revision ID: 60f89c046096
Revises: 983a81fe5e98
Create Date: 2020-10-12 15:49:24.523019

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '60f89c046096'
down_revision = '983a81fe5e98'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.alter_column('binary_image_id', existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table('request_merge_index_image', schema=None) as batch_op:
        batch_op.alter_column('binary_image_id', existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table('request_rm', schema=None) as batch_op:
        batch_op.alter_column('binary_image_id', existing_type=sa.INTEGER(), nullable=True)


def downgrade():
    with op.batch_alter_table('request_rm', schema=None) as batch_op:
        batch_op.alter_column('binary_image_id', existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table('request_merge_index_image', schema=None) as batch_op:
        batch_op.alter_column('binary_image_id', existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.alter_column('binary_image_id', existing_type=sa.INTEGER(), nullable=False)
