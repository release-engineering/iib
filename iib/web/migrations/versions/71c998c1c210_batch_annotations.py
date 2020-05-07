"""
Add batch annotations.

Revision ID: 71c998c1c210
Revises: 56d96595c0f7
Create Date: 2020-05-07 18:07:20.123669
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '71c998c1c210'
down_revision = '56d96595c0f7'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('batch') as batch_op:
        batch_op.add_column(sa.Column('annotations', sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table('batch') as batch_op:
        batch_op.drop_column('annotations')
