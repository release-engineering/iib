"""Update regenerate_bundle_request endpoint.

Revision ID: a0eadb516360
Revises: 3283f52e7329
Create Date: 2022-09-06 15:00:55.115536

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a0eadb516360'
down_revision = '3283f52e7329'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_regenerate_bundle', schema=None) as batch_op:
        batch_op.add_column(sa.Column('bundle_replacements', sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table('request_regenerate_bundle', schema=None) as batch_op:
        batch_op.drop_column('bundle_replacements')
