"""Add RequestCreateEmptyIndex model.

Revision ID: e16a8cd2e028
Revises: 9d60d35786c1
Create Date: 2021-04-29 12:04:28.272171

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e16a8cd2e028'
down_revision = '9d60d35786c1'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'request_create_empty_index',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('from_index_id', sa.Integer(), nullable=False),
        sa.Column('binary_image_id', sa.Integer(), nullable=True),
        sa.Column('labels', sa.Text(), nullable=True),
        sa.Column('binary_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('from_index_resolved_id', sa.Integer(), nullable=True),
        sa.Column('index_image_id', sa.Integer(), nullable=True),
        sa.Column('index_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('distribution_scope', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ['binary_image_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['binary_image_resolved_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['from_index_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['from_index_resolved_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['id'],
            ['request.id'],
        ),
        sa.ForeignKeyConstraint(
            ['index_image_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['index_image_resolved_id'],
            ['image.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    op.drop_table('request_create_empty_index')
