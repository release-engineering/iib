"""
Add fbc-operations api.

Revision ID: 8d50f82f0be9
Revises: a0eadb516360
Create Date: 2023-01-04 10:39:49.366511

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8d50f82f0be9'
down_revision = 'a0eadb516360'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'request_fbc_operations',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('fbc_fragment_id', sa.Integer(), nullable=True),
        sa.Column('fbc_fragment_resolved_id', sa.Integer(), nullable=True),
        sa.Column('binary_image_id', sa.Integer(), nullable=True),
        sa.Column('binary_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('from_index_id', sa.Integer(), nullable=True),
        sa.Column('from_index_resolved_id', sa.Integer(), nullable=True),
        sa.Column('index_image_id', sa.Integer(), nullable=True),
        sa.Column('index_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('internal_index_image_copy_id', sa.Integer(), nullable=True),
        sa.Column('internal_index_image_copy_resolved_id', sa.Integer(), nullable=True),
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
            ['fbc_fragment_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['fbc_fragment_resolved_id'],
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
        sa.ForeignKeyConstraint(
            ['internal_index_image_copy_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['internal_index_image_copy_resolved_id'],
            ['image.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    op.drop_table('request_fbc_operations')
