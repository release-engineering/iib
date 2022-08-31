"""Add recursive_related_bundles endpoint.

Revision ID: 625fba6081be
Revises: 3283f52e7329
Create Date: 2022-08-25 17:40:56.784924

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '625fba6081be'
down_revision = '3283f52e7329'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'request_recursive_related_bundles',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('parent_bundle_image_id', sa.Integer(), nullable=True),
        sa.Column('parent_bundle_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('organization', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ['id'],
            ['request.id'],
        ),
        sa.ForeignKeyConstraint(
            ['parent_bundle_image_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['parent_bundle_image_resolved_id'],
            ['image.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    op.drop_table('request_recursive_related_bundles')
