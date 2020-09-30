"""Add merge-index-image api endpoint.

Revision ID: 4c9db41195ec
Revises: bc29053265ba
Create Date: 2020-09-28 23:06:43.267716

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4c9db41195ec'
down_revision = 'bc29053265ba'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'request_merge_index_image',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('binary_image_id', sa.Integer(), nullable=False),
        sa.Column('binary_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('index_image_id', sa.Integer(), nullable=True),
        sa.Column('source_from_index_id', sa.Integer(), nullable=False),
        sa.Column('source_from_index_resolved_id', sa.Integer(), nullable=True),
        sa.Column('target_index_id', sa.Integer(), nullable=True),
        sa.Column('target_index_resolved_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['binary_image_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['binary_image_resolved_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['id'], ['request.id'],),
        sa.ForeignKeyConstraint(['index_image_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['source_from_index_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['source_from_index_resolved_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['target_index_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['target_index_resolved_id'], ['image.id'],),
        sa.PrimaryKeyConstraint('id'),
    )

    op.create_table(
        'bundle_deprecation',
        sa.Column('merge_index_image_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('bundle_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(['bundle_id'], ['image.id'],),
        sa.ForeignKeyConstraint(['merge_index_image_id'], ['request_merge_index_image.id'],),
        sa.PrimaryKeyConstraint('merge_index_image_id', 'bundle_id'),
        sa.UniqueConstraint(
            'merge_index_image_id', 'bundle_id', name='merge_index_bundle_constraint'
        ),
    )
    with op.batch_alter_table('bundle_deprecation', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_bundle_deprecation_bundle_id'), ['bundle_id'], unique=False
        )
        batch_op.create_index(
            batch_op.f('ix_bundle_deprecation_merge_index_image_id'),
            ['merge_index_image_id'],
            unique=False,
        )


def downgrade():
    op.drop_table('request_merge_index_image')
    with op.batch_alter_table('bundle_deprecation', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_bundle_deprecation_merge_index_image_id'))
        batch_op.drop_index(batch_op.f('ix_bundle_deprecation_bundle_id'))

    op.drop_table('bundle_deprecation')
