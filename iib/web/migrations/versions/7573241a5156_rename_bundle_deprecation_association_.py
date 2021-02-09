"""Rename bundle deprecation association for merge index.

Revision ID: 7573241a5156
Revises: eec630370e68
Create Date: 2021-02-09 13:50:01.905796

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '7573241a5156'
down_revision = 'eec630370e68'
branch_labels = None
depends_on = None


def upgrade():

    with op.batch_alter_table('bundle_deprecation', schema=None) as batch_op:
        batch_op.drop_index('ix_bundle_deprecation_bundle_id')
        batch_op.drop_index('ix_bundle_deprecation_merge_index_image_id')

    op.rename_table('bundle_deprecation', 'request_merge_bundle_deprecation', schema=None)

    with op.batch_alter_table('request_merge_bundle_deprecation', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_merge_bundle_deprecation_bundle_id'), ['bundle_id'], unique=False
        )
        batch_op.create_index(
            batch_op.f('ix_request_merge_bundle_deprecation_merge_index_image_id'),
            ['merge_index_image_id'],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table('request_merge_bundle_deprecation', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_request_merge_bundle_deprecation_merge_index_image_id'))
        batch_op.drop_index(batch_op.f('ix_request_merge_bundle_deprecation_bundle_id'))

    op.rename_table('request_merge_bundle_deprecation', 'bundle_deprecation', schema=None)

    with op.batch_alter_table('bundle_deprecation', schema=None) as batch_op:
        batch_op.create_index(
            'ix_bundle_deprecation_merge_index_image_id', ['merge_index_image_id'], unique=False
        )
        batch_op.create_index('ix_bundle_deprecation_bundle_id', ['bundle_id'], unique=False)
