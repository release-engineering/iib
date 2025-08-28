"""Add support for multiple FBC fragments in fbc-operations.

Revision ID: c32bffd4dbea
Revises: 49d13af4b328
Create Date: 2025-08-26 22:45:34.247259

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c32bffd4dbea'
down_revision = '49d13af4b328'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'request_fbc_operations_fragment',
        sa.Column('request_fbc_operations_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('image_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ['image_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['request_fbc_operations_id'],
            ['request_fbc_operations.id'],
        ),
        sa.PrimaryKeyConstraint('request_fbc_operations_id', 'image_id'),
        sa.UniqueConstraint('request_fbc_operations_id', 'image_id'),
    )
    with op.batch_alter_table('request_fbc_operations_fragment', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_fbc_operations_fragment_image_id'), ['image_id'], unique=False
        )
        batch_op.create_index(
            batch_op.f('ix_request_fbc_operations_fragment_request_fbc_operations_id'),
            ['request_fbc_operations_id'],
            unique=False,
        )

    op.create_table(
        'request_fbc_operations_fragment_resolved',
        sa.Column('request_fbc_operations_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('image_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ['image_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['request_fbc_operations_id'],
            ['request_fbc_operations.id'],
        ),
        sa.PrimaryKeyConstraint('request_fbc_operations_id', 'image_id'),
        sa.UniqueConstraint('request_fbc_operations_id', 'image_id'),
    )
    with op.batch_alter_table('request_fbc_operations_fragment_resolved', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_fbc_operations_fragment_resolved_image_id'),
            ['image_id'],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f('ix_request_fbc_operations_fragment_resolved_request_fbc_operations_id'),
            ['request_fbc_operations_id'],
            unique=False,
        )

    with op.batch_alter_table('request_fbc_operations', schema=None) as batch_op:
        batch_op.add_column(sa.Column('used_fbc_fragment', sa.Boolean(), nullable=True))
        batch_op.drop_column('fbc_fragment_id')


def downgrade():
    with op.batch_alter_table('request_fbc_operations', schema=None) as batch_op:
        batch_op.add_column(sa.Column('fbc_fragment_id', sa.INTEGER(), nullable=True))
        batch_op.create_foreign_key(
            'request_fbc_operations_fbc_fragment_id_fkey', 'image', ['fbc_fragment_id'], ['id']
        )
        batch_op.drop_column('used_fbc_fragment')

    with op.batch_alter_table('request_fbc_operations_fragment_resolved', schema=None) as batch_op:
        batch_op.drop_index(
            batch_op.f('ix_request_fbc_operations_fragment_resolved_request_fbc_operations_id')
        )
        batch_op.drop_index(batch_op.f('ix_request_fbc_operations_fragment_resolved_image_id'))

    op.drop_table('request_fbc_operations_fragment_resolved')
    with op.batch_alter_table('request_fbc_operations_fragment', schema=None) as batch_op:
        batch_op.drop_index(
            batch_op.f('ix_request_fbc_operations_fragment_request_fbc_operations_id')
        )
        batch_op.drop_index(batch_op.f('ix_request_fbc_operations_fragment_image_id'))

    op.drop_table('request_fbc_operations_fragment')
