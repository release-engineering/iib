"""Add internal_index_image_copy to Add and Rm.

Revision ID: 3283f52e7329
Revises: 5188702409d9
Create Date: 2022-06-10 01:41:18.583209

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3283f52e7329'
down_revision = '5188702409d9'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.add_column(sa.Column('internal_index_image_copy_id', sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column('internal_index_image_copy_resolved_id', sa.Integer(), nullable=True)
        )
        batch_op.create_foreign_key(
            "internal_index_image_copy_id_fkey", 'image', ['internal_index_image_copy_id'], ['id']
        )
        batch_op.create_foreign_key(
            "internal_index_image_copy_resolved_id_fkey",
            'image',
            ['internal_index_image_copy_resolved_id'],
            ['id'],
        )

    with op.batch_alter_table('request_create_empty_index', schema=None) as batch_op:
        batch_op.add_column(sa.Column('internal_index_image_copy_id', sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column('internal_index_image_copy_resolved_id', sa.Integer(), nullable=True)
        )
        batch_op.create_foreign_key(
            "internal_index_image_copy_id_fkey", 'image', ['internal_index_image_copy_id'], ['id']
        )
        batch_op.create_foreign_key(
            "internal_index_image_copy_resolved_id_fkey",
            'image',
            ['internal_index_image_copy_resolved_id'],
            ['id'],
        )

    with op.batch_alter_table('request_rm', schema=None) as batch_op:
        batch_op.add_column(sa.Column('internal_index_image_copy_id', sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column('internal_index_image_copy_resolved_id', sa.Integer(), nullable=True)
        )
        batch_op.create_foreign_key(
            "internal_index_image_copy_resolved_id_fkey",
            'image',
            ['internal_index_image_copy_resolved_id'],
            ['id'],
        )
        batch_op.create_foreign_key(
            "internal_index_image_copy_id_fkey", 'image', ['internal_index_image_copy_id'], ['id']
        )


def downgrade():
    with op.batch_alter_table('request_rm', schema=None) as batch_op:
        batch_op.drop_constraint("internal_index_image_copy_resolved_id_fkey", type_='foreignkey')
        batch_op.drop_constraint("internal_index_image_copy_id_fkey", type_='foreignkey')
        batch_op.drop_column('internal_index_image_copy_resolved_id')
        batch_op.drop_column('internal_index_image_copy_id')

    with op.batch_alter_table('request_create_empty_index', schema=None) as batch_op:
        batch_op.drop_constraint("internal_index_image_copy_resolved_id_fkey", type_='foreignkey')
        batch_op.drop_constraint("internal_index_image_copy_id_fkey", type_='foreignkey')
        batch_op.drop_column('internal_index_image_copy_resolved_id')
        batch_op.drop_column('internal_index_image_copy_id')

    with op.batch_alter_table('request_add', schema=None) as batch_op:
        batch_op.drop_constraint("internal_index_image_copy_resolved_id_fkey", type_='foreignkey')
        batch_op.drop_constraint("internal_index_image_copy_id_fkey", type_='foreignkey')
        batch_op.drop_column('internal_index_image_copy_resolved_id')
        batch_op.drop_column('internal_index_image_copy_id')
