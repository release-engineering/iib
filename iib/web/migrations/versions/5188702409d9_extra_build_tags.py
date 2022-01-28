"""Added BuildTag and RequestBuildTag.

Revision ID: 5188702409d9
Revises: e16a8cd2e028
Create Date: 2021-09-29 12:19:11.632047

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5188702409d9'
down_revision = 'e16a8cd2e028'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'build_tag',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'request_build_tag',
        sa.Column('request_id', sa.Integer(), nullable=False),
        sa.Column('tag_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ['request_id'],
            ['request.id'],
        ),
        sa.ForeignKeyConstraint(
            ['tag_id'],
            ['build_tag.id'],
        ),
        sa.PrimaryKeyConstraint('request_id', 'tag_id'),
        sa.UniqueConstraint('request_id', 'tag_id'),
    )
    with op.batch_alter_table('request_build_tag', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_build_tag_request_id'), ['request_id'], unique=False
        )
        batch_op.create_index(batch_op.f('ix_request_build_tag_tag_id'), ['tag_id'], unique=False)

    with op.batch_alter_table('request_create_empty_index', schema=None) as batch_op:
        batch_op.alter_column('from_index_id', existing_type=sa.INTEGER(), nullable=True)


def downgrade():
    with op.batch_alter_table('request_create_empty_index', schema=None) as batch_op:
        batch_op.alter_column('from_index_id', existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table('request_build_tag', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_request_build_tag_tag_id'))
        batch_op.drop_index(batch_op.f('ix_request_build_tag_request_id'))

    op.drop_table('request_build_tag')
    op.drop_table('build_tag')
