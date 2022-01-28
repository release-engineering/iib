"""Support deprecation_list in Add request type.

Revision ID: eec630370e68
Revises: 60f89c046096
Create Date: 2021-01-20 20:36:29.184275

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eec630370e68'
down_revision = '60f89c046096'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'request_add_bundle_deprecation',
        sa.Column('request_add_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('bundle_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ['bundle_id'],
            ['image.id'],
        ),
        sa.ForeignKeyConstraint(
            ['request_add_id'],
            ['request_add.id'],
        ),
        sa.PrimaryKeyConstraint('request_add_id', 'bundle_id'),
        sa.UniqueConstraint(
            'request_add_id', 'bundle_id', name='request_add_bundle_deprecation_constraint'
        ),
    )
    with op.batch_alter_table('request_add_bundle_deprecation', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_add_bundle_deprecation_bundle_id'), ['bundle_id'], unique=False
        )
        batch_op.create_index(
            batch_op.f('ix_request_add_bundle_deprecation_request_add_id'),
            ['request_add_id'],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table('request_add_bundle_deprecation', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_request_add_bundle_deprecation_request_add_id'))
        batch_op.drop_index(batch_op.f('ix_request_add_bundle_deprecation_bundle_id'))

    op.drop_table('request_add_bundle_deprecation')
