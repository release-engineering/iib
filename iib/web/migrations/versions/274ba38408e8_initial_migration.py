"""
Initial migration.

Revision ID: 274ba38408e8
Create Date: 2020-02-06 13:26:54.944598
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '274ba38408e8'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'architecture',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_table(
        'operator',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_table(
        'image',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('operator_id', sa.Integer(), nullable=True),
        sa.Column('pull_specification', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['operator_id'], ['operator.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('pull_specification'),
    )
    op.create_table(
        'user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('username', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'request',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('binary_image_id', sa.Integer(), nullable=False),
        sa.Column('binary_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('from_index_id', sa.Integer(), nullable=True),
        sa.Column('from_index_resolved_id', sa.Integer(), nullable=True),
        sa.Column('index_image_id', sa.Integer(), nullable=True),
        sa.Column('type', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('organization', sa.String(), nullable=True),
        # We specify the constraint names here retroactively to match the
        # naming convention used by Postgreql. This will allow us to drop them
        # in a future migration.
        sa.ForeignKeyConstraint(
            ['binary_image_id'],
            ['image.id'],
            name='request_binary_image_id_fkey',
        ),
        sa.ForeignKeyConstraint(
            ['binary_image_resolved_id'],
            ['image.id'],
            name='request_binary_image_resolved_id_fkey',
        ),
        sa.ForeignKeyConstraint(
            ['from_index_id'],
            ['image.id'],
            name='request_from_index_id_fkey',
        ),
        sa.ForeignKeyConstraint(
            ['from_index_resolved_id'],
            ['image.id'],
            name='request_from_index_resolved_id_fkey',
        ),
        sa.ForeignKeyConstraint(
            ['index_image_id'], ['image.id'], name='request_index_image_id_fkey'
        ),
        sa.ForeignKeyConstraint(['user_id'], ['user.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'request_architecture',
        sa.Column('request_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('architecture_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(['architecture_id'], ['architecture.id']),
        sa.ForeignKeyConstraint(['request_id'], ['request.id']),
        sa.PrimaryKeyConstraint('request_id', 'architecture_id'),
        sa.UniqueConstraint('request_id', 'architecture_id'),
    )
    op.create_index(
        op.f('ix_request_architecture_architecture_id'),
        'request_architecture',
        ['architecture_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_request_architecture_request_id'),
        'request_architecture',
        ['request_id'],
        unique=False,
    )
    op.create_table(
        'request_bundle',
        sa.Column('request_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('image_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(['image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['request_id'], ['request.id']),
        sa.PrimaryKeyConstraint('request_id', 'image_id'),
        sa.UniqueConstraint('request_id', 'image_id'),
    )
    op.create_table(
        'request_operator',
        sa.Column('request_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('operator_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(['operator_id'], ['operator.id']),
        sa.ForeignKeyConstraint(['request_id'], ['request.id']),
        sa.PrimaryKeyConstraint('request_id', 'operator_id'),
        sa.UniqueConstraint('request_id', 'operator_id'),
    )
    op.create_index(
        op.f('ix_request_operator_operator_id'), 'request_operator', ['operator_id'], unique=False
    )
    op.create_index(
        op.f('ix_request_operator_request_id'), 'request_operator', ['request_id'], unique=False
    )
    op.create_index(
        op.f('ix_request_bundle_image_id'), 'request_bundle', ['image_id'], unique=False
    )
    op.create_index(
        op.f('ix_request_bundle_request_id'), 'request_bundle', ['request_id'], unique=False
    )
    op.create_table(
        'request_state',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('request_id', sa.Integer(), nullable=False),
        sa.Column('state', sa.Integer(), nullable=False),
        sa.Column('state_reason', sa.String(), nullable=False),
        sa.Column('updated', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['request_id'], ['request.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_request_state_request_id'), 'request_state', ['request_id'], unique=False
    )
    op.create_index(op.f('ix_user_username'), 'user', ['username'], unique=True)
    op.create_index(op.f('ix_operator_name'), 'operator', ['name'], unique=True)
    op.create_index(
        op.f('ix_image_pull_specification'), 'image', ['pull_specification'], unique=True
    )
    # This must be performed after the request and request_state tables are created
    with op.batch_alter_table('request', schema=None) as batch_op:
        batch_op.add_column(sa.Column('request_state_id', sa.Integer(), nullable=True))
        batch_op.create_index(
            batch_op.f('ix_request_request_state_id'), ['request_state_id'], unique=True
        )
        batch_op.create_foreign_key(
            'fk_request_state_id', 'request_state', ['request_state_id'], ['id']
        )


def downgrade():
    op.drop_index(op.f('ix_user_username'), table_name='user')
    op.drop_index(op.f('ix_image_pull_specification'), table_name='image')
    op.drop_index(op.f('ix_operator_name'), table_name='operator')
    op.drop_table('user')
    op.drop_index(op.f('ix_request_state_request_id'), table_name='request_state')
    op.drop_table('request_state')
    op.drop_index(op.f('ix_request_bundle_request_id'), table_name='request_bundle')
    op.drop_index(op.f('ix_request_bundle_image_id'), table_name='request_bundle')
    op.drop_table('request_bundle')
    op.drop_index(op.f('ix_request_operator_operator_id'), table_name='request_operator')
    op.drop_index(op.f('ix_request_operator_request_id'), table_name='request_operator')
    op.drop_table('request_operator')
    op.drop_index(op.f('ix_request_request_state_id'), table_name='request')
    op.drop_index(
        op.f('ix_request_architecture_architecture_id'), table_name='request_architecture'
    )
    op.drop_table('request_architecture')
    op.drop_table('request')
    op.drop_index(op.f('ix_request_architecture_request_id'), table_name='request_architecture')
    op.drop_table('image')
    op.drop_table('architecture')
    op.drop_table('operator')
