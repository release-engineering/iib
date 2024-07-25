"""Add add-deprecations API endpoint.

Revision ID: 49d13af4b328
Revises: 1920ad83d0ab
Create Date: 2024-07-26 00:17:44.283197

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '49d13af4b328'
down_revision = '1920ad83d0ab'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'deprecation_schema',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('schema', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    with op.batch_alter_table('deprecation_schema', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_deprecation_schema_schema'), ['schema'], unique=True)

    op.create_table(
        'request_add_deprecations',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
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
    op.create_table(
        'request_add_deprecations_deprecation_schema',
        sa.Column('request_add_deprecations_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('deprecation_schema_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ['deprecation_schema_id'],
            ['deprecation_schema.id'],
        ),
        sa.ForeignKeyConstraint(
            ['request_add_deprecations_id'],
            ['request_add_deprecations.id'],
        ),
        sa.PrimaryKeyConstraint('request_add_deprecations_id', 'deprecation_schema_id'),
        sa.UniqueConstraint('request_add_deprecations_id', 'deprecation_schema_id'),
    )
    with op.batch_alter_table(
        'request_add_deprecations_deprecation_schema', schema=None
    ) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_add_deprecations_deprecation_schema_deprecation_schema_id'),
            ['deprecation_schema_id'],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f(
                'ix_request_add_deprecations_deprecation_schema_request_add_deprecations_id'
            ),
            ['request_add_deprecations_id'],
            unique=False,
        )

    op.create_table(
        'request_add_deprecations_operator',
        sa.Column('request_add_deprecations_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('operator_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ['operator_id'],
            ['operator.id'],
        ),
        sa.ForeignKeyConstraint(
            ['request_add_deprecations_id'],
            ['request_add_deprecations.id'],
        ),
        sa.PrimaryKeyConstraint('request_add_deprecations_id', 'operator_id'),
        sa.UniqueConstraint('request_add_deprecations_id', 'operator_id'),
    )
    with op.batch_alter_table('request_add_deprecations_operator', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_request_add_deprecations_operator_operator_id'),
            ['operator_id'],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f('ix_request_add_deprecations_operator_request_add_deprecations_id'),
            ['request_add_deprecations_id'],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table('request_add_deprecations_operator', schema=None) as batch_op:
        batch_op.drop_index(
            batch_op.f('ix_request_add_deprecations_operator_request_add_deprecations_id')
        )
        batch_op.drop_index(batch_op.f('ix_request_add_deprecations_operator_operator_id'))

    op.drop_table('request_add_deprecations_operator')
    with op.batch_alter_table(
        'request_add_deprecations_deprecation_schema', schema=None
    ) as batch_op:
        batch_op.drop_index(
            batch_op.f('ix_request_add_deprecations_deprecation_schema_request_add_deprecations_id')
        )
        batch_op.drop_index(
            batch_op.f('ix_request_add_deprecations_deprecation_schema_deprecation_schema_id')
        )

    op.drop_table('request_add_deprecations_deprecation_schema')
    op.drop_table('request_add_deprecations')
    with op.batch_alter_table('deprecation_schema', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_deprecation_schema_schema'))

    op.drop_table('deprecation_schema')
