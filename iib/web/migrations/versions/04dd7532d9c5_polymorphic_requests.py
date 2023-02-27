"""
Use polymorphic tables for different types of build requests.

Revision ID: 04dd7532d9c5
Revises: 274ba38408e8
Create Date: 2020-04-17 14:52:40.766500

"""
from alembic import op
from sqlalchemy.sql.expression import select
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '04dd7532d9c5'
down_revision = '274ba38408e8'
branch_labels = None
depends_on = None


REQUEST_TYPE_ADD = 1
REQUEST_TYPE_RM = 2


# Create references to the various tables used to migrate data during
# the upgrade and the downgrade processes.

# sqlalchemy 2.0: https://docs.sqlalchemy.org/en/20/changelog/migration_20.html#migration-core-usage
# where clause parameter in select is not longer supported and list in select has been deprecated.
old_request_table = sa.Table(
    'request',
    sa.MetaData(),
    sa.Column('id', sa.Integer(), primary_key=True),
    sa.Column('type', sa.Integer()),
    sa.Column('binary_image_resolved_id', sa.Integer()),
    sa.Column('from_index_id', sa.Integer()),
    sa.Column('binary_image_id', sa.Integer()),
    sa.Column('from_index_resolved_id', sa.Integer()),
    sa.Column('organization', sa.Integer()),
    sa.Column('index_image_id', sa.Integer()),
)


request_add_table = sa.Table(
    'request_add',
    sa.MetaData(),
    sa.Column('id', sa.Integer()),
    sa.Column('organization', sa.String()),
    sa.Column('binary_image_id', sa.Integer()),
    sa.Column('binary_image_resolved_id', sa.Integer()),
    sa.Column('from_index_id', sa.Integer()),
    sa.Column('from_index_resolved_id', sa.Integer()),
    sa.Column('index_image_id', sa.Integer()),
)


request_rm_table = sa.Table(
    'request_rm',
    sa.MetaData(),
    sa.Column('id', sa.Integer()),
    sa.Column('binary_image_id', sa.Integer()),
    sa.Column('binary_image_resolved_id', sa.Integer()),
    sa.Column('from_index_id', sa.Integer()),
    sa.Column('from_index_resolved_id', sa.Integer()),
    sa.Column('index_image_id', sa.Integer()),
)


request_bundle_table = sa.Table(
    'request_bundle',
    sa.MetaData(),
    sa.Column('request_id', sa.Integer()),
    sa.Column('image_id', sa.Integer()),
)


request_add_bundle_table = sa.Table(
    'request_add_bundle',
    sa.MetaData(),
    sa.Column('request_add_id', sa.Integer()),
    sa.Column('image_id', sa.Integer()),
)


request_operator_table = sa.Table(
    'request_operator',
    sa.MetaData(),
    sa.Column('request_id', sa.Integer()),
    sa.Column('operator_id', sa.Integer()),
)


request_rm_operator_table = sa.Table(
    'request_rm_operator',
    sa.MetaData(),
    sa.Column('request_rm_id', sa.Integer()),
    sa.Column('operator_id', sa.Integer()),
)


def upgrade():
    op.create_table(
        'request_add',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('organization', sa.String(), nullable=True),
        sa.Column('binary_image_id', sa.Integer(), nullable=False),
        sa.Column('binary_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('from_index_id', sa.Integer(), nullable=True),
        sa.Column('from_index_resolved_id', sa.Integer(), nullable=True),
        sa.Column('index_image_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['binary_image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['binary_image_resolved_id'], ['image.id']),
        sa.ForeignKeyConstraint(['from_index_id'], ['image.id']),
        sa.ForeignKeyConstraint(['from_index_resolved_id'], ['image.id']),
        sa.ForeignKeyConstraint(['id'], ['request.id']),
        sa.ForeignKeyConstraint(['index_image_id'], ['image.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'request_add_bundle',
        sa.Column('request_add_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('image_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(['image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['request_add_id'], ['request_add.id']),
        sa.PrimaryKeyConstraint('request_add_id', 'image_id'),
        sa.UniqueConstraint('request_add_id', 'image_id'),
    )
    op.create_index(
        op.f('ix_request_add_bundle_image_id'),
        'request_add_bundle',
        ['image_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_request_add_bundle_request_add_id'),
        'request_add_bundle',
        ['request_add_id'],
        unique=False,
    )

    op.create_table(
        'request_rm',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('binary_image_id', sa.Integer(), nullable=False),
        sa.Column('binary_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('from_index_id', sa.Integer(), nullable=False),
        sa.Column('from_index_resolved_id', sa.Integer(), nullable=True),
        sa.Column('index_image_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['binary_image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['binary_image_resolved_id'], ['image.id']),
        sa.ForeignKeyConstraint(['from_index_id'], ['image.id']),
        sa.ForeignKeyConstraint(['from_index_resolved_id'], ['image.id']),
        sa.ForeignKeyConstraint(['id'], ['request.id']),
        sa.ForeignKeyConstraint(['index_image_id'], ['image.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'request_rm_operator',
        sa.Column('request_rm_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('operator_id', sa.Integer(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(['operator_id'], ['operator.id']),
        sa.ForeignKeyConstraint(['request_rm_id'], ['request_rm.id']),
        sa.PrimaryKeyConstraint('request_rm_id', 'operator_id'),
        sa.UniqueConstraint('request_rm_id', 'operator_id'),
    )
    op.create_index(
        op.f('ix_request_rm_operator_operator_id'),
        'request_rm_operator',
        ['operator_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_request_rm_operator_request_rm_id'),
        'request_rm_operator',
        ['request_rm_id'],
        unique=False,
    )

    with op.batch_alter_table('request_bundle') as batch_op:
        batch_op.drop_index('ix_request_bundle_image_id')
        batch_op.drop_index('ix_request_bundle_request_id')

    _upgrade_data()

    op.drop_table('request_bundle')
    with op.batch_alter_table('request_operator') as batch_op:
        batch_op.drop_index('ix_request_operator_operator_id')
        batch_op.drop_index('ix_request_operator_request_id')

    op.drop_table('request_operator')
    with op.batch_alter_table('request') as batch_op:
        batch_op.drop_constraint('request_binary_image_id_fkey', type_='foreignkey')
        batch_op.drop_constraint('request_binary_image_resolved_id_fkey', type_='foreignkey')
        batch_op.drop_constraint('request_from_index_id_fkey', type_='foreignkey')
        batch_op.drop_constraint('request_from_index_resolved_id_fkey', type_='foreignkey')
        batch_op.drop_constraint('request_index_image_id_fkey', type_='foreignkey')
        batch_op.drop_column('binary_image_resolved_id')
        batch_op.drop_column('from_index_id')
        batch_op.drop_column('binary_image_id')
        batch_op.drop_column('from_index_resolved_id')
        batch_op.drop_column('organization')
        batch_op.drop_column('index_image_id')


def _upgrade_data():
    connection = op.get_bind()

    connection.execute(
        request_add_table.insert().from_select(
            [
                'id',
                'organization',
                'binary_image_id',
                'binary_image_resolved_id',
                'from_index_id',
                'from_index_resolved_id',
                'index_image_id',
            ],
            select(
                old_request_table.c.id,
                old_request_table.c.organization,
                old_request_table.c.binary_image_id,
                old_request_table.c.binary_image_resolved_id,
                old_request_table.c.from_index_id,
                old_request_table.c.from_index_resolved_id,
                old_request_table.c.index_image_id,
            ).where(old_request_table.c.type == REQUEST_TYPE_ADD),
        )
    )

    connection.execute(
        request_add_bundle_table.insert().from_select(
            ['request_add_id', 'image_id'],
            select(request_bundle_table.c.request_id, request_bundle_table.c.image_id),
        )
    )

    connection.execute(
        request_rm_table.insert().from_select(
            [
                'id',
                'binary_image_id',
                'binary_image_resolved_id',
                'from_index_id',
                'from_index_resolved_id',
                'index_image_id',
            ],
            select(
                old_request_table.c.id,
                old_request_table.c.binary_image_id,
                old_request_table.c.binary_image_resolved_id,
                old_request_table.c.from_index_id,
                old_request_table.c.from_index_resolved_id,
                old_request_table.c.index_image_id,
            ).where(old_request_table.c.type == REQUEST_TYPE_RM),
        )
    )

    connection.execute(
        request_rm_operator_table.insert().from_select(
            ['request_rm_id', 'operator_id'],
            select(request_operator_table.c.request_id, request_operator_table.c.operator_id),
        )
    )


def downgrade():
    with op.batch_alter_table('request') as batch_op:
        batch_op.add_column(sa.Column('index_image_id', sa.INTEGER(), nullable=True))
        batch_op.add_column(sa.Column('organization', sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column('from_index_resolved_id', sa.INTEGER(), nullable=True))
        # We need to make this nullable, migrate the data, then we can set it
        # to not nullable
        batch_op.add_column(sa.Column('binary_image_id', sa.INTEGER(), nullable=True))
        batch_op.add_column(sa.Column('from_index_id', sa.INTEGER(), nullable=True))
        batch_op.add_column(sa.Column('binary_image_resolved_id', sa.INTEGER(), nullable=True))
        batch_op.create_foreign_key(
            'request_from_index_resolved_id_fkey',
            'image',
            ['from_index_resolved_id'],
            ['id'],
        )
        batch_op.create_foreign_key(
            'request_index_image_id_fkey',
            'image',
            ['index_image_id'],
            ['id'],
        )
        batch_op.create_foreign_key(
            'request_binary_image_resolved_id_fkey',
            'image',
            ['binary_image_resolved_id'],
            ['id'],
        )
        batch_op.create_foreign_key(
            'request_binary_image_id_fkey',
            'image',
            ['binary_image_id'],
            ['id'],
        )
        batch_op.create_foreign_key(
            'request_from_index_id_fkey',
            'image',
            ['from_index_id'],
            ['id'],
        )

    op.create_table(
        'request_operator',
        sa.Column('request_id', sa.INTEGER(), nullable=False),
        sa.Column('operator_id', sa.INTEGER(), nullable=False),
        sa.ForeignKeyConstraint(['operator_id'], ['operator.id']),
        sa.ForeignKeyConstraint(['request_id'], ['request.id']),
        sa.PrimaryKeyConstraint('request_id', 'operator_id'),
        sa.UniqueConstraint('request_id', 'operator_id'),
    )
    op.create_index(
        'ix_request_operator_request_id',
        'request_operator',
        ['request_id'],
        unique=False,
    )
    op.create_index(
        'ix_request_operator_operator_id',
        'request_operator',
        ['operator_id'],
        unique=False,
    )

    op.create_table(
        'request_bundle',
        sa.Column('request_id', sa.INTEGER(), nullable=False),
        sa.Column('image_id', sa.INTEGER(), nullable=False),
        sa.ForeignKeyConstraint(['image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['request_id'], ['request.id']),
        sa.PrimaryKeyConstraint('request_id', 'image_id'),
        sa.UniqueConstraint('request_id', 'image_id'),
    )
    op.create_index('ix_request_bundle_request_id', 'request_bundle', ['request_id'], unique=False)
    op.create_index('ix_request_bundle_image_id', 'request_bundle', ['image_id'], unique=False)

    _downgrade_data()

    # Although a single alter operation is performed, not using batch_alter_table
    # causes issues with sqlite: https://github.com/miguelgrinberg/Flask-Migrate/issues/306
    with op.batch_alter_table('request') as batch_op:
        batch_op.alter_column('binary_image_id', nullable=False)

    with op.batch_alter_table('request_rm_operator') as batch_op:
        batch_op.drop_index(batch_op.f('ix_request_rm_operator_request_rm_id'))
        batch_op.drop_index(batch_op.f('ix_request_rm_operator_operator_id'))

    op.drop_table('request_rm_operator')
    op.drop_table('request_rm')

    with op.batch_alter_table('request_add_bundle') as batch_op:
        batch_op.drop_index(batch_op.f('ix_request_add_bundle_request_add_id'))
        batch_op.drop_index(batch_op.f('ix_request_add_bundle_image_id'))

    op.drop_table('request_add_bundle')
    op.drop_table('request_add')


def _downgrade_data():
    connection = op.get_bind()

    connection.execute(
        request_bundle_table.insert().from_select(
            ['request_id', 'image_id'],
            select(request_add_bundle_table.c.request_add_id, request_add_bundle_table.c.image_id),
        )
    )

    # Ideally, a single statement could be used to update all the records.
    # However, SQLite doesn't support this operation and fails with:
    #   This backend does not support multiple-table criteria within UPDATE
    # More info:
    # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_using_multiple_table_updates.htm
    # For this reason, we must iterate through each record.
    for request_add in connection.execute(request_add_table.select()):
        connection.execute(
            old_request_table.update()
            .where(old_request_table.c.id == request_add.id)
            .values(
                binary_image_id=request_add.binary_image_id,
                binary_image_resolved_id=request_add.binary_image_resolved_id,
                from_index_id=request_add.from_index_id,
                from_index_resolved_id=request_add.from_index_resolved_id,
                index_image_id=request_add.index_image_id,
                organization=request_add.organization,
            )
        )

    connection.execute(
        request_operator_table.insert().from_select(
            ['request_id', 'operator_id'],
            select(
                request_rm_operator_table.c.request_rm_id, request_rm_operator_table.c.operator_id
            ),
        )
    )

    for request_rm in connection.execute(request_rm_table.select()):
        connection.execute(
            old_request_table.update()
            .where(old_request_table.c.id == request_rm.id)
            .values(
                binary_image_id=request_rm.binary_image_id,
                binary_image_resolved_id=request_rm.binary_image_resolved_id,
                from_index_id=request_rm.from_index_id,
                from_index_resolved_id=request_rm.from_index_resolved_id,
                index_image_id=request_rm.index_image_id,
            )
        )
