"""
Add regenerate-bundle request type.

Revision ID: 5d6808c0ce1f
Revises: 04dd7532d9c5
Create Date: 2020-04-20 15:25:49.509996
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5d6808c0ce1f'
down_revision = '04dd7532d9c5'
branch_labels = None
depends_on = None


REQUEST_TYPE_REGENERATE_BUNDLE = 3


# Create references to the tables used to migrate data during the upgrade
# and the downgrade processes.
request_table = sa.Table(
    'request',
    sa.MetaData(),
    sa.Column('id', sa.Integer(), primary_key=True),
    sa.Column('type', sa.Integer()),
)


def upgrade():
    op.create_table(
        'request_regenerate_bundle',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=False),
        sa.Column('bundle_image_id', sa.Integer(), nullable=True),
        sa.Column('from_bundle_image_id', sa.Integer(), nullable=False),
        sa.Column('from_bundle_image_resolved_id', sa.Integer(), nullable=True),
        sa.Column('organization', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['bundle_image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['from_bundle_image_id'], ['image.id']),
        sa.ForeignKeyConstraint(['from_bundle_image_resolved_id'], ['image.id']),
        sa.ForeignKeyConstraint(['id'], ['request.id']),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    connection = op.get_bind()

    # Before we can drop the request_regenerate_bundle table, we need to be sure
    # there are no records of that type in the database since the data loss is
    # irreversible.
    regenerate_bundle_requests = connection.execute(
        sa.select([sa.func.count()])
        .select_from(request_table)
        .where(request_table.c.type == REQUEST_TYPE_REGENERATE_BUNDLE)
    ).scalar()
    if regenerate_bundle_requests:
        raise RuntimeError(
            'Unable to perform migration. {} regenerate-bundle request(s) exist!'.format(
                regenerate_bundle_requests
            )
        )

    op.drop_table('request_regenerate_bundle')
