"""
Add batches for requests.

Revision ID: 56d96595c0f7
Revises: 5d6808c0ce1f
Create Date: 2020-04-23 15:52:38.614572

"""
import logging

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '56d96595c0f7'
down_revision = '5d6808c0ce1f'
branch_labels = None
depends_on = None

log = logging.getLogger('alembic')

request_table = sa.Table(
    'request',
    sa.MetaData(),
    sa.Column('id', sa.Integer(), primary_key=True),
    sa.Column('batch_id', sa.Integer()),
)

batch_table = sa.Table('batch', sa.MetaData(), sa.Column('id', sa.Integer(), primary_key=True))


def upgrade():
    op.create_table(
        'batch', sa.Column('id', sa.Integer(), nullable=False), sa.PrimaryKeyConstraint('id')
    )

    with op.batch_alter_table('request') as batch_op:
        batch_op.add_column(sa.Column('batch_id', sa.Integer(), nullable=True))
        batch_op.create_index(batch_op.f('ix_request_batch_id'), ['batch_id'], unique=False)
        batch_op.create_foreign_key('request_batch_id_fk', 'batch', ['batch_id'], ['id'])

    connection = op.get_bind()
    # Iterate through all the existing requests
    for request in connection.execute(request_table.select()).fetchall():
        # Create a new batch per request
        connection.execute(batch_table.insert())
        # Get the ID of the last created batch
        new_batch_id = connection.execute(
            batch_table.select().order_by(batch_table.c.id.desc()).limit(1)
        ).scalar()
        # Set the request's batch as the last created batch
        log.info('Adding request %d to batch %d', request.id, new_batch_id)
        connection.execute(
            request_table.update()
            .where(request_table.c.id == request.id)
            .values(batch_id=new_batch_id)
        )

    # Now that the batches are all set on the requests, make the batch value not nullable
    with op.batch_alter_table('request') as batch_op:
        batch_op.alter_column('batch_id', existing_type=sa.INTEGER(), nullable=False)


def downgrade():
    with op.batch_alter_table('request') as batch_op:
        batch_op.drop_constraint('request_batch_id_fk', type_='foreignkey')
        batch_op.drop_index(batch_op.f('ix_request_batch_id'))
        batch_op.drop_column('batch_id')

    op.drop_table('batch')
