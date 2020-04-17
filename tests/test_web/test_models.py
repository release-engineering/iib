# SPDX-License-Identifier: GPL-3.0-or-later
import pytest

from iib.exceptions import ValidationError
from iib.web import models


def test_request_add_architecture(db, minimal_request):
    minimal_request.add_architecture('amd64')
    minimal_request.add_architecture('s390x')
    db.session.commit()
    assert len(minimal_request.architectures) == 2
    assert minimal_request.architectures[0].name == 'amd64'
    assert minimal_request.architectures[1].name == 's390x'

    # Verify that the method is idempotent
    minimal_request.add_architecture('amd64')
    db.session.commit()
    assert len(minimal_request.architectures) == 2


def test_request_add_state(db, minimal_request):
    minimal_request.add_state('in_progress', 'Starting things up')
    minimal_request.add_state('complete', 'All done!')
    db.session.commit()

    assert len(minimal_request.states) == 2
    assert minimal_request.state.state_name == 'complete'
    assert minimal_request.state.state_reason == 'All done!'
    assert minimal_request.states[0].state_name == 'in_progress'
    # Ensure that minimal_request.state is the latest state
    assert minimal_request.state == minimal_request.states[1]


def test_request_add_state_invalid_state(db, minimal_request):
    with pytest.raises(ValidationError, match='The state "invalid" is invalid'):
        minimal_request.add_state('invalid', 'Starting things up')


@pytest.mark.parametrize('state', ('complete', 'failed'))
def test_request_add_state_already_done(state, db, minimal_request):
    with pytest.raises(ValidationError, match=f'A {state} request cannot change states'):
        minimal_request.add_state(state, 'Done')
        db.session.commit()
        minimal_request.add_state('in_progress', 'Oops!')


def test_get_state_names():
    assert models.RequestStateMapping.get_names() == ['complete', 'failed', 'in_progress']


def test_get_type_names():
    assert models.RequestTypeMapping.get_names() == ['add', 'generic', 'rm']


@pytest.mark.parametrize(
    'type_num, is_valid', [(0, True), (1, True), (2, True), (3, False), ('1', False), (None, False)]
)
def test_request_type_validation(type_num, is_valid):
    if is_valid:
        models.Request(type=type_num)
    else:
        with pytest.raises(ValidationError, match=f'{type_num} is not a valid request type number'):
            models.Request(type=type_num)


@pytest.fixture(params=[models.RequestAdd, models.RequestRm])
def minimal_request(db, request):
    """
    Create and return an instance of the request class from the fixture params.

    The request instance will have the minimal set of required attributes set,
    and it'll be committed to the database.

    :param _pytest.fixtures.SubRequest request: the Request subclass to instantiate
    :param flask_sqlalchemy.SQLAlchemy db: the connection to the database
    :return: the newly created request object
    :rtype: Request
    """
    kwargs = {}

    request_class = request.param
    if request_class in (models.RequestAdd, models.RequestRm):
        binary_image = models.Image(pull_specification='quay.io/binary-image:latest')
        db.session.add(binary_image)
        kwargs['binary_image'] = binary_image

    if request_class == models.RequestRm:
        from_index_image = models.Image(pull_specification='quay.io/index-image:latest')
        db.session.add(from_index_image)
        kwargs['from_index'] = from_index_image

    request = request_class(**kwargs)
    db.session.add(request)
    return request
