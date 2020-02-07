# SPDX-License-Identifier: GPL-3.0-or-later
import pytest

from iib.exceptions import ValidationError
from iib.web import models


def test_image_add_architecture(db):
    image = models.Image(pull_specification='quay.io/image:latest')
    db.session.add(image)
    image.add_architecture('amd64')
    image.add_architecture('s390x')
    db.session.commit()
    assert len(image.architectures) == 2
    assert image.architectures[0].name == 'amd64'
    assert image.architectures[1].name == 's390x'

    # Verify that the method is idempotent
    image.add_architecture('amd64')
    db.session.commit()
    assert len(image.architectures) == 2


def test_request_add_state(db):
    binary_image = models.Image(pull_specification='quay.io/image:latest')
    db.session.add(binary_image)
    request = models.Request(
        binary_image=binary_image,
        type=models.RequestTypeMapping.add.value,
    )
    db.session.add(request)
    request.add_state('in_progress', 'Starting things up')
    request.add_state('complete', 'All done!')
    db.session.commit()

    assert len(request.states) == 2
    assert request.state.state_name == 'complete'
    assert request.state.state_reason == 'All done!'
    assert request.states[0].state_name == 'in_progress'
    # Ensure that request.state is the latest state
    assert request.state == request.states[1]


def test_request_add_state_invalid_state(db):
    binary_image = models.Image(pull_specification='quay.io/image:latest')
    db.session.add(binary_image)
    request = models.Request(
        binary_image=binary_image,
        type=models.RequestTypeMapping.add.value,
    )
    db.session.add(request)
    with pytest.raises(ValidationError, match='The state "invalid" is invalid'):
        request.add_state('invalid', 'Starting things up')


@pytest.mark.parametrize('state', ('complete', 'failed'))
def test_request_add_state_already_done(state, db):
    binary_image = models.Image(pull_specification='quay.io/image:latest')
    db.session.add(binary_image)
    request = models.Request(
        binary_image=binary_image,
        type=models.RequestTypeMapping.add.value,
    )
    db.session.add(request)
    with pytest.raises(ValidationError, match=f'A {state} request cannot change states'):
        request.add_state(state, 'Done')
        db.session.commit()
        request.add_state('in_progress', 'Oops!')


def test_get_state_names():
    assert models.RequestStateMapping.get_names() == ['complete', 'failed', 'in_progress']


def test_get_type_names():
    assert models.RequestTypeMapping.get_names() == ['add', 'rm']
