# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import requests
import pytest

from iib.exceptions import IIBError
from iib.workers import api_utils


@mock.patch('iib.workers.api_utils.requests_session')
def test_get_request(mock_session):
    mock_session.get.return_value.ok = True
    mock_session.get.return_value.json.return_value = '{"id": 3}'

    api_utils.get_request(3)

    mock_session.get.assert_called_once_with('http://iib-api:8080/api/v1/builds/3', timeout=30)


@mock.patch('iib.workers.api_utils.requests_session')
def test_get_request_connection_failed(mock_session):
    mock_session.get.side_effect = requests.ConnectionError()

    with pytest.raises(IIBError, match='The connection failed.+'):
        api_utils.get_request(3)


@mock.patch('iib.workers.api_utils.requests_session')
def test_get_request_not_ok(mock_session):
    mock_session.get.return_value.ok = False

    with pytest.raises(IIBError, match='The worker failed to get the request 3'):
        api_utils.get_request(3)


@mock.patch('iib.workers.api_utils.update_request')
def test_set_request_state(mock_update_request):
    state = 'failed'
    state_reason = 'Ran out of gas'
    api_utils.set_request_state(3, state, state_reason)

    mock_update_request.assert_called_once()
    mock_update_request.call_args[0][1] == {'state': state, 'state_reason': state_reason}


@mock.patch('iib.workers.api_utils.requests_auth_session')
def test_update_request(mock_session):
    mock_session.patch.return_value.ok = True
    mock_session.patch.return_value.json.return_value = '{"id": 3}'

    api_utils.update_request(3, {'index_image': 'index-image:latest'})

    mock_session.patch.assert_called_once_with(
        'http://iib-api:8080/api/v1/builds/3',
        json={'index_image': 'index-image:latest'},
        timeout=30,
    )


@mock.patch('iib.workers.api_utils.requests_auth_session')
def test_update_request_connection_failed(mock_session):
    mock_session.patch.side_effect = requests.ConnectionError()

    with pytest.raises(IIBError, match='The connection failed.+'):
        api_utils.update_request(3, {'index_image': 'index-image:latest'})


@pytest.mark.parametrize(
    'exc_msg, expected',
    (
        (None, 'The worker failed to update the request 3'),
        ('Failed to set index_image={index_image}', 'Failed to set index_image=index-image:latest'),
    ),
)
@mock.patch('iib.workers.api_utils.requests_auth_session')
def test_update_request_not_ok(mock_session, exc_msg, expected):
    mock_session.patch.return_value.ok = False

    with pytest.raises(IIBError, match=expected):
        api_utils.update_request(3, {'index_image': 'index-image:latest'}, exc_msg=exc_msg)
