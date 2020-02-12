# SPDX-License-Identifier: GPL-3.0-or-later
import datetime
from unittest import mock

import pytest


@mock.patch('iib.web.api_v1.ping')
def test_example_endpoint(mock_ping, db, client):
    response = client.get('/api/v1/test')
    mock_ping.delay.assert_called_once()
    assert response.status_code == 200
    assert response.data == b'Test request success!'


@pytest.mark.parametrize('bundles, from_index, binary_image, add_arches, error_msg', (
    (['some:thing'], 'pull:spec', '', ['s390x'], '"binary_image" should be a non-empty string'),
    ([], 'pull_spec', 'binary:img', ['s390x'], '"bundles" should be a non-empty array of strings'),
    (['some:thing'], 32, 'binary:image', ['s390x'], '"from_index" must be a string'),
    (
        ['something'],
        'pull_spec',
        'binary_image',
        [1, 2, 3],
        '"add_arches" should be an array of strings'
    ),
))
def test_add_bundle_invalid_params_format(
    bundles, from_index, binary_image, add_arches, error_msg, db, auth_env, client
):
    data = {
        'bundles': bundles,
        'from_index': from_index,
        'binary_image': binary_image,
        'add_arches': add_arches
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


def test_add_bundle_missing_required_param(db, auth_env, client):
    data = {
        'from_index': 'from_index',
        'binary_image': 'binary:image',
        'add_arches': ['add_arches']
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'Missing required parameter(s): bundles'


def test_add_bundle_invalid_param(db, auth_env, client):
    data = {
        'best_batsman': 'Virat Kohli',
        'binary_image': 'binary:image',
        'bundles': ['some:thing']
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'The following parameters are invalid: best_batsman'


def test_add_bundle_from_index_and_add_arches_missing(db, auth_env, client):
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image'
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'One of "from_index" or "add_arches" must be specified'


def test_add_bundle_success(db, auth_env, client):
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['s390x']
    }

    response_json = {
        'arches': [],
        'binary_image': 'binary:image',
        'binary_image_resolved': None,
        'bundles': ['some:thing'],
        'from_index': None,
        'from_index_resolved': None,
        'id': 1,
        'index_image': None,
        'state': 'in_progress',
        'state_history': [{
            'state': 'in_progress',
            'state_reason': 'The request was initiated',
            'updated': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        }],
        'state_reason': 'The request was initiated',
        'updated': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'user': 'tbrady@DOMAIN.LOCAL'
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    assert response_json == rv.json
