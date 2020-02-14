# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.web import models
from iib.web.models import Image, Request


@mock.patch('iib.web.api_v1.ping')
def test_example_endpoint(mock_ping, db, client):
    response = client.get('/api/v1/test')
    mock_ping.delay.assert_called_once()
    assert response.status_code == 200
    assert response.data == b'Test request success!'


def test_get_build(app, auth_env, client, db):
    # flask_login.current_user is used in Request.from_json, which requires a request context
    with app.test_request_context(environ_base=auth_env):
        data = {
            'binary_image': 'quay.io/namespace/binary_image:latest',
            'bundles': [f'quay.io/namespace/bundle:1.0-3'],
            'from_index': f'quay.io/namespace/repo:latest',
        }
        request = Request.from_json(data)
        request.binary_image_resolved = Image.get_or_create(
            'quay.io/namespace/binary_image@sha256:abcdef'
        )
        request.from_index_resolved = Image.get_or_create(
            'quay.io/namespace/from_index@sha256:defghi'
        )
        request.index_image = Image.get_or_create('quay.io/namespace/index@sha256:fghijk')
        request.add_architecture('amd64')
        request.add_architecture('s390x')
        request.add_state('complete', 'Completed successfully')
        db.session.add(request)
        db.session.commit()

    rv = client.get('/api/v1/builds/1').json
    for state in rv['state_history']:
        # Set this to a stable timestamp so the tests are dependent on it
        state['updated'] = '2020-02-12T17:03:00Z'
    rv['updated'] = '2020-02-12T17:03:00Z'

    expected = {
        'arches': ['amd64', 's390x'],
        'binary_image': 'quay.io/namespace/binary_image:latest',
        'binary_image_resolved': 'quay.io/namespace/binary_image@sha256:abcdef',
        'bundles': ['quay.io/namespace/bundle:1.0-3'],
        'from_index': 'quay.io/namespace/repo:latest',
        'from_index_resolved': 'quay.io/namespace/from_index@sha256:defghi',
        'id': 1,
        'index_image': 'quay.io/namespace/index@sha256:fghijk',
        'state': 'complete',
        'state_history': [
            {
                'state': 'complete',
                'state_reason': 'Completed successfully',
                'updated': '2020-02-12T17:03:00Z',
            },
            {
                'state': 'in_progress',
                'state_reason': 'The request was initiated',
                'updated': '2020-02-12T17:03:00Z',
            },
        ],
        'state_reason': 'Completed successfully',
        'updated': '2020-02-12T17:03:00Z',
        'user': 'tbrady@DOMAIN.LOCAL',
    }
    assert rv == expected


def test_get_builds(app, auth_env, client, db):
    total_requests = 50
    # flask_login.current_user is used in Request.from_json, which requires a request context
    with app.test_request_context(environ_base=auth_env):
        for i in range(total_requests):
            data = {
                'binary_image': 'quay.io/namespace/binary_image:latest',
                'bundles': [f'quay.io/namespace/bundle:{i}'],
                'from_index': f'quay.io/namespace/repo:{i}',
            }
            request = Request.from_json(data)
            if i % 5 == 0:
                request.add_state('failed', 'Failed due to an unknown error')
            db.session.add(request)
        db.session.commit()

    rv_json = client.get('/api/v1/builds?page=2').json
    assert len(rv_json['items']) == app.config['IIB_MAX_PER_PAGE']
    # This key is only present the verbose=true
    assert 'state_history' not in rv_json['items'][0]
    assert rv_json['meta']['page'] == 2
    assert rv_json['meta']['pages'] == 3
    assert rv_json['meta']['per_page'] == app.config['IIB_MAX_PER_PAGE']
    assert rv_json['meta']['total'] == total_requests

    rv_json = client.get('/api/v1/builds?state=failed&per_page=5').json
    total_failed_requests = total_requests // 5
    assert len(rv_json['items']) == 5
    assert 'state=failed' in rv_json['meta']['next']
    assert rv_json['meta']['page'] == 1
    assert rv_json['meta']['pages'] == 2
    assert rv_json['meta']['per_page'] == 5
    assert rv_json['meta']['total'] == total_failed_requests

    rv_json = client.get('/api/v1/builds?verbose=true&per_page=1').json
    # This key is only present the verbose=true
    assert 'state_history' in rv_json['items'][0]


def test_get_builds_invalid_state(app, client, db):
    rv = client.get('/api/v1/builds?state=is_it_lunch_yet%3F')
    assert rv.status_code == 400
    assert rv.json == {
        'error': (
            'is_it_lunch_yet? is not a valid build request state. Valid states are: complete, '
            'failed, in_progress'
        )
    }


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {
                'bundles': ['some:thing'],
                'from_index': 'pull:spec',
                'binary_image': '',
                'add_arches': ['s390x'],
            },
            '"binary_image" should be a non-empty string',
        ),
        (
            {
                'bundles': [],
                'from_index': 'pull:spec',
                'binary_image': 'binary:img',
                'add_arches': ['s390x'],
            },
            '"bundles" should be a non-empty array of strings',
        ),
        (
            {
                'bundles': ['some:thing'],
                'from_index': 32,
                'binary_image': 'binary:image',
                'add_arches': ['s390x'],
            },
            '"from_index" must be a string',
        ),
        (
            {
                'bundles': ['something'],
                'from_index': 'pull_spec',
                'binary_image': 'binary_image',
                'add_arches': [1, 2, 3],
            },
            'Architectures should be specified as a non-empty array of strings',
        ),
    ),
)
def test_add_bundle_invalid_params_format(data, error_msg, db, auth_env, client):
    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {'from_index': 'pull:spec', 'binary_image': 'binary:image', 'add_arches': ['s390x1']},
            'Missing required parameter(s): bundles',
        ),
        (
            {'bundles': ['some:thing'], 'from_index': 'pull:spec', 'add_arches': ['s390x1']},
            'Missing required parameter(s): binary_image',
        ),
    ),
)
def test_add_bundle_missing_required_param(data, error_msg, db, auth_env, client):
    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == error_msg


def test_add_bundle_invalid_param(db, auth_env, client):
    data = {
        'best_batsman': 'Virat Kohli',
        'binary_image': 'binary:image',
        'bundles': ['some:thing'],
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'The following parameters are invalid: best_batsman'


def test_add_bundle_from_index_and_add_arches_missing(db, auth_env, client):
    data = {'bundles': ['some:thing'], 'binary_image': 'binary:image'}

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'One of "from_index" or "add_arches" must be specified'


@mock.patch('iib.web.api_v1.handle_add_request')
def test_add_bundle_success(mock_har, db, auth_env, client):
    data = {'bundles': ['some:thing'], 'binary_image': 'binary:image', 'add_arches': ['s390x']}

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
        'state_history': [
            {
                'state': 'in_progress',
                'state_reason': 'The request was initiated',
                'updated': '2020-02-12T17:03:00Z',
            }
        ],
        'state_reason': 'The request was initiated',
        'updated': '2020-02-12T17:03:00Z',
        'user': 'tbrady@DOMAIN.LOCAL',
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    rv_json = rv.json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    assert rv.status_code == 201
    assert response_json == rv_json
    mock_har.apply_async.assert_called_once()


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {
                'arches': [''],
                'binary_image_resolved': 'resolved:binary',
                'from_index_resolved': 'resolved:index',
                'index_image': 'index:image',
                'state': 'state',
                'state_reason': 'state:reason',
            },
            'Architectures should be specified as a non-empty array of strings',
        ),
        (
            {
                'arches': ['s390x'],
                'binary_image_resolved': '',
                'from_index_resolved': 'resolved:index',
                'index_image': 'index:image',
                'state': 'state',
                'state_reason': 'state:reason',
            },
            'The value for "binary_image_resolved" must be a non-empty string',
        ),
        (
            {
                'arches': ['s390x'],
                'binary_image_resolved': 'resolved:binary',
                'from_index_resolved': 'resolved:index',
                'index_image': 'index:image',
                'state_reason': 'state_reason',
            },
            'The "state" key is required when "state_reason" is supplied',
        ),
    ),
)
def test_patch_request_invalid_params_format(data, error_msg, db, worker_auth_env, client):
    binary_image = models.Image(pull_specification='quay.io/image:latest')
    db.session.add(binary_image)
    request = models.Request(binary_image=binary_image, type=models.RequestTypeMapping.add.value,)
    db.session.add(request)

    rv = client.patch('/api/v1/builds/1', json=data, environ_base=worker_auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


def test_patch_request_success(db, worker_auth_env, client):
    data = {
        'arches': ['arches'],
        'state': 'complete',
        'state_reason': 'All done!',
        'index_image': 'index:image',
    }

    response_json = {
        'arches': ['arches'],
        'binary_image': 'quay.io/image:latest',
        'binary_image_resolved': None,
        'bundles': [],
        'from_index': None,
        'from_index_resolved': None,
        'id': 1,
        'index_image': 'index:image',
        'state': 'complete',
        'state_history': [
            {'state': 'complete', 'state_reason': 'All done!', 'updated': '2020-02-12T17:03:00Z'},
            {
                'state': 'in_progress',
                'state_reason': 'Starting things up',
                'updated': '2020-02-12T17:03:00Z',
            },
        ],
        'state_reason': 'All done!',
        'updated': '2020-02-12T17:03:00Z',
        'user': None,
    }

    binary_image = models.Image(pull_specification='quay.io/image:latest')
    db.session.add(binary_image)
    request = models.Request(binary_image=binary_image, type=models.RequestTypeMapping.add.value,)
    db.session.add(request)
    request.add_state('in_progress', 'Starting things up')
    db.session.commit()

    rv = client.patch('/api/v1/builds/1', json=data, environ_base=worker_auth_env)
    rv_json = rv.json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['state_history'][1]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    assert rv.status_code == 200
    assert rv_json == response_json
