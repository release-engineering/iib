# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.web.models import Image, RequestAdd


def test_get_build(app, auth_env, client, db):
    # flask_login.current_user is used in RequestAdd.from_json, which requires a request context
    with app.test_request_context(environ_base=auth_env):
        data = {
            'binary_image': 'quay.io/namespace/binary_image:latest',
            'bundles': [f'quay.io/namespace/bundle:1.0-3'],
            'from_index': f'quay.io/namespace/repo:latest',
        }
        request = RequestAdd.from_json(data)
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
        'batch': 1,
        'binary_image': 'quay.io/namespace/binary_image:latest',
        'binary_image_resolved': 'quay.io/namespace/binary_image@sha256:abcdef',
        'bundle_mapping': {},
        'bundles': ['quay.io/namespace/bundle:1.0-3'],
        'from_index': 'quay.io/namespace/repo:latest',
        'from_index_resolved': 'quay.io/namespace/from_index@sha256:defghi',
        'id': 1,
        'index_image': 'quay.io/namespace/index@sha256:fghijk',
        'organization': None,
        'removed_operators': [],
        'request_type': 'add',
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
    # flask_login.current_user is used in RequestAdd.from_json, which requires a request context
    with app.test_request_context(environ_base=auth_env):
        for i in range(total_requests):
            data = {
                'binary_image': 'quay.io/namespace/binary_image:latest',
                'bundles': [f'quay.io/namespace/bundle:{i}'],
                'from_index': f'quay.io/namespace/repo:{i}',
            }
            request = RequestAdd.from_json(data)
            if i % 5 == 0:
                request.add_state('failed', 'Failed due to an unknown error')
            db.session.add(request)
        db.session.commit()

    rv_json = client.get('/api/v1/builds?page=2').json
    # Verify the order_by is correct
    assert rv_json['items'][0]['id'] == total_requests - app.config['IIB_MAX_PER_PAGE']
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

    rv_json = client.get('/api/v1/builds?batch=3').json
    assert len(rv_json['items']) == 1
    assert 'batch=3' in rv_json['meta']['first']
    assert rv_json['meta']['total'] == 1

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


@pytest.mark.parametrize('batch', (0, 'right_one'))
def test_get_builds_invalid_batch(batch, app, client, db):
    rv = client.get(f'/api/v1/builds?batch={batch}')
    assert rv.status_code == 400
    assert rv.json == {'error': 'The batch must be a positive integer'}


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
            '"binary_image" must be set',
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
                'bundles': ['some:thing'],
                'from_index': 'pull:spec',
                'binary_image': 'binary:image',
                'add_arches': [1, 2, 3],
            },
            'Architectures should be specified as a non-empty array of strings',
        ),
        (
            {
                'bundles': ['some:thing'],
                'from_index': 'pull:spec',
                'binary_image': 'binary:image',
                'overwrite_from_index': 123,
            },
            'The "overwrite_from_index" parameter must be a boolean',
        ),
    ),
)
def test_add_bundles_invalid_params_format(data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


def test_add_bundles_overwrite_not_allowed(client, db):
    data = {
        'binary_image': 'binary:image',
        'bundles': ['some:thing'],
        'from_index': 'pull:spec',
        'overwrite_from_index': True,
    }
    rv = client.post(f'/api/v1/builds/add', json=data, environ_base={'REMOTE_USER': 'tom_hanks'})
    assert rv.status_code == 403
    error_msg = 'You must be a privileged user to set "overwrite_from_index"'
    assert error_msg == rv.json['error']


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {'from_index': 'pull:spec', 'binary_image': 'binary:image', 'add_arches': ['s390x']},
            '"operators" should be a non-empty array of strings',
        ),
        (
            {
                'cnr_token': 'token',
                'from_index': 'pull:spec',
                'binary_image': 'binary:image',
                'operators': ['prometheus'],
            },
            'The following parameters are invalid: cnr_token',
        ),
        (
            {
                'organization': 'organization',
                'from_index': 'pull:spec',
                'binary_image': 'binary:image',
                'operators': ['prometheus'],
            },
            'The following parameters are invalid: organization',
        ),
    ),
)
def test_rm_operators_invalid_params_format(db, auth_env, client, data, error_msg):
    rv = client.post(f'/api/v1/builds/rm', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


def test_rm_operators_overwrite_not_allowed(client, db):
    data = {
        'binary_image': 'binary:image',
        'operators': ['prometheus'],
        'from_index': 'pull:spec',
        'overwrite_from_index': True,
    }
    rv = client.post(f'/api/v1/builds/rm', json=data, environ_base={'REMOTE_USER': 'tom_hanks'})
    assert rv.status_code == 403
    error_msg = 'You must be a privileged user to set "overwrite_from_index"'
    assert error_msg == rv.json['error']


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {'from_index': 'pull:spec', 'binary_image': 'binary:image', 'add_arches': ['s390x']},
            '"bundles" should be a non-empty array of strings',
        ),
        (
            {'bundles': ['some:thing'], 'from_index': 'pull:spec', 'add_arches': ['s390x']},
            'Missing required parameter(s): binary_image',
        ),
    ),
)
def test_add_bundle_missing_required_param(data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == error_msg


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {'operators': ['some:thing'], 'from_index': 'pull:spec', 'add_arches': ['s390x']},
            'Missing required parameter(s): binary_image',
        ),
        (
            {'from_index': 'pull:spec', 'binary_image': 'binary:image', 'add_arches': ['s390x']},
            '"operators" should be a non-empty array of strings',
        ),
        (
            {'operators': ['some:thing'], 'binary_image': 'pull:spec', 'add_arches': ['s390x']},
            'Missing required parameter(s): from_index',
        ),
    ),
)
def test_rm_operator_missing_required_param(data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/rm', json=data, environ_base=auth_env)
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


@pytest.mark.parametrize('overwrite_from_index', (False, True))
@mock.patch('iib.web.api_v1.handle_add_request')
def test_add_bundle_success(mock_har, overwrite_from_index, db, auth_env, client):
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['s390x'],
        'organization': 'org',
        'cnr_token': 'token',
        'overwrite_from_index': overwrite_from_index,
    }

    response_json = {
        'arches': [],
        'batch': 1,
        'binary_image': 'binary:image',
        'binary_image_resolved': None,
        'bundle_mapping': {},
        'bundles': ['some:thing'],
        'from_index': None,
        'from_index_resolved': None,
        'id': 1,
        'index_image': None,
        'removed_operators': [],
        'request_type': 'add',
        'state': 'in_progress',
        'organization': 'org',
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
    assert 'cnr_token' not in rv_json
    assert 'token' not in mock_har.apply_async.call_args[1]['argsrepr']
    assert '*****' in mock_har.apply_async.call_args[1]['argsrepr']
    mock_har.apply_async.assert_called_once()


@pytest.mark.parametrize('force_overwrite', (False, True))
@mock.patch('iib.web.api_v1.handle_add_request')
def test_add_bundle_forced_overwrite(mock_har, force_overwrite, app, auth_env, client, db):
    app.config['IIB_FORCE_OVERWRITE_FROM_INDEX'] = force_overwrite
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['amd64'],
        'overwrite_from_index': False,
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    mock_har.apply_async.assert_called_once()
    assert mock_har.apply_async.call_args[1]['args'][-1] == force_overwrite


@pytest.mark.parametrize(
    'user_to_queue, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, 'Buccaneers'),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_add_request')
def test_add_bundle_custom_user_queue(
    mock_har, app, auth_env, client, user_to_queue, expected_queue
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['s390x'],
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    mock_har.apply_async.assert_called_once()
    mock_har.apply_async.assert_called_with(
        args=mock.ANY, argsrepr=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )


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
        (
            {'bundle_mapping': 'not a dict'},
            'The "bundle_mapping" key must be an object with the values as lists of strings',
        ),
        (
            {'bundle_mapping': {'operator': 'not a list'}},
            'The "bundle_mapping" key must be an object with the values as lists of strings',
        ),
        (
            {'bundle_mapping': {'operator': [1, 2]}},
            'The "bundle_mapping" key must be an object with the values as lists of strings',
        ),
    ),
)
def test_patch_add_request_invalid_params_format(
    data, error_msg, minimal_request_add, worker_auth_env, client
):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request_add.id}', json=data, environ_base=worker_auth_env
    )
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


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
def test_patch_rm_request_invalid_params_format(
    data, error_msg, minimal_request_rm, worker_auth_env, client
):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request_rm.id}', json=data, environ_base=worker_auth_env
    )
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {
                'arches': [''],
                'from_bundle_image_resolved': 'resolved:bundle',
                'state': 'state',
                'state_reason': 'state:reason',
            },
            'Architectures should be specified as a non-empty array of strings',
        ),
        (
            {
                'arches': ['s390x'],
                'from_bundle_image_resolved': '',
                'state': 'state',
                'state_reason': 'state:reason',
            },
            'The value for "from_bundle_image_resolved" must be a non-empty string',
        ),
        (
            {
                'arches': ['s390x'],
                'from_bundle_image_resolved': 'resolved:bundle',
                'state_reason': 'state_reason',
            },
            'The "state" key is required when "state_reason" is supplied',
        ),
    ),
)
def test_patch_regenerate_bundle_request_invalid_params_format(
    data, error_msg, minimal_request_regenerate_bundle, worker_auth_env, client
):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request_regenerate_bundle.id}',
        json=data,
        environ_base=worker_auth_env,
    )
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


def test_patch_request_forbidden_user(minimal_request, worker_forbidden_env, client):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request.id}',
        json={'arches': ['s390x']},
        environ_base=worker_forbidden_env,
    )
    assert rv.status_code == 403
    assert 'This API endpoint is restricted to IIB workers' == rv.json['error']


def test_patch_request_add_success(db, minimal_request_add, worker_auth_env, client):
    bundles = [
        'quay.io/some-operator:v1.0.0',
        'quay.io/some-operator:v1.1.0',
        'quay.io/some-operator2:v2.0.0',
        'quay.io/some-operator2:v2.1.0',
    ]

    bundle_mapping = {
        'some-operator': bundles[0:2],
        'some-operator2': bundles[2:],
    }

    data = {
        'arches': ['arches'],
        'bundle_mapping': bundle_mapping,
        'state': 'complete',
        'state_reason': 'All done!',
        'index_image': 'index:image',
        'binary_image_resolved': 'binary-image@sha256:1234',
    }

    response_json = {
        'arches': ['arches'],
        'batch': 1,
        'binary_image': 'quay.io/add/binary-image:latest',
        'binary_image_resolved': 'binary-image@sha256:1234',
        'bundle_mapping': bundle_mapping,
        'bundles': bundles,
        'from_index': None,
        'from_index_resolved': None,
        'id': minimal_request_add.id,
        'index_image': 'index:image',
        'organization': None,
        'removed_operators': [],
        'request_type': 'add',
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

    for bundle in bundles:
        minimal_request_add.bundles.append(Image.get_or_create(bundle))
    minimal_request_add.add_state('in_progress', 'Starting things up')
    db.session.commit()

    rv = client.patch(
        f'/api/v1/builds/{minimal_request_add.id}', json=data, environ_base=worker_auth_env,
    )
    rv_json = rv.json
    assert rv.status_code == 200, rv_json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['state_history'][1]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    assert rv_json == response_json


def test_patch_request_rm_success(db, minimal_request_rm, worker_auth_env, client):
    data = {
        'arches': ['arches'],
        'state': 'complete',
        'state_reason': 'All done!',
        'index_image': 'index:image',
        'binary_image_resolved': 'binary-image@sha256:1234',
    }

    response_json = {
        'arches': ['arches'],
        'batch': 1,
        'binary_image': minimal_request_rm.binary_image.pull_specification,
        'binary_image_resolved': 'binary-image@sha256:1234',
        'bundle_mapping': {},
        'bundles': [],
        'from_index': 'quay.io/rm/index-image:latest',
        'from_index_resolved': None,
        'id': minimal_request_rm.id,
        'index_image': 'index:image',
        'organization': None,
        'removed_operators': ['operator'],
        'request_type': 'rm',
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

    minimal_request_rm.add_state('in_progress', 'Starting things up')
    db.session.commit()

    rv = client.patch(
        f'/api/v1/builds/{minimal_request_rm.id}', json=data, environ_base=worker_auth_env,
    )
    rv_json = rv.json
    assert rv.status_code == 200, rv_json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['state_history'][1]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    assert rv_json == response_json


def test_patch_request_regenerate_bundle_success(
    db, minimal_request_regenerate_bundle, worker_auth_env, client
):
    data = {
        'arches': ['arches'],
        'state': 'complete',
        'state_reason': 'All done!',
        'bundle_image': 'bundle:image',
        'from_bundle_image_resolved': 'from-bundle-image:resolved',
    }

    response_json = {
        'arches': ['arches'],
        'batch': 1,
        'bundle_image': 'bundle:image',
        'from_bundle_image': minimal_request_regenerate_bundle.from_bundle_image.pull_specification,
        'from_bundle_image_resolved': 'from-bundle-image:resolved',
        'id': minimal_request_regenerate_bundle.id,
        'organization': None,
        'request_type': 'regenerate-bundle',
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

    minimal_request_regenerate_bundle.add_state('in_progress', 'Starting things up')
    db.session.commit()

    rv = client.patch(
        f'/api/v1/builds/{minimal_request_regenerate_bundle.id}',
        json=data,
        environ_base=worker_auth_env,
    )
    rv_json = rv.json
    assert rv.status_code == 200, rv_json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['state_history'][1]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    assert rv_json == response_json


@mock.patch('iib.web.api_v1.handle_rm_request')
def test_remove_operator_success(mock_rm, db, auth_env, client):
    data = {
        'operators': ['some:thing'],
        'binary_image': 'binary:image',
        'from_index': 'index:image',
    }

    response_json = {
        'arches': [],
        'batch': 1,
        'binary_image': 'binary:image',
        'binary_image_resolved': None,
        'bundle_mapping': {},
        'bundles': [],
        'from_index': 'index:image',
        'from_index_resolved': None,
        'id': 1,
        'index_image': None,
        'organization': None,
        'removed_operators': ['some:thing'],
        'request_type': 'rm',
        'state': 'in_progress',
        'state_history': [
            {
                'state': 'in_progress',
                'state_reason': 'The request was initiated',
                'updated': '2020-02-12T17:03:00Z',
            },
        ],
        'state_reason': 'The request was initiated',
        'updated': '2020-02-12T17:03:00Z',
        'user': 'tbrady@DOMAIN.LOCAL',
    }

    rv = client.post('/api/v1/builds/rm', json=data, environ_base=auth_env)
    rv_json = rv.json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    mock_rm.apply_async.assert_called_once()
    assert rv.status_code == 201
    assert response_json == rv_json


@pytest.mark.parametrize('force_overwrite', (False, True))
@mock.patch('iib.web.api_v1.handle_rm_request')
def test_remove_operator_forced_overwrite(mock_hrr, force_overwrite, app, auth_env, client, db):
    app.config['IIB_FORCE_OVERWRITE_FROM_INDEX'] = force_overwrite
    data = {
        'binary_image': 'binary:image',
        'from_index': 'some:thing2',
        'operators': ['some:thing'],
        'overwrite_from_index': False,
    }

    rv = client.post('/api/v1/builds/rm', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    mock_hrr.apply_async.assert_called_once()
    assert mock_hrr.apply_async.call_args[1]['args'][-1] == force_overwrite


@pytest.mark.parametrize(
    'user_to_queue, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, 'Buccaneers'),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_rm_request')
def test_remove_operator_custom_user_queue(
    mock_hrr, app, auth_env, client, user_to_queue, expected_queue
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {
        'binary_image': 'binary:image',
        'from_index': 'some:thing2',
        'operators': ['some:thing'],
    }

    rv = client.post('/api/v1/builds/rm', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    mock_hrr.apply_async.assert_called_once()
    mock_hrr.apply_async.assert_called_with(
        args=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )


def test_not_found(client):
    rv = client.get('/api/v1/builds/1234')
    assert rv.status_code == 404
    assert rv.json == {'error': 'The requested resource was not found'}


@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
def test_regenerate_bundle_success(mock_hrbr, db, auth_env, client):
    data = {
        'from_bundle_image': 'registry.example.com/bundle-image:latest',
    }

    # Assume a timestamp to simplify tests
    _timestamp = '2020-02-12T17:03:00Z'

    response_json = {
        'arches': [],
        'batch': 1,
        'bundle_image': None,
        'from_bundle_image': 'registry.example.com/bundle-image:latest',
        'from_bundle_image_resolved': None,
        'organization': None,
        'id': 1,
        'request_type': 'regenerate-bundle',
        'state': 'in_progress',
        'state_history': [
            {
                'state': 'in_progress',
                'state_reason': 'The request was initiated',
                'updated': _timestamp,
            }
        ],
        'state_reason': 'The request was initiated',
        'updated': _timestamp,
        'user': 'tbrady@DOMAIN.LOCAL',
    }

    rv = client.post('/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    rv_json = rv.json
    rv_json['state_history'][0]['updated'] = _timestamp
    rv_json['updated'] = _timestamp
    assert response_json == rv_json
    mock_hrbr.apply_async.assert_called_once()


@pytest.mark.parametrize(
    'data, error_msg',
    (
        ({'from_bundle_image': ''}, '"from_bundle_image" must be set'),
        ({'from_bundle_image': 123}, '"from_bundle_image" must be a string'),
        (
            {'from_bundle_image': 'registry.example.com/bundle-image:latest', 'organization': 123},
            '"organization" must be a string',
        ),
        (
            {'from_bundle_image': 'registry.example.com/bundle-image:latest', 'spam': 'maps'},
            'The following parameters are invalid: spam',
        ),
    ),
)
def test_regenerate_bundle_invalid_params_format(data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']


@pytest.mark.parametrize(
    'data, error_msg',
    (
        ({}, 'Missing required parameter(s): from_bundle_image'),
        ({'organization': 'acme'}, 'Missing required parameter(s): from_bundle_image'),
    ),
)
def test_regenerate_bundle_missing_required_param(data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == error_msg


@pytest.mark.parametrize(
    'user_to_queue, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, 'Buccaneers'),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
def test_regenerate_bundle_custom_user_queue(
    mock_hrbr, app, auth_env, client, user_to_queue, expected_queue
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {'from_bundle_image': 'registry.example.com/bundle-image:latest'}

    rv = client.post('/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 201, rv.json
    mock_hrbr.apply_async.assert_called_once()
    mock_hrbr.apply_async.assert_called_with(
        args=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )
