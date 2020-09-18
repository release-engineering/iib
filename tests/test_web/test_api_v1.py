# SPDX-License-Identifier: GPL-3.0-or-later
import json
from unittest import mock

import pytest
from sqlalchemy.exc import DisconnectionError

from iib.web.models import Image, RequestAdd, RequestRm


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
    rv['logs']['expiration'] = '2020-02-15T17:03:00Z'

    expected = {
        'arches': ['amd64', 's390x'],
        'batch': 1,
        'batch_annotations': None,
        'binary_image': 'quay.io/namespace/binary_image:latest',
        'binary_image_resolved': 'quay.io/namespace/binary_image@sha256:abcdef',
        'bundle_mapping': {},
        'bundles': ['quay.io/namespace/bundle:1.0-3'],
        'from_index': 'quay.io/namespace/repo:latest',
        'from_index_resolved': 'quay.io/namespace/from_index@sha256:defghi',
        'id': 1,
        'index_image': 'quay.io/namespace/index@sha256:fghijk',
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
        'omps_operator_version': {},
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


@mock.patch('sqlalchemy.engine.base.Engine.execute')
def test_get_healthcheck_db_fail(mock_db_execute, app, client, db):
    mock_db_execute.side_effect = DisconnectionError('DB failed')
    rv = client.get('/api/v1/healthcheck')
    assert rv.status_code == 500
    assert rv.json == {'error': 'Database health check failed.'}


def test_get_healthcheck_ok(app, client, db):
    rv = client.get('/api/v1/healthcheck')
    assert rv.status_code == 200
    assert rv.json == {'status': 'Health check OK'}


@pytest.mark.parametrize(
    ('logs_content', 'expired', 'finalized', 'expected'),
    (
        ('foobar', False, False, {'status': 200, 'mimetype': 'text/plain', 'data': 'foobar'}),
        ('foobar', True, False, {'status': 200, 'mimetype': 'text/plain', 'data': 'foobar'}),
        ('', False, False, {'status': 200, 'mimetype': 'text/plain', 'data': ''}),
        ('', True, False, {'status': 200, 'mimetype': 'text/plain', 'data': ''}),
        (None, False, False, {'status': 200, 'mimetype': 'text/plain', 'data': ''}),
        (
            None,
            True,
            False,
            {'status': 410, 'mimetype': 'application/json', 'json': {'error': mock.ANY}},
        ),
        (
            None,
            False,
            True,
            {'status': 404, 'mimetype': 'application/json', 'json': {'error': mock.ANY}},
        ),
    ),
)
def test_get_build_logs(
    client, db, minimal_request_add, tmpdir, logs_content, expired, finalized, expected
):
    minimal_request_add.add_state('in_progress', 'Starting things up!')
    db.session.commit()

    client.application.config['IIB_REQUEST_LOGS_DIR'] = str(tmpdir)
    if expired:
        client.application.config['IIB_REQUEST_LOGS_DAYS_TO_LIVE'] = -1
    if finalized:
        minimal_request_add.add_state('complete', 'The request is complete')
        db.session.commit()
    request_id = minimal_request_add.id
    if logs_content is not None:
        tmpdir.join(f'{request_id}.log').write(logs_content)
    rv = client.get(f'/api/v1/builds/{request_id}/logs')
    assert rv.status_code == expected['status']
    assert rv.mimetype == expected['mimetype']
    if 'data' in expected:
        assert rv.data.decode('utf-8') == expected['data']
    if 'json' in expected:
        assert rv.json == expected['json']


def test_get_build_logs_not_configured(client, db, minimal_request_add):
    minimal_request_add.add_state('in_progress', 'Starting things up!')
    db.session.commit()

    client.application.config['IIB_REQUEST_LOGS_DIR'] = None
    request_id = minimal_request_add.id
    rv = client.get(f'/api/v1/builds/{request_id}/logs')
    assert rv.status_code == 404
    assert rv.mimetype == 'application/json'
    assert rv.json == {'error': mock.ANY}

    rv = client.get(f'/api/v1/builds/{request_id}')
    assert rv.status_code == 200
    assert 'logs' not in rv.json


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
        (
            {
                'bundles': ['some:thing'],
                'from_index': 'pull:spec',
                'binary_image': 'binary:image',
                'overwrite_from_index': True,
                'overwrite_from_index_token': True,
            },
            'The "overwrite_from_index_token" parameter must be a string',
        ),
        (
            {'bundles': ['some:thing'], 'binary_image': 'binary:image', 'cnr_token': True},
            '"cnr_token" must be a string',
        ),
        (
            {'bundles': ['some:thing'], 'binary_image': 'binary:image', 'organization': True},
            '"organization" must be a string',
        ),
        (
            {'bundles': ['some:thing'], 'binary_image': 'binary:image', 'force_backport': 'spam'},
            '"force_backport" must be a boolean',
        ),
    ),
)
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundles_invalid_params_format(mock_smfsc, data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundles_overwrite_not_allowed(mock_smfsc, client, db):
    data = {
        'binary_image': 'binary:image',
        'bundles': ['some:thing'],
        'from_index': 'pull:spec',
        'overwrite_from_index': True,
    }
    rv = client.post(f'/api/v1/builds/add', json=data, environ_base={'REMOTE_USER': 'tom_hanks'})
    assert rv.status_code == 403
    error_msg = (
        'You must be a privileged user to set "overwrite_from_index" without setting '
        '"overwrite_from_index_token"'
    )
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


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
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_rm_operators_invalid_params_format(mock_smfsc, db, auth_env, client, data, error_msg):
    rv = client.post(f'/api/v1/builds/rm', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_rm_operators_overwrite_not_allowed(mock_smfsc, client, db):
    data = {
        'binary_image': 'binary:image',
        'operators': ['prometheus'],
        'from_index': 'pull:spec',
        'overwrite_from_index': True,
    }
    rv = client.post(f'/api/v1/builds/rm', json=data, environ_base={'REMOTE_USER': 'tom_hanks'})
    assert rv.status_code == 403
    error_msg = (
        'You must be a privileged user to set "overwrite_from_index" without setting '
        '"overwrite_from_index_token"'
    )
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


@pytest.mark.parametrize(
    'data, error_msg',
    (
        (
            {'bundles': ['some:thing'], 'from_index': 'pull:spec', 'add_arches': ['s390x']},
            'Missing required parameter(s): binary_image',
        ),
        (
            {'from_index': 'pull:spec', 'add_arches': ['s390x']},
            '"from_index" and "binary_image" must be specified if no bundles are specified',
        ),
        (
            {'add_arches': ['s390x'], 'binary_image': 'binary:image'},
            '"from_index" and "binary_image" must be specified if no bundles are specified',
        ),
        (
            {'add_arches': ['s390x']},
            '"from_index" and "binary_image" must be specified if no bundles are specified',
        ),
        (
            {
                'bundles': ['some:thing'],
                'binary_image': 'binary:image',
                'add_arches': ['s390x'],
                'overwrite_from_index_token': 'username:password',
            },
            (
                'The "overwrite_from_index" parameter is required when the '
                '"overwrite_from_index_token" parameter is used'
            ),
        ),
    ),
)
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_missing_required_param(mock_smfsc, data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == error_msg
    mock_smfsc.assert_not_called()


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
        (
            {
                'operators': ['some:thing'],
                'binary_image': 'pull:spec',
                'from_index': 'pull:spec',
                'overwrite_from_index_token': 'username:password',
            },
            (
                'The "overwrite_from_index" parameter is required when the '
                '"overwrite_from_index_token" parameter is used'
            ),
        ),
    ),
)
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_rm_operator_missing_required_param(mock_smfsc, data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/rm', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == error_msg
    mock_smfsc.assert_not_called()


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_invalid_param(mock_smfsc, db, auth_env, client):
    data = {
        'best_batsman': 'Virat Kohli',
        'binary_image': 'binary:image',
        'bundles': ['some:thing'],
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'The following parameters are invalid: best_batsman'
    mock_smfsc.assert_not_called()


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_from_index_and_add_arches_missing(mock_smfsc, db, auth_env, client):
    data = {'bundles': ['some:thing'], 'binary_image': 'binary:image'}

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == 'One of "from_index" or "add_arches" must be specified'
    mock_smfsc.assert_not_called()


@pytest.mark.parametrize(
    ('overwrite_from_index', 'bundles', 'from_index'),
    (
        (False, ['some:thing'], None),
        (False, [], 'some:thing'),
        (True, ['some:thing'], 'some:thing'),
        (True, [], 'some:thing'),
    ),
)
@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_success(
    mock_smfsc, mock_har, overwrite_from_index, db, auth_env, client, bundles, from_index,
):
    data = {
        'binary_image': 'binary:image',
        'add_arches': ['s390x'],
        'organization': 'org',
        'cnr_token': 'token',
        'overwrite_from_index': overwrite_from_index,
        'from_index': from_index,
    }
    if bundles:
        data['bundles'] = bundles

    response_json = {
        'arches': [],
        'batch': 1,
        'batch_annotations': None,
        'binary_image': 'binary:image',
        'binary_image_resolved': None,
        'bundle_mapping': {},
        'bundles': bundles,
        'from_index': from_index,
        'from_index_resolved': None,
        'id': 1,
        'index_image': None,
        'removed_operators': [],
        'request_type': 'add',
        'state': 'in_progress',
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
        'omps_operator_version': {},
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
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    assert rv.status_code == 201
    assert response_json == rv_json
    assert 'cnr_token' not in rv_json
    assert 'token' not in mock_har.apply_async.call_args[1]['argsrepr']
    assert '*****' in mock_har.apply_async.call_args[1]['argsrepr']
    mock_har.apply_async.assert_called_once()
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@pytest.mark.parametrize('force_overwrite', (False, True))
@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_forced_overwrite(
    mock_smfsc, mock_har, force_overwrite, app, auth_env, client, db
):
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
    # Third to last element in args is the overwrite_from_index parameter
    assert mock_har.apply_async.call_args[1]['args'][-3] == force_overwrite
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@pytest.mark.parametrize('force_backport', (False, True))
@mock.patch('iib.web.api_v1.handle_add_request')
def test_add_bundle_force_backport(mock_har, force_backport, db, auth_env, client):
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'from_index': 'index:image',
        'force_backport': force_backport,
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    mock_har.apply_async.assert_called_once()
    # Eigth element in args is the force_backport parameter
    assert mock_har.apply_async.call_args[1]['args'][7] == force_backport


@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_overwrite_token_redacted(mock_smfsc, mock_har, app, auth_env, client, db):
    token = 'username:password'
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['amd64'],
        'overwrite_from_index': True,
        'overwrite_from_index_token': token,
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    rv_json = rv.json
    assert rv.status_code == 201
    mock_har.apply_async.assert_called_once()
    # Third to last element in args is the overwrite_from_index parameter
    assert mock_har.apply_async.call_args[1]['args'][-3] is True
    # Second to last element in args is the overwrite_from_index_token parameter
    assert mock_har.apply_async.call_args[1]['args'][-2] == token
    assert 'overwrite_from_index_token' not in rv_json
    assert token not in json.dumps(rv_json)
    assert token not in mock_har.apply_async.call_args[1]['argsrepr']
    assert '*****' in mock_har.apply_async.call_args[1]['argsrepr']


@pytest.mark.parametrize(
    'user_to_queue, overwrite_from_index, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, False, 'Buccaneers'),
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, True, 'Buccaneers'),
        ({'PARALLEL:tbrady@DOMAIN.LOCAL': 'Buccaneers'}, False, 'Buccaneers'),
        ({'SERIAL:tbrady@DOMAIN.LOCAL': 'Buccaneers'}, True, 'Buccaneers'),
        (
            {'tbrady@DOMAIN.LOCAL': 'Patriots', 'PARALLEL:tbrady@DOMAIN.LOCAL': 'Buccaneers'},
            False,
            'Buccaneers',
        ),
        (
            {'tbrady@DOMAIN.LOCAL': 'Patriots', 'SERIAL:tbrady@DOMAIN.LOCAL': 'Buccaneers'},
            True,
            'Buccaneers',
        ),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, False, None),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, True, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_add_bundle_custom_user_queue(
    mock_smfsc, mock_har, app, auth_env, client, user_to_queue, overwrite_from_index, expected_queue
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['s390x'],
    }
    if overwrite_from_index:
        data['from_index'] = 'index:image'
        data['overwrite_from_index'] = True

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    assert rv.status_code == 201, rv.json
    mock_har.apply_async.assert_called_once()
    mock_har.apply_async.assert_called_with(
        args=mock.ANY, argsrepr=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


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
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_add_request_invalid_params_format(
    mock_smfsc, data, error_msg, minimal_request_add, worker_auth_env, client
):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request_add.id}', json=data, environ_base=worker_auth_env
    )
    assert rv.status_code == 400
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


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
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_rm_request_invalid_params_format(
    mock_smfsc, data, error_msg, minimal_request_rm, worker_auth_env, client
):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request_rm.id}', json=data, environ_base=worker_auth_env
    )
    assert rv.status_code == 400
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


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
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_regenerate_bundle_request_invalid_params_format(
    mock_smfsc, data, error_msg, minimal_request_regenerate_bundle, worker_auth_env, client
):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request_regenerate_bundle.id}',
        json=data,
        environ_base=worker_auth_env,
    )
    assert rv.status_code == 400
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_request_forbidden_user(mock_smfsc, minimal_request, worker_forbidden_env, client):
    rv = client.patch(
        f'/api/v1/builds/{minimal_request.id}',
        json={'arches': ['s390x']},
        environ_base=worker_forbidden_env,
    )
    assert rv.status_code == 403
    assert 'This API endpoint is restricted to IIB workers' == rv.json['error']
    mock_smfsc.assert_not_called()


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_request_add_success(mock_smfsc, db, minimal_request_add, worker_auth_env, client):
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
        'batch_annotations': None,
        'binary_image': 'quay.io/add/binary-image:latest',
        'binary_image_resolved': 'binary-image@sha256:1234',
        'bundle_mapping': bundle_mapping,
        'bundles': bundles,
        'from_index': None,
        'from_index_resolved': None,
        'id': minimal_request_add.id,
        'index_image': 'index:image',
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
        'omps_operator_version': {},
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
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    assert rv_json == response_json
    mock_smfsc.assert_called_once_with(mock.ANY)


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_request_rm_success(mock_smfsc, db, minimal_request_rm, worker_auth_env, client):
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
        'batch_annotations': None,
        'binary_image': minimal_request_rm.binary_image.pull_specification,
        'binary_image_resolved': 'binary-image@sha256:1234',
        'bundle_mapping': {},
        'bundles': [],
        'from_index': 'quay.io/rm/index-image:latest',
        'from_index_resolved': None,
        'id': minimal_request_rm.id,
        'index_image': 'index:image',
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
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
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    assert rv_json == response_json
    mock_smfsc.assert_called_once_with(mock.ANY)


@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_patch_request_regenerate_bundle_success(
    mock_smfsc, db, minimal_request_regenerate_bundle, worker_auth_env, client
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
        'batch_annotations': None,
        'bundle_image': 'bundle:image',
        'from_bundle_image': minimal_request_regenerate_bundle.from_bundle_image.pull_specification,
        'from_bundle_image_resolved': 'from-bundle-image:resolved',
        'id': minimal_request_regenerate_bundle.id,
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
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
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    assert rv_json == response_json
    mock_smfsc.assert_called_once_with(mock.ANY)


@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_remove_operator_success(mock_smfsc, mock_rm, db, auth_env, client):
    data = {
        'operators': ['some:thing'],
        'binary_image': 'binary:image',
        'from_index': 'index:image',
    }

    response_json = {
        'arches': [],
        'batch': 1,
        'batch_annotations': None,
        'binary_image': 'binary:image',
        'binary_image_resolved': None,
        'bundle_mapping': {},
        'bundles': [],
        'from_index': 'index:image',
        'from_index_resolved': None,
        'id': 1,
        'index_image': None,
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
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
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    mock_rm.apply_async.assert_called_once()
    assert rv.status_code == 201
    assert response_json == rv_json
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@pytest.mark.parametrize('force_overwrite', (False, True))
@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_remove_operator_forced_overwrite(
    mock_smfsc, mock_hrr, force_overwrite, app, auth_env, client, db
):
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
    # Second to last element in args is the overwrite_from_index parameter
    assert mock_hrr.apply_async.call_args[1]['args'][-2] == force_overwrite
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_remove_operator_overwrite_token_redacted(mock_smfsc, mock_hrr, app, auth_env, client, db):
    token = 'username:password'
    data = {
        'binary_image': 'binary:image',
        'from_index': 'some:thing2',
        'operators': ['some:thing'],
        'overwrite_from_index': True,
        'overwrite_from_index_token': token,
    }

    rv = client.post('/api/v1/builds/rm', json=data, environ_base=auth_env)
    rv_json = rv.json
    assert rv.status_code == 201
    mock_hrr.apply_async.assert_called_once()
    # Second to last element in args is the overwrite_from_index parameter
    assert mock_hrr.apply_async.call_args[1]['args'][-2] is True
    assert mock_hrr.apply_async.call_args[1]['args'][-1] == token
    assert 'overwrite_from_index_token' not in rv_json
    assert token not in json.dumps(rv_json)
    assert token not in mock_hrr.apply_async.call_args[1]['argsrepr']
    assert '*****' in mock_hrr.apply_async.call_args[1]['argsrepr']


@pytest.mark.parametrize(
    'user_to_queue, overwrite_from_index, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, False, 'Buccaneers'),
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, True, 'Buccaneers'),
        ({'PARALLEL:tbrady@DOMAIN.LOCAL': 'Buccaneers'}, False, 'Buccaneers'),
        ({'SERIAL:tbrady@DOMAIN.LOCAL': 'Buccaneers'}, True, 'Buccaneers'),
        (
            {'tbrady@DOMAIN.LOCAL': 'Patriots', 'PARALLEL:tbrady@DOMAIN.LOCAL': 'Buccaneers'},
            False,
            'Buccaneers',
        ),
        (
            {'tbrady@DOMAIN.LOCAL': 'Patriots', 'SERIAL:tbrady@DOMAIN.LOCAL': 'Buccaneers'},
            True,
            'Buccaneers',
        ),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, False, None),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, True, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_remove_operator_custom_user_queue(
    mock_smfsc, mock_hrr, app, auth_env, client, user_to_queue, overwrite_from_index, expected_queue
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {
        'binary_image': 'binary:image',
        'from_index': 'some:thing2',
        'operators': ['some:thing'],
    }
    if overwrite_from_index:
        data['from_index'] = 'index:image'
        data['overwrite_from_index'] = True

    rv = client.post('/api/v1/builds/rm', json=data, environ_base=auth_env)
    assert rv.status_code == 201, rv.json
    mock_hrr.apply_async.assert_called_once()
    mock_hrr.apply_async.assert_called_with(
        args=mock.ANY, argsrepr=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


def test_not_found(client):
    rv = client.get('/api/v1/builds/1234')
    assert rv.status_code == 404
    assert rv.json == {'error': 'The requested resource was not found'}


@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_regenerate_bundle_success(mock_smfsc, mock_hrbr, db, auth_env, client):
    data = {
        'from_bundle_image': 'registry.example.com/bundle-image:latest',
    }

    # Assume a timestamp to simplify tests
    _timestamp = '2020-02-12T17:03:00Z'

    response_json = {
        'arches': [],
        'batch': 1,
        'batch_annotations': None,
        'bundle_image': None,
        'from_bundle_image': 'registry.example.com/bundle-image:latest',
        'from_bundle_image_resolved': None,
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
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
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    assert response_json == rv_json
    mock_hrbr.apply_async.assert_called_once()
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


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
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_regenerate_bundle_invalid_params_format(mock_smfsc, data, error_msg, db, auth_env, client):
    rv = client.post(f'/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert error_msg == rv.json['error']
    mock_smfsc.assert_not_called()


@pytest.mark.parametrize(
    'data, error_msg',
    (
        ({}, 'Missing required parameter(s): from_bundle_image'),
        ({'organization': 'acme'}, 'Missing required parameter(s): from_bundle_image'),
    ),
)
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_regenerate_bundle_missing_required_param(
    mock_smfsc, data, error_msg, db, auth_env, client
):
    rv = client.post(f'/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 400
    assert rv.json['error'] == error_msg
    mock_smfsc.assert_not_called()


@pytest.mark.parametrize(
    'user_to_queue, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, 'Buccaneers'),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_regenerate_bundle_custom_user_queue(
    mock_smfsc, mock_hrbr, app, auth_env, client, user_to_queue, expected_queue
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {'from_bundle_image': 'registry.example.com/bundle-image:latest'}

    rv = client.post('/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    assert rv.status_code == 201, rv.json
    mock_hrbr.apply_async.assert_called_once()
    mock_hrbr.apply_async.assert_called_with(
        args=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@pytest.mark.parametrize(
    'user_to_queue, expected_queue, annotations',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Patriots'}, 'Patriots', None),
        ({'jabba@DOMAIN.LOCAL': 'The Hut'}, None, None),
        ({}, None, {'Han Solo': 'Don\'t everybody thank me at once.'}),
    ),
)
@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
@mock.patch('iib.web.api_v1.messaging.send_messages_for_new_batch_of_requests')
def test_regenerate_bundle_batch_success(
    mock_smfnbor, mock_hrbr, user_to_queue, expected_queue, annotations, app, auth_env, client, db
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue

    data = {
        'build_requests': [
            {'from_bundle_image': 'registry.example.com/bundle-image:latest'},
            {'from_bundle_image': 'registry.example.com/bundle-image2:latest'},
        ]
    }
    if annotations:
        data['annotations'] = annotations
    rv = client.post('/api/v1/builds/regenerate-bundle-batch', json=data, environ_base=auth_env)

    assert rv.status_code == 201, rv.json
    assert mock_hrbr.apply_async.call_count == 2
    mock_hrbr.apply_async.assert_has_calls(
        (
            mock.call(
                args=['registry.example.com/bundle-image:latest', None, 1],
                link_error=mock.ANY,
                queue=expected_queue,
            ),
            mock.call(
                args=['registry.example.com/bundle-image2:latest', None, 2],
                link_error=mock.ANY,
                queue=expected_queue,
            ),
        )
    )
    assert len(rv.json) == 2
    assert all(r['batch_annotations'] == annotations for r in rv.json)

    requests_to_send_msgs_for = mock_smfnbor.call_args[0][0]
    assert len(requests_to_send_msgs_for) == 2
    assert requests_to_send_msgs_for[0].id == 1
    assert requests_to_send_msgs_for[1].id == 2


@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
def test_regenerate_bundle_batch_invalid_request_type(mock_hrbr, app, auth_env, client, db):
    data = {
        'build_requests': [
            {'from_bundle_image': 'registry.example.com/bundle-image:latest'},
            {
                'binary_image': 'binary:image',
                'from_index': 'some:thing2',
                'operators': ['some:thing'],
            },
        ]
    }
    rv = client.post('/api/v1/builds/regenerate-bundle-batch', json=data, environ_base=auth_env)

    assert rv.status_code == 400, rv.json
    assert rv.json == {
        'error': (
            'Missing required parameter(s): from_bundle_image. This occurred on the build request '
            'in index 1.'
        )
    }
    mock_hrbr.apply_async.assert_not_called()


@pytest.mark.parametrize(
    'payload, error_msg',
    (
        (
            ['bundle:latest'],
            (
                'The input data must be a JSON object and the "build_requests" value must be a '
                'non-empty array'
            ),
        ),
        (
            {
                'build_requests': [
                    {'from_bundle_image': 'registry.example.com/bundle-image:latest'}
                ],
                'annotations': 'Will someone get this big walking carpet out of my way?',
            },
            'The value of "annotations" must be a JSON object',
        ),
    ),
)
def test_regenerate_bundle_batch_invalid_input(payload, error_msg, app, auth_env, client, db):
    rv = client.post('/api/v1/builds/regenerate-bundle-batch', json=payload, environ_base=auth_env)

    assert rv.status_code == 400, rv.json
    assert rv.json == {'error': error_msg}


@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_messages_for_new_batch_of_requests')
def test_add_rm_batch_success(mock_smfnbor, mock_hrr, mock_har, app, auth_env, client, db):
    annotations = {'msdhoni': 'The best captain ever!'}
    data = {
        'annotations': annotations,
        'build_requests': [
            {
                'bundles': ['registry-proxy/rh-osbs/lgallett-bundle:v1.0-9'],
                'binary_image': 'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5',
                'from_index': 'registry-proxy/rh-osbs-stage/iib:v4.5',
                'add_arches': ['amd64'],
                'cnr_token': 'no_tom_brady_anymore',
                'organization': 'hello-operator',
                'overwrite_from_index': True,
                'overwrite_from_index_token': 'some_token',
            },
            {
                'operators': ['kiali-ossm'],
                'binary_image': 'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5',
                'from_index': 'registry:8443/iib-build:11',
            },
        ],
    }
    rv = client.post('/api/v1/builds/add-rm-batch', json=data, environ_base=auth_env)

    assert rv.status_code == 201, rv.json
    assert mock_hrr.apply_async.call_count == 1
    assert mock_har.apply_async.call_count == 1
    mock_har.apply_async.assert_has_calls(
        (
            mock.call(
                args=[
                    ['registry-proxy/rh-osbs/lgallett-bundle:v1.0-9'],
                    'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5',
                    1,
                    'registry-proxy/rh-osbs-stage/iib:v4.5',
                    ['amd64'],
                    'no_tom_brady_anymore',
                    'hello-operator',
                    None,
                    True,
                    'some_token',
                    None,
                ],
                argsrepr=(
                    "[['registry-proxy/rh-osbs/lgallett-bundle:v1.0-9'], "
                    "'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5', 1, "
                    "'registry-proxy/rh-osbs-stage/iib:v4.5', ['amd64'], '*****', "
                    "'hello-operator', None, True, '*****', None]"
                ),
                link_error=mock.ANY,
                queue=None,
            ),
        )
    )
    mock_hrr.apply_async.assert_has_calls(
        (
            mock.call(
                args=[
                    ['kiali-ossm'],
                    'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5',
                    2,
                    'registry:8443/iib-build:11',
                    None,
                    None,
                    None,
                ],
                argsrepr=(
                    "[['kiali-ossm'], 'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5'"
                    ", 2, 'registry:8443/iib-build:11', None, None, None]"
                ),
                link_error=mock.ANY,
                queue=None,
            ),
        )
    )

    assert db.session.query(RequestAdd).filter_by(id=1).scalar()
    assert db.session.query(RequestRm).filter_by(id=2).scalar()
    assert len(rv.json) == 2
    assert all(r['batch_annotations'] == annotations for r in rv.json)

    requests_to_send_msgs_for = mock_smfnbor.call_args[0][0]
    assert len(requests_to_send_msgs_for) == 2
    assert requests_to_send_msgs_for[0].id == 1
    assert requests_to_send_msgs_for[1].id == 2


@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
def test_add_rm_batch_invalid_request_type(mock_hrbr, app, auth_env, client, db):
    data = {
        'build_requests': [
            {'from_bundle_image': 'registry.example.com/bundle-image:latest'},
            {
                'operators': ['kiali-ossm'],
                'binary_image': 'registry-proxy/rh-osbs/openshift-ose-operator-registry:v4.5',
                'from_index': 'registry:8443/iib-build:11',
            },
        ]
    }
    rv = client.post('/api/v1/builds/add-rm-batch', json=data, environ_base=auth_env)

    assert rv.status_code == 400, rv.json
    assert rv.json == {
        'error': (
            'Build request is not a valid Add/Rm request. This occurred on the build request '
            'in index 0.'
        )
    }
    mock_hrbr.apply_async.assert_not_called()


@pytest.mark.parametrize(
    'payload, error_msg',
    (
        (
            ['bundle:latest'],
            (
                'The input data must be a JSON object and the "build_requests" value must be a '
                'non-empty array'
            ),
        ),
        (
            {
                'build_requests': [
                    {
                        'operators': ['kiali-ossm'],
                        'binary_image': 'registry-proxy/openshift-ose-operator-registry:v4.5',
                        'from_index': 'registry:8443/iib-build:11',
                    }
                ],
                'annotations': 'Country music is good.',
            },
            'The value of "annotations" must be a JSON object',
        ),
    ),
)
def test_regenerate_add_rm_batch_invalid_input(payload, error_msg, app, auth_env, client, db):
    rv = client.post('/api/v1/builds/add-rm-batch', json=payload, environ_base=auth_env)

    assert rv.status_code == 400, rv.json
    assert rv.json == {'error': error_msg}


@mock.patch('iib.web.api_v1.handle_merge_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_merge_index_image_success(mock_smfsc, mock_merge, db, auth_env, client):
    data = {
        'deprecation_list': ['some@sha256:bundle'],
        'binary_image': 'binary:image',
        'source_from_index': 'source_index:image',
        'target_index': 'target_index:image',
    }

    response_json = {
        "arches": [],
        "batch": 1,
        "batch_annotations": None,
        "binary_image": "binary:image",
        "binary_image_resolved": None,
        "bundle_mapping": {},
        "bundles": [],
        "deprecation_list": ["some@sha256:bundle"],
        "from_index": None,
        "from_index_resolved": None,
        "id": 1,
        "index_image": None,
        'logs': {
            'url': 'http://localhost/api/v1/builds/1/logs',
            'expiration': '2020-02-15T17:03:00Z',
        },
        "organization": None,
        "removed_operators": [],
        "request_type": "merge-index-image",
        "source_from_index": "source_index:image",
        "state": "in_progress",
        "state_history": [
            {
                "state": "in_progress",
                "state_reason": "The request was initiated",
                "updated": "2020-02-12T17:03:00Z",
            }
        ],
        "state_reason": "The request was initiated",
        "target_index": "target_index:image",
        "updated": "2020-02-12T17:03:00Z",
        "user": 'tbrady@DOMAIN.LOCAL',
    }

    rv = client.post('/api/v1/builds/merge-index-image', json=data, environ_base=auth_env)
    rv_json = rv.json
    rv_json['state_history'][0]['updated'] = '2020-02-12T17:03:00Z'
    rv_json['updated'] = '2020-02-12T17:03:00Z'
    rv_json['logs']['expiration'] = '2020-02-15T17:03:00Z'
    mock_merge.apply_async.assert_called_once()
    assert rv.status_code == 201
    assert response_json == rv_json
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@pytest.mark.parametrize('force_overwrite', (False, True))
@mock.patch('iib.web.api_v1.handle_merge_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_merge_index_image_forced_overwrite(
    mock_smfsc, mock_merge, force_overwrite, app, auth_env, client, db
):
    app.config['IIB_FORCE_OVERWRITE_FROM_INDEX'] = force_overwrite
    data = {
        'deprecation_list': ['some@sha256:bundle'],
        'binary_image': 'binary:image',
        'source_from_index': 'source_index:image',
        'target_index': 'target_index:image',
        'overwrite_from_index': False,
    }

    rv = client.post('/api/v1/builds/merge-index-image', json=data, environ_base=auth_env)
    assert rv.status_code == 201
    mock_merge.apply_async.assert_called_once()
    # Second to last element in args is the overwrite_from_index parameter
    assert mock_merge.apply_async.call_args[1]['args'][-2] == force_overwrite
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)


@mock.patch('iib.web.api_v1.handle_merge_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_merge_index_image_overwrite_token_redacted(
    mock_smfsc, mock_merge, app, auth_env, client, db
):
    token = 'username:password'
    data = {
        'deprecation_list': ['some@sha256:bundle'],
        'binary_image': 'binary:image',
        'source_from_index': 'source_index:image',
        'target_index': 'target_index:image',
        'overwrite_from_index': True,
        'overwrite_from_index_token': token,
    }

    rv = client.post('/api/v1/builds/merge-index-image', json=data, environ_base=auth_env)
    rv_json = rv.json
    assert rv.status_code == 201
    mock_merge.apply_async.assert_called_once()
    # Second to last element in args is the overwrite_from_index parameter
    assert mock_merge.apply_async.call_args[1]['args'][-2] is True
    assert mock_merge.apply_async.call_args[1]['args'][-1] == token
    assert 'overwrite_from_index_token' not in rv_json
    assert token not in json.dumps(rv_json)
    assert token not in mock_merge.apply_async.call_args[1]['argsrepr']
    assert '*****' in mock_merge.apply_async.call_args[1]['argsrepr']


@pytest.mark.parametrize(
    'user_to_queue, overwrite_from_index, expected_queue',
    (
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, False, 'Buccaneers'),
        ({'tbrady@DOMAIN.LOCAL': 'Buccaneers'}, True, 'Buccaneers'),
        ({'PARALLEL:tbrady@DOMAIN.LOCAL': 'Buccaneers'}, False, 'Buccaneers'),
        ({'SERIAL:tbrady@DOMAIN.LOCAL': 'Buccaneers'}, True, 'Buccaneers'),
        (
            {'tbrady@DOMAIN.LOCAL': 'Patriots', 'PARALLEL:tbrady@DOMAIN.LOCAL': 'Buccaneers'},
            False,
            'Buccaneers',
        ),
        (
            {'tbrady@DOMAIN.LOCAL': 'Patriots', 'SERIAL:tbrady@DOMAIN.LOCAL': 'Buccaneers'},
            True,
            'Buccaneers',
        ),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, False, None),
        ({'not.tbrady@DOMAIN.LOCAL': 'Patriots'}, True, None),
    ),
)
@mock.patch('iib.web.api_v1.handle_merge_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_merge_index_image_custom_user_queue(
    mock_smfsc,
    mock_merge,
    app,
    auth_env,
    client,
    user_to_queue,
    overwrite_from_index,
    expected_queue,
):
    app.config['IIB_USER_TO_QUEUE'] = user_to_queue
    data = {
        'deprecation_list': ['some@sha256:bundle'],
        'binary_image': 'binary:image',
        'source_from_index': 'source_index:image',
        'target_index': 'target_index:image',
    }
    if overwrite_from_index:
        data['overwrite_from_index'] = True

    rv = client.post('/api/v1/builds/merge-index-image', json=data, environ_base=auth_env)
    assert rv.status_code == 201, rv.json
    mock_merge.apply_async.assert_called_once()
    mock_merge.apply_async.assert_called_with(
        args=mock.ANY, argsrepr=mock.ANY, link_error=mock.ANY, queue=expected_queue
    )
    mock_smfsc.assert_called_once_with(mock.ANY, new_batch_msg=True)
