from unittest import mock

from kombu.exceptions import OperationalError

from iib.web.models import Request, RequestStateMapping, RequestState, RequestAdd, RequestRm


def assert_testing(rv, mock_smfsc, db):
    response_json = {'error': 'The scheduling of the build request with ID 1 failed'}
    assert rv.status_code == 500
    assert response_json == rv.json
    assert mock_smfsc.call_count == 2

    req_state = db.session.query(Request).get(1)
    assert req_state.state.state == RequestStateMapping.failed.value


@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_catch_add_bundle_failure(mock_smfsc, mock_har, db, auth_env, client):
    mock_har.apply_async.side_effect = OperationalError
    data = {
        'bundles': ['some:thing'],
        'binary_image': 'binary:image',
        'add_arches': ['s390x'],
        'organization': 'org',
        'cnr_token': 'token',
        'overwrite_from_index': True,
    }

    rv = client.post('/api/v1/builds/add', json=data, environ_base=auth_env)
    mock_har.apply_async.assert_called_once()
    assert_testing(rv, mock_smfsc, db)


@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_catch_regenerate_bundle_failure(mock_smfsc, mock_hrbr, db, auth_env, client):
    mock_hrbr.apply_async.side_effect = OperationalError

    data = {
        'from_bundle_image': 'registry.example.com/bundle-image:latest',
    }

    rv = client.post('/api/v1/builds/regenerate-bundle', json=data, environ_base=auth_env)
    mock_hrbr.apply_async.assert_called_once()
    assert_testing(rv, mock_smfsc, db)


@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
def test_catch_remove_operator_failure(mock_smfsc, mock_rm, db, auth_env, client):
    mock_rm.apply_async.side_effect = OperationalError
    data = {
        'operators': ['some:thing'],
        'binary_image': 'binary:image',
        'from_index': 'index:image',
    }

    rv = client.post('/api/v1/builds/rm', json=data, environ_base=auth_env)
    mock_rm.apply_async.assert_called_once()
    assert_testing(rv, mock_smfsc, db)


@mock.patch('iib.web.api_v1.handle_regenerate_bundle_request')
@mock.patch('iib.web.api_v1.messaging.send_message_for_state_change')
@mock.patch('iib.web.api_v1.messaging.send_messages_for_new_batch_of_requests')
def test_catch_regenerate_bundle_batch_failure(
    mock_smfnbor, mock_smfsc, mock_hrbr, app, auth_env, client, db,
):
    mock_hrbr.apply_async.side_effect = OperationalError

    data = {
        'build_requests': [
            {'from_bundle_image': 'registry.example.com/bundle-image:latest'},
            {'from_bundle_image': 'registry.example.com/bundle-image2:latest'},
            {'from_bundle_image': 'registry.example.com/bundle-image3:latest'},
        ]
    }
    rv = client.post('/api/v1/builds/regenerate-bundle-batch', json=data, environ_base=auth_env)

    response_json = {'error': 'The scheduling of the build requests with IDs 1, 2, 3 failed'}
    assert rv.status_code == 500
    assert rv.json == response_json
    assert mock_smfsc.call_count == 3
    assert mock_hrbr.apply_async.call_count == 1

    requests_to_send_msgs_for = mock_smfnbor.call_args[0][0]
    assert len(requests_to_send_msgs_for) == 3
    assert requests_to_send_msgs_for[0].id == 1
    assert requests_to_send_msgs_for[1].id == 2

    req_states = (
        db.session.query(RequestState)
        .join(Request, Request.request_state_id == RequestState.id)
        .filter(Request.id.in_((1, 2, 3)))
        .all()
    )
    assert len(req_states) == 3
    for r in req_states:
        assert r.state == RequestStateMapping.failed.value


@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_messages_for_new_batch_of_requests')
def test_add_rm_batch_add_failure(mock_smfnbor, mock_hrr, mock_har, app, auth_env, client, db):
    mock_har.apply_async.side_effect = OperationalError

    annotations = {'msdhoni': 'What? Who?'}
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

    assert rv.status_code == 500, rv.json
    assert mock_hrr.apply_async.call_count == 0
    assert mock_har.apply_async.call_count == 1

    assert db.session.query(RequestAdd).get(1)
    assert db.session.query(RequestRm).get(2)

    response_json = {'error': 'The scheduling of the build requests with IDs 1, 2 failed'}
    assert rv.json == response_json

    requests_to_send_msgs_for = mock_smfnbor.call_args[0][0]
    assert len(requests_to_send_msgs_for) == 2
    assert requests_to_send_msgs_for[0].id == 1
    assert requests_to_send_msgs_for[1].id == 2

    req_add = db.session.query(RequestAdd).get(1)
    req_rm = db.session.query(RequestRm).get(2)
    assert req_add.state.state == RequestStateMapping.failed.value
    assert req_rm.state.state == RequestStateMapping.failed.value


@mock.patch('iib.web.api_v1.handle_add_request')
@mock.patch('iib.web.api_v1.handle_rm_request')
@mock.patch('iib.web.api_v1.messaging.send_messages_for_new_batch_of_requests')
def test_add_rm_batch_rm_failure(mock_smfnbor, mock_hrr, mock_har, app, auth_env, client, db):
    mock_hrr.apply_async.side_effect = OperationalError

    annotations = {'msdhoni': 'Who is that guy?'}
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

    assert rv.status_code == 500, rv.json
    assert mock_hrr.apply_async.call_count == 1
    assert mock_har.apply_async.call_count == 1

    response_json = {'error': 'The scheduling of the build requests with IDs 2 failed'}
    assert rv.json == response_json

    requests_to_send_msgs_for = mock_smfnbor.call_args[0][0]
    assert len(requests_to_send_msgs_for) == 2
    assert requests_to_send_msgs_for[0].id == 1
    assert requests_to_send_msgs_for[1].id == 2

    req_add = db.session.query(RequestAdd).get(1)
    req_rm = db.session.query(RequestRm).get(2)
    # First request is processed because we are testing failing on RequestRM
    assert req_add.state.state == RequestStateMapping.in_progress.value
    assert req_rm.state.state == RequestStateMapping.failed.value
