# SPDX-License-Identifier: GPL-3.0-or-later
import json
from unittest import mock

import proton
import pytest

from iib.web import messaging


@pytest.mark.parametrize(
    'request_state, new_batch, envelope_expected',
    (
        ('in_progress', False, False),
        ('complete', False, True),
        ('failed', False, True),
        ('in_progress', True, True),
    ),
)
def test_get_batch_state_change_envelope(
    request_state,
    new_batch,
    envelope_expected,
    app,
    db,
    minimal_request_add,
    minimal_request_rm,
    minimal_request_regenerate_bundle,
):
    minimal_request_add.add_state(request_state, 'For some reason')
    minimal_request_rm.add_state('complete', 'For some other reason')
    minimal_request_regenerate_bundle.add_state('complete', 'For some other reason')
    minimal_request_add.organization = 'mos-eisley-marketplace'
    minimal_request_regenerate_bundle.organization = 'dagobah'
    db.session.add(minimal_request_add)
    batch = minimal_request_add.batch
    annotations = {'Yoda': 'Do or do not. There is no try.'}
    batch.annotations = annotations
    # Put all three requests in the same batch
    minimal_request_rm.batch = batch
    minimal_request_regenerate_bundle.batch = batch
    db.session.commit()

    envelope = messaging._get_batch_state_change_envelope(batch, new_batch=new_batch)

    if envelope_expected:
        assert envelope
        assert envelope.address == 'topic://VirtualTopic.eng.iib.batch.state'
        # Since there is only a single request in the batch, the request states dictate's the
        # batch's state
        assert envelope.message.properties == {
            'batch': 1,
            'state': request_state,
            'user': None,
        }
        assert json.loads(envelope.message.body) == {
            'annotations': annotations,
            'batch': 1,
            'request_ids': [1, 2, 3],
            'requests': [
                {'id': 1, 'organization': 'mos-eisley-marketplace', 'type': 'add'},
                {'id': 2, 'organization': None, 'type': 'rm'},
                {'id': 3, 'organization': 'dagobah', 'type': 'regenerate-bundle'},
            ],
            'state': request_state,
            'user': None,
        }
    else:
        assert envelope is None


def test_get_batch_state_change_envelope_missing_config(app, db, minimal_request_add):
    minimal_request_add.add_state('complete', 'For some reason')
    db.session.add(minimal_request_add)
    db.session.commit()
    app.config.pop('IIB_MESSAGING_BATCH_STATE_DESTINATION')

    batch = minimal_request_add.batch
    assert messaging._get_batch_state_change_envelope(batch) is None


def test_get_request_state_change_envelope(app, db, minimal_request_add):
    minimal_request_add.add_state('complete', 'For some reason')
    db.session.add(minimal_request_add)
    db.session.commit()

    envelope = messaging._get_request_state_change_envelope(minimal_request_add)

    assert envelope
    assert envelope.address == 'topic://VirtualTopic.eng.iib.build.state'
    assert envelope.message.properties == {
        'batch': 1,
        'id': 1,
        'state': 'complete',
        'user': None,
    }
    assert json.loads(envelope.message.body) == minimal_request_add.to_json(verbose=False)


def test_get_request_state_change_envelope_missing_config(app, db, minimal_request_add):
    minimal_request_add.add_state('complete', 'For some reason')
    db.session.add(minimal_request_add)
    db.session.commit()
    app.config.pop('IIB_MESSAGING_BUILD_STATE_DESTINATION')

    assert messaging._get_request_state_change_envelope(minimal_request_add) is None


@mock.patch('iib.web.messaging.os.path.exists')
@mock.patch('iib.web.messaging.proton.SSLDomain')
def test_get_ssl_domain(mock_ssldomain, mock_exists, app):
    mock_exists.return_value = True

    ssl_domain = messaging._get_ssl_domain()

    assert ssl_domain is mock_ssldomain.return_value
    assert mock_exists.call_count == 3
    mock_ssldomain.return_value.set_credentials.assert_called_once_with(
        '/etc/iib/messaging.crt', '/etc/iib/messaging.key', None
    )
    mock_ssldomain.return_value.set_trusted_ca_db.assert_called_once_with(
        '/etc/iib/messaging-ca.crt'
    )
    mock_ssldomain.return_value.set_peer_authentication.assert_called_once_with(
        proton.SSLDomain.VERIFY_PEER
    )


@pytest.mark.parametrize('cert_exists', (True, False))
@pytest.mark.parametrize('key_exists', (True, False))
@pytest.mark.parametrize('ca_exists', (True, False))
@mock.patch('iib.web.messaging.os.path.exists')
def test_get_ssl_domain_files_missing(mock_exists, cert_exists, key_exists, ca_exists, app):
    if mock_exists and cert_exists and key_exists:
        # Skip the case when all are set
        return

    mock_exists.side_effect = [cert_exists, key_exists, ca_exists]

    ssl_domain = messaging._get_ssl_domain()

    assert ssl_domain is None


@pytest.mark.parametrize(
    'missing_key', ('IIB_MESSAGING_CERT', 'IIB_MESSAGING_KEY', 'IIB_MESSAGING_CA')
)
@mock.patch('iib.web.messaging.os.path.exists')
@mock.patch('iib.web.messaging.current_app')
def test_get_ssl_domain_cert_config_not_set(mock_current_app, mock_exists, missing_key):
    mock_exists.return_value = True
    mock_current_app.config = {
        'IIB_MESSAGING_CERT': '/etc/iib/messaging.crt',
        'IIB_MESSAGING_KEY': '/etc/iib/messaging.key',
        'IIB_MESSAGING_CA': '/etc/iib/messaging.ca',
    }
    mock_current_app.config.pop(missing_key)

    ssl_domain = messaging._get_ssl_domain()

    assert ssl_domain is None


@pytest.mark.parametrize('durable', (True, False))
@mock.patch('iib.web.messaging.current_app')
def test_json_to_envelope(mock_current_app, durable):
    mock_current_app.config = {'IIB_MESSAGING_DURABLE': durable}

    address = 'topic://VirtualTopic.eng.iib.build.state'
    content = {'han': 'solo'}
    envelope = messaging.json_to_envelope(address, content)

    assert envelope.address == address
    # Verify that the ID is a UUID
    assert len(envelope.message.id) == 36
    assert envelope.message.body == '{"han": "solo"}'
    assert envelope.message.content_encoding == 'utf-8'
    assert envelope.message.content_type == 'application/json'
    assert envelope.message.durable is durable


@mock.patch('iib.web.messaging.BlockingConnection')
@mock.patch('iib.web.messaging._get_ssl_domain')
def test_send_messages(mock_gsd, mock_bc, app):
    mock_sender_one = mock.Mock()
    mock_sender_two = mock.Mock()
    mock_connection = mock.Mock()
    mock_connection.create_sender.side_effect = [mock_sender_one, mock_sender_two]
    mock_bc.return_value = mock_connection

    msg_one = proton.Message('{"han": "solo"}')
    msg_two = proton.Message('{"star": "wars"}')
    envelopes = [
        messaging.Envelope('topic://VirtualTopic.eng.star_wars', msg_one),
        messaging.Envelope('topic://VirtualTopic.eng.star_wars2', msg_one),
        messaging.Envelope('topic://VirtualTopic.eng.star_wars', msg_two),
    ]
    messaging.send_messages(envelopes)

    mock_bc.assert_called_once_with(
        urls=['amqps://message-broker:5671'], timeout=30, ssl_domain=mock_gsd.return_value
    )
    # Verify that even though three messages were sent, only two senders were created since only
    # two unique addresses were used
    assert mock_connection.create_sender.call_count == 2
    # Ensure the order is respected
    mock_connection.create_sender.assert_has_calls(
        (
            mock.call('topic://VirtualTopic.eng.star_wars'),
            mock.call('topic://VirtualTopic.eng.star_wars2'),
        )
    )
    assert mock_sender_one.send.call_count == 2
    mock_sender_one.send.assert_has_calls(
        (mock.call(msg_one, timeout=30), mock.call(msg_two, timeout=30))
    )
    mock_sender_two.send.assert_called_once_with(msg_one, timeout=30)
    mock_connection.close.assert_called_once_with()


@mock.patch('iib.web.messaging.BlockingConnection')
@mock.patch('iib.web.messaging._get_ssl_domain')
def test_send_messages_nonfatal(mock_gsd, mock_bc, app):
    mock_bc.side_effect = proton.Timeout

    # Verfies that an infrastructure issue is a nonfatal error. If this raises an exception,
    # the test will fail.
    messaging.send_messages(
        [messaging.Envelope('topic://VirtualTopic.eng.star_wars', '{"han": "solo"}')]
    )


@pytest.mark.parametrize('request_msg_expected', (True, False))
@pytest.mark.parametrize('batch_msg_expected', (True, False))
@mock.patch('iib.web.messaging._get_request_state_change_envelope')
@mock.patch('iib.web.messaging._get_batch_state_change_envelope')
@mock.patch('iib.web.messaging.send_messages')
def test_send_message_for_state_change(
    mock_sm,
    mock_gbsce,
    mock_grstce,
    batch_msg_expected,
    request_msg_expected,
    app,
    db,
    minimal_request_add,
):
    expected_msgs = []
    if request_msg_expected:
        request_envelope = mock.Mock()
        expected_msgs.append(request_envelope)
        mock_grstce.return_value = request_envelope
    else:
        mock_grstce.return_value = None

    if batch_msg_expected:
        batch_envelope = mock.Mock()
        expected_msgs.append(batch_envelope)
        mock_gbsce.return_value = batch_envelope
    else:
        mock_gbsce.return_value = None

    messaging.send_message_for_state_change(minimal_request_add)

    if expected_msgs:
        mock_sm.assert_called_once_with(expected_msgs)
    else:
        mock_sm.assert_not_called()


@pytest.mark.parametrize('request_msg_expected', (True, False))
@pytest.mark.parametrize('batch_msg_expected', (True, False))
@mock.patch('iib.web.messaging._get_request_state_change_envelope')
@mock.patch('iib.web.messaging._get_batch_state_change_envelope')
@mock.patch('iib.web.messaging.send_messages')
def test_send_messages_for_new_batch_of_requests(
    mock_sm, mock_gbsce, mock_grsce, batch_msg_expected, request_msg_expected, minimal_request_add
):
    expected_msgs = []
    if request_msg_expected:
        request_envelope1 = mock.Mock()
        request_envelope2 = mock.Mock()
        expected_msgs.extend([request_envelope1, request_envelope2])
        mock_grsce.side_effect = [request_envelope1, request_envelope2]
    else:
        mock_grsce.return_value = None

    if batch_msg_expected:
        batch_envelope = mock.Mock()
        expected_msgs.append(batch_envelope)
        mock_gbsce.return_value = batch_envelope
    else:
        mock_gbsce.return_value = None

    requests = [minimal_request_add, minimal_request_add]
    messaging.send_messages_for_new_batch_of_requests(requests)

    if expected_msgs:
        mock_sm.assert_called_once_with(expected_msgs)
    else:
        mock_sm.assert_not_called()


@mock.patch('iib.web.messaging.send_messages')
def test_send_messages_for_new_batch_of_requests_no_requests(mock_sm, minimal_request_add):
    messaging.send_messages_for_new_batch_of_requests([])

    mock_sm.assert_not_called()
