# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import proton
import pytest

from iib.web import messaging


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


@pytest.mark.parametrize(
    'missing_config, request_state, new_batch_msg, request_expected, batch_expected',
    (
        (None, 'complete', True, True, True),
        (None, 'in_progress', True, True, True),
        (None, 'complete', False, True, True),
        (None, 'in_progress', False, True, False),
        ('IIB_MESSAGING_BUILD_STATE_DESTINATION', 'complete', True, False, True),
        ('IIB_MESSAGING_BUILD_STATE_DESTINATION', 'in_progress', True, False, True),
        ('IIB_MESSAGING_BUILD_STATE_DESTINATION', 'complete', False, False, True),
        ('IIB_MESSAGING_BUILD_STATE_DESTINATION', 'in_progress', False, False, False),
        ('IIB_MESSAGING_BATCH_STATE_DESTINATION', 'complete', True, True, False),
        ('IIB_MESSAGING_BATCH_STATE_DESTINATION', 'in_progress', True, True, False),
        ('IIB_MESSAGING_BATCH_STATE_DESTINATION', 'complete', False, True, False),
        ('IIB_MESSAGING_BATCH_STATE_DESTINATION', 'in_progress', False, True, False),
    ),
)
@mock.patch('iib.web.messaging.send_messages')
def test_send_message_for_state_change(
    mock_sm,
    missing_config,
    request_state,
    new_batch_msg,
    request_expected,
    batch_expected,
    app,
    db,
    minimal_request_add,
):
    minimal_request_add.add_state(request_state, 'For some reason')
    db.session.add(minimal_request_add)
    db.session.commit()
    if missing_config:
        app.config.pop(missing_config)

    messaging.send_message_for_state_change(minimal_request_add, new_batch_msg)

    expected_envelopes = 2
    if not request_expected:
        expected_envelopes -= 1
    if not batch_expected:
        expected_envelopes -= 1

    if expected_envelopes:
        envelopes = mock_sm.call_args[0][0]
        assert len(envelopes) == expected_envelopes
        if request_expected:
            assert any(e.address == 'topic://VirtualTopic.eng.iib.build.state' for e in envelopes)
        if batch_expected:
            assert any(e.address == 'topic://VirtualTopic.eng.iib.batch.state' for e in envelopes)
    else:
        mock_sm.assert_not_called()
