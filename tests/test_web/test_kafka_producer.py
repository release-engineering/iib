# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest
from kafka.errors import KafkaError

from iib.web import kafka_producer


@pytest.fixture(autouse=True)
def reset_kafka_producer():
    """Reset the module-level cached producer before each test."""
    kafka_producer._kafka_producer = None
    yield
    kafka_producer._kafka_producer = None


class TestGetKafkaProducer:
    """Tests for the get_kafka_producer function."""

    @mock.patch('iib.web.kafka_producer.KafkaProducer')
    def test_get_kafka_producer_success(self, mock_kp_class, app):
        """Verify the producer is created with correct config values."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker1:9096', 'broker2:9096']
        app.config['IIB_KAFKA_SECURITY_PROTOCOL'] = 'SASL_SSL'
        app.config['IIB_KAFKA_SASL_MECHANISM'] = 'SCRAM-SHA-512'
        app.config['IIB_KAFKA_USERNAME'] = 'test_user'
        app.config['IIB_KAFKA_PASSWORD'] = 'test_pass'

        producer = kafka_producer.get_kafka_producer()

        assert producer is mock_kp_class.return_value
        mock_kp_class.assert_called_once()
        call_kwargs = mock_kp_class.call_args[1]
        assert call_kwargs['bootstrap_servers'] == ['broker1:9096', 'broker2:9096']
        assert call_kwargs['security_protocol'] == 'SASL_SSL'
        assert call_kwargs['sasl_mechanism'] == 'SCRAM-SHA-512'
        assert call_kwargs['sasl_plain_username'] == 'test_user'
        assert call_kwargs['sasl_plain_password'] == 'test_pass'

    @mock.patch('iib.web.kafka_producer.KafkaProducer')
    def test_get_kafka_producer_reads_config_values(self, mock_kp_class, app):
        """Verify the producer reads security_protocol and sasl_mechanism from config."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_SECURITY_PROTOCOL'] = 'PLAINTEXT'
        app.config['IIB_KAFKA_SASL_MECHANISM'] = 'PLAIN'
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'

        kafka_producer.get_kafka_producer()

        call_kwargs = mock_kp_class.call_args[1]
        assert call_kwargs['security_protocol'] == 'PLAINTEXT'
        assert call_kwargs['sasl_mechanism'] == 'PLAIN'

    def test_get_kafka_producer_no_brokers(self, app):
        """Verify None is returned when brokers is None."""
        app.config['IIB_KAFKA_BROKERS'] = None

        producer = kafka_producer.get_kafka_producer()

        assert producer is None

    def test_get_kafka_producer_empty_brokers(self, app):
        """Verify None is returned when brokers list is empty."""
        app.config['IIB_KAFKA_BROKERS'] = []

        producer = kafka_producer.get_kafka_producer()

        assert producer is None

    @mock.patch('iib.web.kafka_producer.KafkaProducer')
    def test_get_kafka_producer_cached(self, mock_kp_class, app):
        """Verify a second call returns the cached producer without creating a new one."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'

        producer1 = kafka_producer.get_kafka_producer()
        producer2 = kafka_producer.get_kafka_producer()

        assert producer1 is producer2
        mock_kp_class.assert_called_once()

    @mock.patch('iib.web.kafka_producer.KafkaProducer')
    def test_get_kafka_producer_init_failure(self, mock_kp_class, app):
        """Verify None is returned when KafkaProducer init raises an exception."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'
        mock_kp_class.side_effect = KafkaError('connection failed')

        producer = kafka_producer.get_kafka_producer()

        assert producer is None


class TestSendKafkaMessage:
    """Tests for the send_kafka_message function."""

    def test_send_kafka_message_success(self, app):
        """Verify message is sent with correct topic, key, value, and headers."""
        mock_producer = mock.Mock()
        mock_future = mock.Mock()
        mock_producer.send.return_value = mock_future

        content = {'id': 1, 'state': 'complete', 'batch': 1}
        properties = {'id': 1, 'state': 'complete', 'user': 'tbrady'}

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.build.state', content, properties
        )

        mock_producer.send.assert_called_once_with(
            'dev.eng.iib.build.state',
            key=1,
            value=content,
            headers=[
                ('id', b'1'),
                ('state', b'complete'),
                ('user', b'tbrady'),
            ],
        )
        mock_future.add_errback.assert_called_once_with(kafka_producer.on_send_error)

    def test_send_kafka_message_batch_key(self, app):
        """When no 'id' in properties, the message key should fall back to 'batch'."""
        mock_producer = mock.Mock()
        mock_future = mock.Mock()
        mock_producer.send.return_value = mock_future

        content = {'batch': 5, 'state': 'in_progress'}
        properties = {'batch': 5, 'state': 'in_progress', 'user': None}

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.batch.state', content, properties
        )

        call_kwargs = mock_producer.send.call_args[1]
        assert call_kwargs['key'] == 5

    def test_send_kafka_message_no_properties(self, app):
        """Verify message works with no properties passed."""
        mock_producer = mock.Mock()
        mock_future = mock.Mock()
        mock_producer.send.return_value = mock_future

        kafka_producer.send_kafka_message(mock_producer, 'dev.eng.iib.build.state', {'id': 1})

        call_kwargs = mock_producer.send.call_args[1]
        assert call_kwargs['headers'] == []
        assert call_kwargs['key'] is None

    def test_send_kafka_message_kafka_error(self, app):
        """Verify KafkaError is caught and does not raise."""
        mock_producer = mock.Mock()
        mock_producer.send.side_effect = KafkaError('send failed')

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.build.state', {'id': 1}, {'id': 1}
        )

        # Should not raise — error is logged

    def test_on_send_error_logs(self):
        """Verify on_send_error logs the exception without raising."""
        exc = KafkaError('async delivery failed')
        with mock.patch('logging.getLogger') as mock_get_logger:
            kafka_producer.on_send_error(exc)

        mock_get_logger.return_value.error.assert_called_once()


class TestSendKafkaMessages:
    """Tests for the _send_kafka_messages function in messaging.py."""

    @mock.patch('iib.web.messaging.send_kafka_message')
    @mock.patch('iib.web.messaging.get_kafka_producer')
    def test_send_kafka_messages_no_producer(
        self, mock_get_producer, mock_send_msg, app, db, minimal_request_add
    ):
        """When the producer is None, no messages should be sent."""
        mock_get_producer.return_value = None

        from iib.web.messaging import _send_kafka_messages

        _send_kafka_messages([minimal_request_add], minimal_request_add.batch)

        mock_send_msg.assert_not_called()

    @mock.patch('iib.web.messaging.send_kafka_message')
    @mock.patch('iib.web.messaging.get_kafka_producer')
    def test_send_kafka_messages_build_and_batch(
        self, mock_get_producer, mock_send_msg, app, db, minimal_request_add
    ):
        """Verify both build and batch messages are sent on new batch."""
        mock_producer = mock.Mock()
        mock_get_producer.return_value = mock_producer
        app.config['IIB_KAFKA_BUILD_STATE_TOPIC'] = 'dev.eng.iib.build.state'
        app.config['IIB_KAFKA_BATCH_STATE_TOPIC'] = 'dev.eng.iib.batch.state'

        minimal_request_add.add_state('in_progress', 'Starting')
        db.session.commit()

        from iib.web.messaging import _send_kafka_messages

        _send_kafka_messages([minimal_request_add], minimal_request_add.batch, new_batch=True)

        assert mock_send_msg.call_count == 2
        build_call = mock_send_msg.call_args_list[0]
        assert build_call[0][0] is mock_producer
        assert build_call[0][1] == 'dev.eng.iib.build.state'

        batch_call = mock_send_msg.call_args_list[1]
        assert batch_call[0][0] is mock_producer
        assert batch_call[0][1] == 'dev.eng.iib.batch.state'

    @mock.patch('iib.web.messaging.send_kafka_message')
    @mock.patch('iib.web.messaging.get_kafka_producer')
    def test_send_kafka_messages_no_topics(
        self, mock_get_producer, mock_send_msg, app, db, minimal_request_add
    ):
        """When topics are not configured, no messages should be sent."""
        mock_get_producer.return_value = mock.Mock()
        app.config.pop('IIB_KAFKA_BUILD_STATE_TOPIC', None)
        app.config.pop('IIB_KAFKA_BATCH_STATE_TOPIC', None)

        from iib.web.messaging import _send_kafka_messages

        _send_kafka_messages([minimal_request_add], minimal_request_add.batch)

        mock_send_msg.assert_not_called()

    @mock.patch('iib.web.messaging.send_kafka_message')
    @mock.patch('iib.web.messaging.get_kafka_producer')
    def test_send_kafka_messages_batch_not_final(
        self, mock_get_producer, mock_send_msg, app, db, minimal_request_add
    ):
        """Batch message should not be sent when batch is in_progress and new_batch is False."""
        mock_get_producer.return_value = mock.Mock()
        app.config['IIB_KAFKA_BUILD_STATE_TOPIC'] = 'dev.eng.iib.build.state'
        app.config['IIB_KAFKA_BATCH_STATE_TOPIC'] = 'dev.eng.iib.batch.state'

        minimal_request_add.add_state('in_progress', 'Starting')
        db.session.commit()

        from iib.web.messaging import _send_kafka_messages

        _send_kafka_messages([minimal_request_add], minimal_request_add.batch, new_batch=False)

        # Only build message, no batch message (batch is in_progress, not final)
        assert mock_send_msg.call_count == 1
        assert mock_send_msg.call_args_list[0][0][1] == 'dev.eng.iib.build.state'


class TestDualPublishIntegration:
    """
    Verify dual-publish calls _send_kafka_messages alongside AMQP.

    Covers send_message_for_state_change and send_messages_for_new_batch_of_requests.
    """

    @mock.patch('iib.web.messaging._send_kafka_messages')
    @mock.patch('iib.web.messaging.send_messages')
    @mock.patch('iib.web.messaging._get_batch_state_change_envelope')
    @mock.patch('iib.web.messaging._get_request_state_change_envelope')
    def test_send_message_for_state_change_calls_kafka(
        self,
        mock_req_env,
        mock_batch_env,
        mock_send_amqp,
        mock_send_kafka,
        app,
        db,
        minimal_request_add,
    ):
        """Verify send_message_for_state_change invokes _send_kafka_messages."""
        mock_req_env.return_value = mock.Mock()
        mock_batch_env.return_value = None

        from iib.web.messaging import send_message_for_state_change

        send_message_for_state_change(minimal_request_add, new_batch_msg=False)

        mock_send_kafka.assert_called_once_with(
            [minimal_request_add], minimal_request_add.batch, new_batch=False
        )

    @mock.patch('iib.web.messaging._send_kafka_messages')
    @mock.patch('iib.web.messaging.send_messages')
    @mock.patch('iib.web.messaging._get_batch_state_change_envelope')
    @mock.patch('iib.web.messaging._get_request_state_change_envelope')
    def test_send_messages_for_new_batch_calls_kafka(
        self,
        mock_req_env,
        mock_batch_env,
        mock_send_amqp,
        mock_send_kafka,
        app,
        db,
        minimal_request_add,
    ):
        """Verify send_messages_for_new_batch_of_requests invokes _send_kafka_messages."""
        mock_req_env.return_value = mock.Mock()
        mock_batch_env.return_value = None

        from iib.web.messaging import send_messages_for_new_batch_of_requests

        send_messages_for_new_batch_of_requests([minimal_request_add])

        mock_send_kafka.assert_called_once_with(
            [minimal_request_add], minimal_request_add.batch, new_batch=True
        )
