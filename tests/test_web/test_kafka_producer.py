# SPDX-License-Identifier: GPL-3.0-or-later
import logging
from unittest import mock

import pytest
from confluent_kafka import KafkaException

from iib.web import kafka_producer


@pytest.fixture(autouse=True)
def reset_kafka_producer():
    """Reset the module-level cached producer before each test."""
    kafka_producer._kafka_producer = None
    yield
    kafka_producer._kafka_producer = None


class TestGetKafkaProducer:
    """Tests for the get_kafka_producer function."""

    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_success(self, mock_producer_class, app):
        """Verify the producer is created with correct config values."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker1:9096', 'broker2:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'test_user'
        app.config['IIB_KAFKA_PASSWORD'] = 'test_pass'

        producer = kafka_producer.get_kafka_producer()

        assert producer is mock_producer_class.return_value
        mock_producer_class.assert_called_once()
        call_config = mock_producer_class.call_args[0][0]
        assert call_config['bootstrap.servers'] == 'broker1:9096,broker2:9096'
        assert call_config['security.protocol'] == 'SASL_SSL'
        assert call_config['sasl.mechanism'] == 'SCRAM-SHA-512'
        assert call_config['sasl.username'] == 'test_user'
        assert call_config['sasl.password'] == 'test_pass'
        assert call_config['linger.ms'] == 5

    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_ssl_cafile_default(self, mock_producer_class, app):
        """Verify the default ssl_cafile from config is passed to the producer."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'

        kafka_producer.get_kafka_producer()

        call_config = mock_producer_class.call_args[0][0]
        assert call_config['ssl.ca.location'] == '/etc/pki/tls/certs/ca-bundle.crt'

    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_ssl_cafile_custom(self, mock_producer_class, app):
        """Verify a custom ssl_cafile is passed when configured."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'
        app.config['IIB_KAFKA_SSL_CAFILE'] = '/custom/ca-bundle.crt'

        kafka_producer.get_kafka_producer()

        call_config = mock_producer_class.call_args[0][0]
        assert call_config['ssl.ca.location'] == '/custom/ca-bundle.crt'

    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_ssl_cafile_omitted_when_falsy(
        self, mock_producer_class, app, caplog
    ):
        """Verify ssl.ca.location is omitted and a warning logged when explicitly unset."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'
        app.config['IIB_KAFKA_SSL_CAFILE'] = None

        # Setting the logging level via caplog.set_level is not sufficient. The flask
        # related settings from previous tests interfere with this.
        app.logger.disabled = False
        app.logger.setLevel(logging.DEBUG)

        kafka_producer.get_kafka_producer()

        call_config = mock_producer_class.call_args[0][0]
        assert 'ssl.ca.location' not in call_config
        assert 'ssl.ca.location will be omitted and confluent-kafka will use OpenSSL' in caplog.text

    @pytest.mark.parametrize(
        'brokers,username,password',
        [
            (None, 'user', 'pass'),
            ([], 'user', 'pass'),
            (['broker:9096'], None, 'pass'),
            (['broker:9096'], 'user', None),
            (['broker:9096'], None, None),
        ],
    )
    def test_get_kafka_producer_skips_when_any_config_missing(
        self, app, caplog, brokers, username, password
    ):
        """Verify None is returned and a debug log is emitted when any required config is absent."""
        app.config['IIB_KAFKA_BROKERS'] = brokers
        app.config['IIB_KAFKA_USERNAME'] = username
        app.config['IIB_KAFKA_PASSWORD'] = password
        app.logger.disabled = False
        app.logger.setLevel(logging.DEBUG)

        producer = kafka_producer.get_kafka_producer()

        assert producer is None
        assert 'Kafka messaging is disabled: missing config:' in caplog.text

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

    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_cached(self, mock_producer_class, app):
        """Verify a second call returns the cached producer without creating a new one."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'

        producer1 = kafka_producer.get_kafka_producer()
        producer2 = kafka_producer.get_kafka_producer()

        assert producer1 is producer2
        mock_producer_class.assert_called_once()

    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_init_failure(self, mock_producer_class, app):
        """Verify None is returned when Producer init raises an exception."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'
        mock_producer_class.side_effect = KafkaException('connection failed')

        producer = kafka_producer.get_kafka_producer()

        assert producer is None

    @mock.patch('iib.web.kafka_producer.atexit')
    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_registers_atexit_cleanup(
        self, mock_producer_class, mock_atexit, app
    ):
        """Verify a shutdown cleanup hook is registered when the producer is created."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'

        kafka_producer.get_kafka_producer()

        mock_atexit.register.assert_called_once_with(kafka_producer._close_kafka_producer)

    @mock.patch('iib.web.kafka_producer.atexit')
    @mock.patch('iib.web.kafka_producer.Producer')
    def test_get_kafka_producer_cached_does_not_reregister_atexit(
        self, mock_producer_class, mock_atexit, app
    ):
        """Verify the cleanup hook is registered only once, not on every cached call."""
        app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
        app.config['IIB_KAFKA_USERNAME'] = 'u'
        app.config['IIB_KAFKA_PASSWORD'] = 'p'

        kafka_producer.get_kafka_producer()
        kafka_producer.get_kafka_producer()

        mock_atexit.register.assert_called_once()


class TestCloseKafkaProducer:
    """Tests for the _close_kafka_producer shutdown hook."""

    def test_close_kafka_producer_flushes_existing_producer(self):
        """Verify the cached producer is flushed with a timeout on shutdown."""
        mock_producer = mock.Mock()
        kafka_producer._kafka_producer = mock_producer

        kafka_producer._close_kafka_producer()

        mock_producer.flush.assert_called_once_with(timeout=5)

    def test_close_kafka_producer_noop_when_not_initialized(self):
        """Verify nothing happens when no producer was ever created."""
        kafka_producer._kafka_producer = None

        # Should not raise
        kafka_producer._close_kafka_producer()

    def test_close_kafka_producer_swallows_errors(self):
        """Verify exceptions raised while flushing are caught and logged, not propagated."""
        mock_producer = mock.Mock()
        mock_producer.flush.side_effect = RuntimeError('flush failed')
        kafka_producer._kafka_producer = mock_producer

        # Should not raise
        kafka_producer._close_kafka_producer()


class TestSendKafkaMessage:
    """Tests for the send_kafka_message function."""

    def test_send_kafka_message_success(self, app):
        """Verify message is serialized and produced with correct topic, key, value, headers."""
        mock_producer = mock.Mock()

        content = {'id': 1, 'state': 'complete', 'batch': 1}
        properties = {'id': 1, 'state': 'complete', 'user': 'tbrady'}

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.build.state', content, properties
        )

        mock_producer.produce.assert_called_once_with(
            'dev.eng.iib.build.state',
            key=b'1',
            value=b'{"id": 1, "state": "complete", "batch": 1}',
            headers=[
                ('id', b'1'),
                ('state', b'complete'),
                ('user', b'tbrady'),
            ],
            on_delivery=kafka_producer.on_delivery_callback,
        )
        mock_producer.poll.assert_called_once_with(0)

    def test_send_kafka_message_none_values_filtered_from_headers(self, app):
        """Verify None property values are excluded from Kafka headers."""
        mock_producer = mock.Mock()

        content = {'batch': 5, 'state': 'in_progress'}
        properties = {'batch': 5, 'state': 'in_progress', 'user': None}

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.batch.state', content, properties
        )

        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs['headers'] == [
            ('batch', b'5'),
            ('state', b'in_progress'),
        ]

    def test_send_kafka_message_batch_key(self, app):
        """When no 'id' in properties, the message key should fall back to 'batch'."""
        mock_producer = mock.Mock()

        content = {'batch': 5, 'state': 'in_progress'}
        properties = {'batch': 5, 'state': 'in_progress', 'user': None}

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.batch.state', content, properties
        )

        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs['key'] == b'5'

    def test_send_kafka_message_no_properties(self, app):
        """Verify message works with no properties passed."""
        mock_producer = mock.Mock()

        kafka_producer.send_kafka_message(mock_producer, 'dev.eng.iib.build.state', {'id': 1})

        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs['headers'] == []
        assert call_kwargs['key'] is None

    def test_send_kafka_message_produce_error(self, app):
        """Verify exceptions from produce() are caught and do not raise."""
        mock_producer = mock.Mock()
        mock_producer.produce.side_effect = KafkaException('send failed')

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.build.state', {'id': 1}, {'id': 1}
        )

        # Should not raise — error is logged

    def test_send_kafka_message_unexpected_error(self, app):
        """Verify arbitrary unexpected exceptions are also swallowed."""
        mock_producer = mock.Mock()
        mock_producer.produce.side_effect = RuntimeError('unexpected failure')

        kafka_producer.send_kafka_message(
            mock_producer, 'dev.eng.iib.build.state', {'id': 1}, {'id': 1}
        )

        # Should not raise — error is logged

    def test_on_delivery_callback_logs_on_error(self):
        """Verify on_delivery_callback logs delivery failures without raising."""
        mock_err = mock.Mock()
        mock_msg = mock.Mock()
        mock_msg.topic.return_value = 'dev.eng.iib.build.state'

        with mock.patch('logging.getLogger') as mock_get_logger:
            kafka_producer.on_delivery_callback(mock_err, mock_msg)

        mock_get_logger.return_value.error.assert_called_once()

    def test_on_delivery_callback_noop_on_success(self):
        """Verify on_delivery_callback does nothing when err is None."""
        mock_msg = mock.Mock()

        with mock.patch('logging.getLogger') as mock_get_logger:
            kafka_producer.on_delivery_callback(None, mock_msg)

        mock_get_logger.return_value.error.assert_not_called()
