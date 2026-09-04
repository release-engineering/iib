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


@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_success(mock_producer_class, app, monkeypatch):
    app.config['IIB_KAFKA_BROKERS'] = ['broker1:9096', 'broker2:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'test_user'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'test_pass')

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
def test_get_kafka_producer_ssl_cafile_default(mock_producer_class, app, monkeypatch):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')

    kafka_producer.get_kafka_producer()

    call_config = mock_producer_class.call_args[0][0]
    assert call_config['ssl.ca.location'] == '/etc/pki/tls/certs/ca-bundle.crt'


@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_ssl_cafile_custom(mock_producer_class, app, monkeypatch):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')
    app.config['IIB_KAFKA_SSL_CAFILE'] = '/custom/ca-bundle.crt'

    kafka_producer.get_kafka_producer()

    call_config = mock_producer_class.call_args[0][0]
    assert call_config['ssl.ca.location'] == '/custom/ca-bundle.crt'


@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_ssl_cafile_omitted_when_falsy(
    mock_producer_class, app, monkeypatch, caplog
):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')
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
    'brokers,username,set_password',
    [
        (None, 'user', True),
        ([], 'user', True),
        (['broker:9096'], None, True),
        (['broker:9096'], 'user', False),
        (['broker:9096'], None, False),
    ],
)
def test_get_kafka_producer_skips_when_any_config_missing(
    app, monkeypatch, caplog, brokers, username, set_password
):
    app.config['IIB_KAFKA_BROKERS'] = brokers
    app.config['IIB_KAFKA_USERNAME'] = username
    if set_password:
        monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'pass')
    else:
        monkeypatch.delenv('IIB_KAFKA_PASSWORD', raising=False)
    app.logger.disabled = False
    app.logger.setLevel(logging.DEBUG)

    producer = kafka_producer.get_kafka_producer()

    assert producer is None
    assert 'Kafka messaging is disabled: missing config:' in caplog.text


def test_get_kafka_producer_no_brokers(app):
    app.config['IIB_KAFKA_BROKERS'] = None

    producer = kafka_producer.get_kafka_producer()

    assert producer is None


def test_get_kafka_producer_empty_brokers(app):
    app.config['IIB_KAFKA_BROKERS'] = []

    producer = kafka_producer.get_kafka_producer()

    assert producer is None


@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_cached(mock_producer_class, app, monkeypatch):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')

    producer1 = kafka_producer.get_kafka_producer()
    producer2 = kafka_producer.get_kafka_producer()

    assert producer1 is producer2
    mock_producer_class.assert_called_once()


@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_init_failure(mock_producer_class, app, monkeypatch):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')
    mock_producer_class.side_effect = KafkaException('connection failed')

    producer = kafka_producer.get_kafka_producer()

    assert producer is None


@mock.patch('iib.web.kafka_producer.atexit')
@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_registers_atexit_cleanup(
    mock_producer_class, mock_atexit, app, monkeypatch
):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')

    kafka_producer.get_kafka_producer()

    mock_atexit.register.assert_called_once_with(kafka_producer._close_kafka_producer)


@mock.patch('iib.web.kafka_producer.atexit')
@mock.patch('iib.web.kafka_producer.Producer')
def test_get_kafka_producer_cached_does_not_reregister_atexit(
    mock_producer_class, mock_atexit, app, monkeypatch
):
    app.config['IIB_KAFKA_BROKERS'] = ['broker:9096']
    app.config['IIB_KAFKA_USERNAME'] = 'u'
    monkeypatch.setenv('IIB_KAFKA_PASSWORD', 'p')

    kafka_producer.get_kafka_producer()
    kafka_producer.get_kafka_producer()

    mock_atexit.register.assert_called_once()


def test_close_kafka_producer_flushes_existing_producer():
    mock_producer = mock.Mock()
    kafka_producer._kafka_producer = mock_producer

    kafka_producer._close_kafka_producer()

    mock_producer.flush.assert_called_once_with(timeout=5)


def test_close_kafka_producer_noop_when_not_initialized():
    kafka_producer._kafka_producer = None

    # Should not raise
    kafka_producer._close_kafka_producer()


def test_close_kafka_producer_swallows_errors():
    mock_producer = mock.Mock()
    mock_producer.flush.side_effect = RuntimeError('flush failed')
    kafka_producer._kafka_producer = mock_producer

    # Should not raise
    kafka_producer._close_kafka_producer()


def test_send_kafka_message_success(app):
    mock_producer = mock.Mock()

    content = {'id': 1, 'state': 'complete', 'batch': 1}
    properties = {'id': 1, 'state': 'complete', 'user': 'tbrady'}

    kafka_producer.send_kafka_message(mock_producer, 'dev.eng.iib.build.state', content, properties)

    mock_producer.produce.assert_called_once_with(
        'dev.eng.iib.build.state',
        key=b'1',
        value=b'{"id": 1, "state": "complete", "batch": 1}',
        headers=[
            ('id', b'1'),
            ('state', b'complete'),
            ('user', b'tbrady'),
        ],
        on_delivery=kafka_producer._on_delivery_callback,
    )
    mock_producer.poll.assert_called_once_with(0)


def test_send_kafka_message_none_values_filtered_from_headers(app):
    mock_producer = mock.Mock()

    content = {'batch': 5, 'state': 'in_progress'}
    properties = {'batch': 5, 'state': 'in_progress', 'user': None}

    kafka_producer.send_kafka_message(mock_producer, 'dev.eng.iib.batch.state', content, properties)

    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs['headers'] == [
        ('batch', b'5'),
        ('state', b'in_progress'),
    ]


def test_send_kafka_message_batch_key(app):
    mock_producer = mock.Mock()

    content = {'batch': 5, 'state': 'in_progress'}
    properties = {'batch': 5, 'state': 'in_progress', 'user': None}

    kafka_producer.send_kafka_message(mock_producer, 'dev.eng.iib.batch.state', content, properties)

    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs['key'] == b'5'


def test_send_kafka_message_no_properties(app):
    mock_producer = mock.Mock()

    kafka_producer.send_kafka_message(mock_producer, 'dev.eng.iib.build.state', {'id': 1})

    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs['headers'] == []
    assert call_kwargs['key'] is None


def test_send_kafka_message_produce_error(app):
    mock_producer = mock.Mock()
    mock_producer.produce.side_effect = KafkaException('send failed')

    kafka_producer.send_kafka_message(
        mock_producer, 'dev.eng.iib.build.state', {'id': 1}, {'id': 1}
    )

    # Should not raise — error is logged


def test_send_kafka_message_unexpected_error(app):
    mock_producer = mock.Mock()
    mock_producer.produce.side_effect = RuntimeError('unexpected failure')

    kafka_producer.send_kafka_message(
        mock_producer, 'dev.eng.iib.build.state', {'id': 1}, {'id': 1}
    )

    # Should not raise — error is logged


def test__on_delivery_callback_logs_on_error():
    mock_err = mock.Mock()
    mock_msg = mock.Mock()
    mock_msg.topic.return_value = 'dev.eng.iib.build.state'

    with mock.patch('logging.getLogger') as mock_get_logger:
        kafka_producer._on_delivery_callback(mock_err, mock_msg)

    mock_get_logger.return_value.error.assert_called_once()


def test__on_delivery_callback_noop_on_success():
    mock_msg = mock.Mock()

    with mock.patch('logging.getLogger') as mock_get_logger:
        kafka_producer._on_delivery_callback(None, mock_msg)

    mock_get_logger.return_value.error.assert_not_called()
