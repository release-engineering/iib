# SPDX-License-Identifier: GPL-3.0-or-later
import atexit
import json
import logging
import threading
from typing import Any, Dict, Optional, Union

from confluent_kafka import Producer
from flask import current_app

from iib.web.iib_static_types import (
    BaseClassRequestResponse,
    BatchRequestResponseList,
)

_kafka_producer: Optional[Producer] = None
_producer_lock = threading.Lock()


def _close_kafka_producer() -> None:
    """Flush pending messages and release the KafkaProducer singleton on interpreter shutdown."""
    if _kafka_producer is not None:
        try:
            _kafka_producer.flush(timeout=5)
        except Exception:
            logging.getLogger(__name__).exception('Failed to cleanly flush KafkaProducer')


def get_kafka_producer() -> Optional[Producer]:
    """
    Return a cached, thread-safe Kafka Producer.

    Returns ``None`` when any of ``IIB_KAFKA_BROKERS``, ``IIB_KAFKA_USERNAME``, or
    ``IIB_KAFKA_PASSWORD`` is not configured, or when the producer fails to initialize.

    :return: the shared Kafka producer, or ``None`` if Kafka messaging is not configured
    :rtype: confluent_kafka.Producer or None
    """
    global _kafka_producer

    # First check without lock for performance
    if _kafka_producer is not None:
        return _kafka_producer

    with _producer_lock:
        # Second check inside lock to prevent race conditions
        if _kafka_producer is not None:
            return _kafka_producer

        conf = current_app.config
        brokers = conf.get('IIB_KAFKA_BROKERS')
        username = conf.get('IIB_KAFKA_USERNAME')
        password = conf.get('IIB_KAFKA_PASSWORD')

        if not brokers or not username or not password:
            missing = [
                name
                for name, val in (
                    ('IIB_KAFKA_BROKERS', brokers),
                    ('IIB_KAFKA_USERNAME', username),
                    ('IIB_KAFKA_PASSWORD', password),
                )
                if not val
            ]
            current_app.logger.debug(
                'Kafka messaging is disabled: missing config: %s', ', '.join(missing)
            )
            return None

        try:

            producer_config: Dict[str, Any] = {
                'bootstrap.servers': ','.join(brokers),
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'SCRAM-SHA-512',
                'sasl.username': username,
                'sasl.password': password,
                'linger.ms': 5,
            }

            ca_file = conf.get('IIB_KAFKA_SSL_CAFILE')
            if ca_file:
                producer_config['ssl.ca.location'] = ca_file
            else:
                current_app.logger.warning(
                    'IIB_KAFKA_SSL_CAFILE is not set; ssl.ca.location will be omitted and '
                    'confluent-kafka will use OpenSSL\'s default trust store'
                )

            _kafka_producer = Producer(producer_config)
            atexit.register(_close_kafka_producer)

            current_app.logger.info('Kafka producer initialized')
            return _kafka_producer
        except Exception:
            current_app.logger.exception('Failed to initialize Kafka Producer')
            return None


def on_delivery_callback(err: Any, msg: Any) -> None:
    """
    Log failed Kafka message delivery in a background-thread-safe way.

    :param err: the delivery error reported by confluent-kafka, or ``None`` on success
    :param msg: the Kafka message object whose delivery was attempted
    """
    if err:
        logging.getLogger(__name__).error(
            'Failed to deliver message to Kafka topic %s: %s', msg.topic(), err
        )


def send_kafka_message(
    producer: Producer,
    topic: str,
    content: Union[BaseClassRequestResponse, BatchRequestResponseList],
    properties: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Send a single message to a Kafka topic asynchronously.

    :param confluent_kafka.Producer producer: the Kafka producer instance to use
    :param str topic: the Kafka topic to publish the message to
    :param dict content: the message payload; must be JSON-serializable
    :param dict properties: optional key/value pairs sent as Kafka headers and used to derive
        the message key (``id`` takes precedence over ``batch``)
    """
    properties = properties or {}
    headers = [(k, str(v).encode('utf-8')) for k, v in properties.items() if v is not None]
    message_key = properties.get('id', properties.get('batch'))
    serialized_key = str(message_key).encode('utf-8') if message_key is not None else None
    serialized_value = json.dumps(content).encode('utf-8')

    try:
        current_app.logger.info('Queuing message for Kafka topic %s', topic)

        producer.produce(
            topic,
            key=serialized_key,
            value=serialized_value,
            headers=headers,
            on_delivery=on_delivery_callback,
        )
        # Non-blocking poll to trigger any already-completed delivery callbacks.
        producer.poll(0)

    except Exception:
        current_app.logger.exception('Failed to queue message for Kafka topic %s', topic)
