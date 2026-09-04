# SPDX-License-Identifier: GPL-3.0-or-later
import json
import threading
from typing import Any, Dict, Optional, Union

from flask import current_app
from kafka import KafkaProducer

from iib.web.iib_static_types import (
    BaseClassRequestResponse,
    BatchRequestResponseList,
)

_kafka_producer: Optional[KafkaProducer] = None
_producer_lock = threading.Lock()


def get_kafka_producer() -> Optional[KafkaProducer]:
    """Return a cached, thread-safe KafkaProducer."""
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
        if not brokers:
            return None

        try:
            username = conf.get('IIB_KAFKA_USERNAME')
            password = conf.get('IIB_KAFKA_PASSWORD')

            if not username or not password:
                current_app.logger.error('Kafka credentials missing, cannot initialize producer')
                return None

            producer_kwargs = {
                'bootstrap_servers': brokers,
                'security_protocol': 'SASL_SSL',
                'sasl_mechanism': 'SCRAM-SHA-512',
                'sasl_plain_username': username,
                'sasl_plain_password': password,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None,
                'linger_ms': 5,
            }

            ca_file = conf.get('IIB_KAFKA_SSL_CAFILE')
            if ca_file:
                producer_kwargs['ssl_cafile'] = ca_file

            _kafka_producer = KafkaProducer(**producer_kwargs)

            current_app.logger.info('Kafka producer initialised')
            return _kafka_producer
        except Exception:
            current_app.logger.exception('Failed to initialise KafkaProducer')
            return None


def on_send_error(excp):
    """Log failed Kafka message delivery in a background-thread-safe way."""
    import logging

    logging.getLogger(__name__).error('Failed to deliver message to Kafka', exc_info=excp)


def send_kafka_message(
    producer: KafkaProducer,
    topic: str,
    content: Union[BaseClassRequestResponse, BatchRequestResponseList],
    properties: Optional[Dict[str, Any]] = None,
) -> None:
    """Send a single message to a Kafka topic asynchronously."""
    properties = properties or {}
    headers = [(k, str(v).encode('utf-8')) for k, v in properties.items() if v is not None]
    message_key = properties.get('id', properties.get('batch'))

    try:
        current_app.logger.info(f'Queuing message for Kafka topic {topic}')

        future = producer.send(topic, key=message_key, value=content, headers=headers)

        # Attach callbacks for async failure handling
        future.add_errback(on_send_error)

    except Exception:
        current_app.logger.exception(f'Failed to queue message for Kafka topic {topic}')
