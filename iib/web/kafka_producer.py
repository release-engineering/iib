# SPDX-License-Identifier: GPL-3.0-or-later
import json
import threading
from typing import Any, Dict, Optional, Union

from flask import current_app
from kafka import KafkaProducer
from kafka.errors import KafkaError

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
            _kafka_producer = KafkaProducer(
                bootstrap_servers=brokers,
                security_protocol=conf.get('IIB_KAFKA_SECURITY_PROTOCOL'),
                sasl_mechanism=conf.get('IIB_KAFKA_SASL_MECHANISM'),
                sasl_plain_username=conf.get('IIB_KAFKA_USERNAME'),
                sasl_plain_password=conf.get('IIB_KAFKA_PASSWORD'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                linger_ms=5,
            )

            current_app.logger.info('Kafka producer initialised')
            return _kafka_producer
        except Exception:
            current_app.logger.exception('Failed to initialise KafkaProducer')
            return None


def on_send_success(record_metadata):
    """Handle successful Kafka message delivery."""
    pass


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
    headers = [(k, str(v).encode('utf-8')) for k, v in properties.items()]
    message_key = properties.get('id', properties.get('batch'))

    try:
        current_app.logger.info(f'Queuing message for Kafka topic {topic}')

        future = producer.send(topic, key=message_key, value=content, headers=headers)

        # Attach callbacks for async failure handling
        future.add_errback(on_send_error)

    except KafkaError:
        current_app.logger.exception(f'Failed to queue message for Kafka topic {topic}')
