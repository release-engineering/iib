# SPDX-License-Identifier: GPL-3.0-or-later
from collections import namedtuple
import json
import os
from typing import Any, cast, Dict, List, Optional, Union
import uuid

from flask import current_app
import proton
import proton.reactor
import proton.utils
from proton.utils import BlockingConnection

from iib.web.iib_static_types import (
    BaseClassRequestResponse,
    BatchRequestResponseList,
)
from iib.web.models import Batch, Request, RequestStateMapping
from iib.web.kafka_producer import get_kafka_producer, send_kafka_message

__all__ = ['Envelope', 'json_to_envelope', 'send_messages', 'send_message_for_state_change']


Envelope = namedtuple('Envelope', 'address message')


def _build_request_state_change_data(
    request: Request,
) -> tuple:
    """
    Build the content and properties for a request state change message.

    Used by both the legacy AMQP and Kafka paths.

    :param iib.web.models.Request request: the request that changed states
    :return: a tuple of (content, properties)
    :rtype: tuple
    """
    request_json = cast(BaseClassRequestResponse, request.to_json(verbose=False))
    properties = {
        'batch': request_json['batch'],
        'id': request_json['id'],
        'state': request_json['state'],
        'user': request_json['user'],
    }
    return request_json, properties


def _build_batch_state_change_data(
    batch: Batch,
    new_batch: Optional[bool] = False,
) -> Optional[tuple]:
    """
    Build the content and properties for a batch state change message.

    Returns ``None`` when no message should be sent.
    Used by both the legacy AMQP and Kafka paths.

    :param iib.web.models.Batch batch: the batch that changed states
    :param bool new_batch: if ``True``, a new batch message will be generated
    :return: a tuple of (content, properties) or None
    :rtype: tuple or None
    """
    if new_batch:
        batch_state = 'in_progress'
    else:
        batch_state = batch.state

    if not (new_batch or batch_state in RequestStateMapping.get_final_states()):
        return None

    batch_username = getattr(batch.user, 'username', None)
    content: BatchRequestResponseList = {
        'batch': batch.id,
        'annotations': batch.annotations,
        'requests': [
            {
                'id': r.id,
                'organization': getattr(r, 'organization', None),
                'request_type': r.type_name,
            }
            for r in batch.requests
        ],
        'state': batch_state,
        'user': batch_username,
    }
    properties = {
        'batch': batch.id,
        'state': batch_state,
        'user': batch_username,
    }
    return content, properties


def _get_batch_state_change_envelope(
    batch: Batch,
    new_batch: Optional[bool] = False,
) -> Optional[Envelope]:
    """
    Generate a batch state change ``Envelope`` object.

    No message will be generated if IIB is not configured to send batch state change messages or
    no batch state change message is needed .

    :param iib.web.models.Batch batch: the batch that changed states
    :param bool new_batch: if ``True``, a new batch message will be generated; if ``False``,
        IIB will generate a batch state change message if the batch is no longer ``in_progress``
    :return: the ``Envelope`` for the batch state change or ``None``
    :rtype: Envelope or None
    """
    batch_address = current_app.config.get('IIB_MESSAGING_BATCH_STATE_DESTINATION')
    if not batch_address:
        current_app.logger.debug(
            'No batch state change message will be generated since the configuration '
            '"IIB_MESSAGING_BATCH_STATE_DESTINATION" is not set'
        )
        return None

    data = _build_batch_state_change_data(batch, new_batch)
    if data:
        current_app.logger.debug('Preparing to send a state change message for batch %d', batch.id)
        content, properties = data
        return json_to_envelope(batch_address, content, properties)
    return None


def _get_request_state_change_envelope(request: Request) -> Optional[Envelope]:
    """
    Generate a request state change ``Envelope`` object.

    No message will be generated if IIB is not configured to send request state change messages.

    :param iib.web.models.Request request: the request that changed states
    :return: the ``Envelope`` for the request state change or ``None``
    :rtype: Envelope or None
    """
    request_address = current_app.config.get('IIB_MESSAGING_BUILD_STATE_DESTINATION')
    if not request_address:
        current_app.logger.debug(
            'No request state change message will be generated since the configuration '
            '"IIB_MESSAGING_BUILD_STATE_DESTINATION" is not set'
        )
        return None

    current_app.logger.debug('Preparing to send a state change message for request %d', request.id)
    content, properties = _build_request_state_change_data(request)
    return json_to_envelope(request_address, content, properties)


def _get_ssl_domain() -> Optional[proton.SSLDomain]:
    """
    Create the SSL configuration object for qpid-proton.

    :return: the configured ``SSLDomain`` object or ``None`` when SSL is not configured
    :rtype: proton.SSLDomain
    """
    conf = current_app.config
    if not all(
        conf.get(key) and os.path.exists(conf[key])
        for key in ('IIB_MESSAGING_CERT', 'IIB_MESSAGING_KEY', 'IIB_MESSAGING_CA')
    ):
        current_app.logger.warning(
            'Skipping authentication due to missing certificates and/or a private key'
        )
        return None

    domain = proton.SSLDomain(proton.SSLDomain.MODE_CLIENT)
    domain.set_credentials(conf['IIB_MESSAGING_CERT'], conf['IIB_MESSAGING_KEY'], None)
    domain.set_trusted_ca_db(conf['IIB_MESSAGING_CA'])
    domain.set_peer_authentication(proton.SSLDomain.VERIFY_PEER)
    return domain


def json_to_envelope(
    address: str,
    content: Union[BaseClassRequestResponse, BatchRequestResponseList],
    properties: Optional[Dict[str, Any]] = None,
) -> Envelope:
    """
    Create an ``Envelope`` object from a JSON dictionary.

    :param str address: the address to send the message to
    :param dict content: the JSON content of the message
    :param dict properties: the optional application properties of the message
    :return: the ``Envelope`` object
    :rtype: Envelope
    """
    message = proton.Message(body=json.dumps(content), properties=properties)
    message.correlation_id = str(uuid.uuid4())
    message.content_type = 'application/json'
    message.durable = current_app.config['IIB_MESSAGING_DURABLE']
    return Envelope(address, message)


def send_messages(envelopes: List[Envelope]) -> None:
    """
    Send multiple messages in order while using a single connection and reusing sender links.

    If the IIB configuration ``IIB_MESSAGING_URLS`` is not set, the message will not be sent and
    an error will be logged.

    If the message(s) can't be sent, the exception will be logged but no exception will be raised
    since this is not considered a fatal error by the application.

    :param list envelopes: a list of ``Envelope`` objects representing the messages to send
    """
    conf = current_app.config
    if not conf.get('IIB_MESSAGING_URLS'):
        current_app.logger.error('The "IIB_MESSAGING_URLS" must be set to send messages')
        return None

    address_to_sender = {}
    connection = None
    try:
        connection = BlockingConnection(
            urls=conf['IIB_MESSAGING_URLS'],
            timeout=conf['IIB_MESSAGING_TIMEOUT'],
            ssl_domain=_get_ssl_domain(),
        )
        current_app.logger.info('Connected to the message broker %s', connection.url)
        for envelope in envelopes:
            if envelope.address not in address_to_sender:
                address_to_sender[envelope.address] = connection.create_sender(envelope.address)

            current_app.logger.info(
                'Sending message %s (correlation-id) to %s',
                envelope.message.correlation_id,
                envelope.address,
            )
            address_to_sender[envelope.address].send(
                envelope.message, timeout=conf['IIB_MESSAGING_TIMEOUT']
            )
    except:  # noqa: E722
        current_app.logger.exception('Failed to send one or more messages')
    finally:
        if connection:
            connection.close()


def _send_kafka_messages(
    requests: List[Request],
    batch: Batch,
    new_batch: Optional[bool] = False,
) -> None:
    """
    Send request and batch state-change messages to Kafka.

    :param list requests: one or more requests whose state changed
    :param iib.web.models.Batch batch: the batch associated with the requests
    :param bool new_batch: if ``True``, a batch-creation message is sent
    """
    producer = get_kafka_producer()
    if not producer:
        return

    conf = current_app.config

    build_topic = conf.get('IIB_KAFKA_BUILD_STATE_TOPIC')
    if build_topic:
        for request in requests:
            content, properties = _build_request_state_change_data(request)
            send_kafka_message(producer, build_topic, content, properties)

    batch_topic = conf.get('IIB_KAFKA_BATCH_STATE_TOPIC')
    if batch_topic:
        batch_data = _build_batch_state_change_data(batch, new_batch)
        if batch_data:
            content, properties = batch_data
            send_kafka_message(producer, batch_topic, content, properties)


def send_message_for_state_change(request: Request, new_batch_msg: Optional[bool] = False) -> None:
    """
    Send the appropriate message(s) based on a build request state change.

    Batch state messages will also be sent when appropriate.

    If IIB is not configured to send messages, this function will do nothing.

    :param iib.web.models.Request request: the request that changed state
    :param bool new_batch_msg: if ``True``, a new batch message will be sent; if ``False``,
        IIB will send a batch state change message if the batch is no longer ``in_progress``
    """
    envelopes = []
    request_envelope = _get_request_state_change_envelope(request)
    if request_envelope:
        envelopes.append(request_envelope)

    batch_envelope = _get_batch_state_change_envelope(request.batch, new_batch_msg)
    if batch_envelope:
        envelopes.append(batch_envelope)

    if envelopes:
        send_messages(envelopes)

    _send_kafka_messages([request], request.batch, new_batch=new_batch_msg)


def send_messages_for_new_batch_of_requests(requests: List[Request]) -> None:
    """
    Send the appropriate message(s) based on a new batch of build requests.

    If IIB is not configured to send messages, this function will do nothing.

    :param list requests: the requests that were created as part of the batch request
    """
    if not requests:
        return None

    envelopes = []

    for request in requests:
        request_envelope = _get_request_state_change_envelope(request)
        if request_envelope:
            envelopes.append(request_envelope)

    # Just use the first request's batch since the batch is the same for all of them
    batch = requests[0].batch
    batch_envelope = _get_batch_state_change_envelope(batch, new_batch=True)
    if batch_envelope:
        envelopes.append(batch_envelope)

    if envelopes:
        send_messages(envelopes)

    _send_kafka_messages(requests, batch, new_batch=True)
