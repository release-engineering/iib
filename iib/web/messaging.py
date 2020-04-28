# SPDX-License-Identifier: GPL-3.0-or-later
from collections import namedtuple
import json
import logging
import os
import uuid

from flask import current_app
import proton
import proton.reactor
import proton.utils

from iib.web.models import RequestStateMapping

__all__ = ['Envelope', 'json_to_envelope', 'send_messages', 'send_message_for_state_change']

log = logging.getLogger(__name__)


class BlockingConnection(proton.utils.BlockingConnection):  # pragma: no cover
    """
    Add support for multiple connection URLs in the ``BlockingConnection`` class.

    The class from ``proton.utils`` can be used directly when the following PR is released:
    https://github.com/apache/qpid-proton/pull/243
    """

    def __init__(
        self,
        url=None,
        timeout=None,
        container=None,
        ssl_domain=None,
        heartbeat=None,
        urls=None,
        **kwargs,
    ):
        self.disconnected = False
        self.timeout = timeout or 60
        self.container = container or proton.reactor.Container()
        self.container.timeout = self.timeout
        self.container.start()
        self.conn = None
        self.closing = False
        failed = True
        try:
            self.conn = self.container.connect(
                url=url,
                handler=self,
                ssl_domain=ssl_domain,
                reconnect=False,
                heartbeat=heartbeat,
                urls=urls,
                **kwargs,
            )
            self.wait(
                lambda: not (self.conn.state & proton.Endpoint.REMOTE_UNINIT),
                msg='Opening connection',
            )
            failed = False
        finally:
            if failed and self.conn:
                self.close()

    @property
    def url(self):
        """
        Get the current URL of the connection.

        :return: the connection URL or ``None``
        :rtype: str or None
        """
        return self.conn and self.conn.connected_address


Envelope = namedtuple('Envelope', 'address message')


def _get_ssl_domain():
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
        log.warning('Skipping authentication due to missing certificates and/or a private key')
        return

    domain = proton.SSLDomain(proton.SSLDomain.MODE_CLIENT)
    domain.set_credentials(conf['IIB_MESSAGING_CERT'], conf['IIB_MESSAGING_KEY'], None)
    domain.set_trusted_ca_db(conf['IIB_MESSAGING_CA'])
    domain.set_peer_authentication(proton.SSLDomain.VERIFY_PEER)
    return domain


def json_to_envelope(address, content, properties=None):
    """
    Create an ``Envelope`` object from a JSON dictionary.

    :param str address: the address to send the message to
    :param dict content: the JSON content of the message
    :param dict properties: the optional application properties of the message
    :return: the ``Envelope`` object
    :rtype: Envelope
    """
    message = proton.Message(body=json.dumps(content), properties=properties)
    message.id = str(uuid.uuid4())
    message.content_encoding = 'utf-8'
    message.content_type = 'application/json'
    message.durable = current_app.config['IIB_MESSAGING_DURABLE']
    return Envelope(address, message)


def send_messages(envelopes):
    """
    Send multiple messages in order while using a single connection and reusing sender links.

    If the IIB configuration ``IIB_MESSAGING_URLS`` is not set, the message will not be sent and
    an error will be logged.

    :param list envelopes: a list of ``Envelope`` objects representing the messages to send
    """
    conf = current_app.config
    if not conf.get('IIB_MESSAGING_URLS'):
        log.error('The "IIB_MESSAGING_URLS" must be set to send messages')
        return

    address_to_sender = {}
    connection = None
    try:
        connection = BlockingConnection(
            urls=conf['IIB_MESSAGING_URLS'],
            timeout=conf['IIB_MESSAGING_TIMEOUT'],
            ssl_domain=_get_ssl_domain(),
        )
        log.info('Connected to the message broker %s', connection.url)
        for envelope in envelopes:
            if envelope.address not in address_to_sender:
                address_to_sender[envelope.address] = connection.create_sender(envelope.address)

            log.info('Sending message %s to %s', envelope.message.id, envelope.address)
            address_to_sender[envelope.address].send(
                envelope.message, timeout=conf['IIB_MESSAGING_TIMEOUT']
            )
    finally:
        if connection:
            connection.close()


def send_message_for_state_change(request, new_batch_msg=False):
    """
    Send the appropriate message(s) based on a build request state change.

    Batch state messages will also be sent when appropriate.

    If IIB is not configured to send messages, this function will do nothing.

    :param iib.web.models.Request request: the request that changed state
    :param bool new_batch_msg: if ``True``, a new batch message will be sent; if ``False``,
        IIB will send a batch state change message if the batch is no longer ``in_progress``
    """
    request_address = current_app.config.get('IIB_MESSAGING_BUILD_STATE_DESTINATION')
    envelopes = []
    request_json = request.to_json(verbose=False)
    if request_address:
        log.debug('Preparing to send a state change message for request %d', request.id)
        properties = {
            'batch': request_json['batch'],
            'id': request_json['id'],
            'state': request_json['state'],
            'user': request_json['user'],
        }
        envelopes.append(json_to_envelope(request_address, request_json, properties))
    else:
        log.debug(
            'No request state change message will be sent since the configuration '
            '"IIB_MESSAGING_BUILD_STATE_DESTINATION" is not set'
        )

    batch_address = current_app.config.get('IIB_MESSAGING_BATCH_STATE_DESTINATION')
    if batch_address:
        if new_batch_msg:
            # Avoid querying the database for the batch state since we know it's a new batch
            batch_state = 'in_progress'
        else:
            batch_state = request.batch.state

        if new_batch_msg or batch_state in RequestStateMapping.get_final_states():
            log.debug(
                'Preparing to send a state change message for batch %d', request_json['batch']
            )
            content = {
                'batch': request_json['batch'],
                'request_ids': sorted(request.batch.request_ids),
                'state': batch_state,
                'user': request_json['user'],
            }
            properties = {
                'batch': request.batch.id,
                'state': batch_state,
                'user': request_json['user'],
            }
            envelopes.append(json_to_envelope(batch_address, content, properties))
    else:
        log.debug(
            'No batch state change message will be sent since the configuration '
            '"IIB_MESSAGING_BATCH_STATE_DESTINATION" is not set'
        )

    if envelopes:
        send_messages(envelopes)
