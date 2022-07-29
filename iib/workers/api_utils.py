# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging

import requests
from requests.packages.urllib3.util.retry import Retry
import requests_kerberos

from iib.exceptions import IIBError

log = logging.getLogger(__name__)


def get_requests_session(auth=False):
    """
    Create a requests session with authentication (when enabled).

    :param bool auth: configure authentication on the session
    :return: the configured requests session
    :rtype: requests.Session
    """
    session = requests.Session()
    if auth:
        session.auth = requests_kerberos.HTTPKerberosAuth(
            mutual_authentication=requests_kerberos.OPTIONAL
        )
    retry = Retry(
        total=3, read=3, connect=3, backoff_factor=3, status_forcelist=(408, 500, 502, 503, 504)
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_request(request_id):
    """
    Get the IIB build request from the REST API.

    :param int request_id: the ID of the IIB request
    :return: the request
    :rtype: dict
    :raises IIBError: if the HTTP request fails
    """
    # Prevent a circular import
    from iib.workers.config import get_worker_config

    config = get_worker_config()
    request_url = f'{config.iib_api_url.rstrip("/")}/builds/{request_id}'
    log.info('Getting the request %d', request_id)

    try:
        rv = requests_session.get(request_url, timeout=config.iib_api_timeout)
    except requests.RequestException:
        msg = f'The connection failed when getting the request {request_id}'
        log.exception(msg)
        raise IIBError(msg)

    if not rv.ok:
        log.error(
            'The worker failed to get the request %d. The status was %d. The text was:\n%s',
            request_id,
            rv.status_code,
            rv.text,
        )
        raise IIBError(f'The worker failed to get the request {request_id}')

    return rv.json()


def set_request_state(request_id, state, state_reason):
    """
    Set the state of the request using the IIB API.

    :param int request_id: the ID of the IIB request
    :param str state: the state to set the IIB request to
    :param str state_reason: the state reason to set the IIB request to
    :return: the updated request
    :rtype: dict
    :raise IIBError: if the request to the IIB API fails
    """
    log.info(
        'Setting the state of request %d to "%s" with the reason "%s"',
        request_id,
        state,
        state_reason,
    )
    payload = {'state': state, 'state_reason': state_reason}
    exc_msg = 'Setting the state to "{state}" on request {request_id} failed'
    return update_request(request_id, payload, exc_msg=exc_msg)


def set_omps_operator_version(request_id, omps_operator_version):
    """
    Set the set_omps_operator_version of the request using the IIB API.

    :param int request_id: the ID of the IIB request
    :param dict omps_operator_version: the state to set the IIB request to
    :return: the updated request
    :rtype: dict
    :raise IIBError: if the request to the IIB API fails
    """
    omps_operator_version_json = json.dumps(omps_operator_version)
    log.info(
        'Setting the omps_operator_version of request %d to "%s"',
        request_id,
        omps_operator_version_json,
    )
    payload = {'omps_operator_version': omps_operator_version_json}
    exc_msg = 'Setting the omps_operator_version to "{omps_operator_version}" failed'

    return update_request(request_id, payload, exc_msg=exc_msg)


def update_request(request_id, payload, exc_msg=None):
    """
    Update the IIB build request.

    :param int request_id: the ID of the IIB request
    :param dict payload: the payload to send to the PATCH API endpoint
    :param str exc_msg: an optional custom exception that can be a template
    :return: the updated request
    :rtype: dict
    :raises IIBError: if the request to the IIB API fails
    """
    # Prevent a circular import
    from iib.workers.config import get_worker_config

    config = get_worker_config()
    request_url = f'{config.iib_api_url.rstrip("/")}/builds/{request_id}'
    log.info('Patching the request %d with %r', request_id, payload)

    try:
        rv = requests_auth_session.patch(request_url, json=payload, timeout=config.iib_api_timeout)
    except requests.RequestException:
        msg = f'The connection failed when updating the request {request_id}'
        log.exception(msg)
        raise IIBError(msg)

    if not rv.ok:
        log.error(
            'The worker failed to update the request %d. The status was %d. The text was:\n%s',
            request_id,
            rv.status_code,
            rv.text,
        )
        if exc_msg:
            _exc_msg = exc_msg.format(**payload)
        else:
            _exc_msg = f'The worker failed to update the request {request_id}'
        raise IIBError(_exc_msg)

    return rv.json()


requests_auth_session = get_requests_session(auth=True)
requests_session = get_requests_session()
