# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
from typing import Any, Dict, Optional

import requests
from urllib3.util.retry import Retry
import requests_kerberos
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from iib.exceptions import IIBError, ValidationError
from iib.workers.config import get_worker_config
from iib.workers.tasks.iib_static_types import UpdateRequestPayload
import time
from iib.common.tracing import instrument_tracing

log = logging.getLogger(__name__)
config = get_worker_config()


def get_requests_session(auth: bool = False) -> requests.Session:
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


def get_request(request_id: int) -> Dict[str, Any]:
    """
    Get the IIB build request from the REST API.

    :param int request_id: the ID of the IIB request
    :return: the request
    :rtype: dict
    :raises IIBError: if the HTTP request fails
    """
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


@instrument_tracing(span_name="workers.api_utils.set_request_state")
def set_request_state(request_id: int, state: str, state_reason: str) -> Dict[str, Any]:
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
    payload: UpdateRequestPayload = {'state': state, 'state_reason': state_reason}
    exc_msg = 'Setting the state to "{state}" on request {request_id} failed'
    return update_request(request_id, payload, exc_msg=exc_msg)


def set_omps_operator_version(
    request_id: int,
    omps_operator_version: Dict[str, str],
) -> Dict[str, Any]:
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
    payload: UpdateRequestPayload = {'omps_operator_version': omps_operator_version_json}
    exc_msg = 'Setting the omps_operator_version to "{omps_operator_version}" failed'

    return update_request(request_id, payload, exc_msg=exc_msg)


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(config.iib_total_attempts),
    wait=wait_exponential(config.iib_retry_multiplier),
)
def update_request(
    request_id: int,
    payload: UpdateRequestPayload,
    exc_msg: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update the IIB build request.

    :param int request_id: the ID of the IIB request
    :param dict payload: the payload to send to the PATCH API endpoint
    :param str exc_msg: an optional custom exception that can be a template
    :return: the updated request
    :rtype: dict
    :raises ValidationError: if the request fails trying changing final state (complete, failed)
    :raises IIBError: if the request to the IIB API fails otherwise
    """
    # Prevent a circular import
    start_time = time.time()
    request_url = f'{config.iib_api_url.rstrip("/")}/builds/{request_id}'
    log.info('Patching the request %d with %r', request_id, payload)

    try:
        patch_start_time = time.time()
        rv = requests_auth_session.patch(request_url, json=payload, timeout=config.iib_api_timeout)
        log.debug(f"Update_request patch duration: {time.time() - patch_start_time}")
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
        if rv.json().get("error") in [
            "A failed request cannot change states",
            "A complete request cannot change states",
        ]:
            raise ValidationError(rv.json().get("error"))
        if exc_msg:
            _exc_msg = exc_msg.format(**payload, request_id=request_id)
        else:
            _exc_msg = f'The worker failed to update the request {request_id}'
        raise IIBError(_exc_msg)

    log.debug(f"Update_request duration: {time.time() - start_time}")
    return rv.json()


requests_auth_session = get_requests_session(auth=True)
requests_session = get_requests_session()
