# SPDX-License-Identifier: GPL-3.0-or-later
from copy import deepcopy
import json
import logging

import requests

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import get_image_labels

log = logging.getLogger(__name__)


def gate_bundles(bundles, greenwave_config):
    """
    Check if all bundle images have passed gating tests in the CVP pipeline.

    This function queries Greenwave to check if the policies are satisfied for each bundle image.

    :param list bundles: a list of strings representing the pull specifications of the bundles to
        be gated.
    :param dict greenwave_config: the dict of config required to query Greenwave to gate bundles.
    :raises IIBError: if any of the bundles fail the gating checks or IIB fails to get a
        response from Greenwave.
    """
    conf = get_worker_config()
    _validate_greenwave_params_and_config(conf, greenwave_config)

    log.info('Gating on bundles: %s', ', '.join(bundles))
    gating_unsatisfied_bundles = []
    testcases = []
    for bundle in bundles:
        koji_build_nvr = _get_koji_build_nvr(bundle)
        log.debug('Querying Greenwave for decision on %s', koji_build_nvr)
        payload = deepcopy(greenwave_config)
        payload['subject_identifier'] = koji_build_nvr
        log.debug(
            'Querying Greenwave with decision_context: %s, product_version: %s, '
            'subject_identifier: %s and subject_type: %s',
            payload["decision_context"],
            payload["product_version"],
            payload["subject_identifier"],
            payload["subject_type"],
        )

        request_url = f'{conf["iib_greenwave_url"].rstrip("/")}/decision'
        resp = requests.post(request_url, json=payload)
        try:
            data = resp.json()
        except json.JSONDecodeError:
            log.error('Error encountered in decoding JSON %s', resp.text)
            data = {}

        if not resp.ok:
            error_msg = data.get('message') or resp.text
            log.error('Request to Greenwave failed: %s', error_msg)
            raise IIBError(f'Gating check failed for {bundle}: {error_msg}')

        try:
            if not data['policies_satisfied']:
                log.info('Gating decision for %s: %s', bundle, data)
                gating_unsatisfied_bundles.append(bundle)
                testcases = [item['testcase'] for item in data.get('unsatisfied_requirements', [])]

        except KeyError:
            log.error('Missing key "policies_satisfied" for %s: %s', bundle, data)
            raise IIBError(f'Key "policies_satisfied" missing in Greenwave response for {bundle}')

    if gating_unsatisfied_bundles:
        error_msg = (
            f'Unsatisfied Greenwave policy for {", ".join(gating_unsatisfied_bundles)} '
            f'with decision_context: {greenwave_config["decision_context"]}, '
            f'product_version: {greenwave_config["product_version"]}, '
            f'subject_type: {greenwave_config["subject_type"]} '
            f'and test cases: {", ".join(testcases)}'
        )
        raise IIBError(error_msg)


def _get_koji_build_nvr(bundle):
    """
    Get the Koji build NVR of the bundle from its labels.

    :param str bundle: the pull specification of the bundle image to be gated.
    :return: the Koji build NVR of the bundle image.
    :rtype: str
    """
    labels = get_image_labels(bundle)
    return '{}-{}-{}'.format(labels['com.redhat.component'], labels['version'], labels['release'])


def _validate_greenwave_params_and_config(conf, greenwave_config):
    """
    Validate payload parameters and config variables required for gating bundles.

    :param dict conf: the IIB worker configuration.
    :param dict greenwave_config: the dict of config required to query Greenwave to gate bundles.
    :raises IIBError: if IIB is not configured to handle gating of bundles.
    """
    if not conf.get('iib_greenwave_url'):
        log.error('iib_greenwave_url not set in the Celery config')
        raise IIBError('IIB is not configured to handle gating of bundles')
