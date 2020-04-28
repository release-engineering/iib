# SPDX-License-Identifier: GPL-3.0-or-later
import json
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers import greenwave


@mock.patch('iib.workers.greenwave._get_koji_build_nvr')
@mock.patch('iib.workers.greenwave.requests.post')
def test_gate_bundles_success(mock_requests, mock_gkbn):
    mock_gkbn.return_value = 'n-v-r'
    mock_requests.return_value.ok = True
    mock_requests.return_value.json.return_value = {"policies_satisfied": True}

    greenwave_config = {
        'subject_type': 'koji_build',
        'decision_context': 'iib_cvp_redhat_operator',
        'product_version': 'cvp',
    }
    greenwave.gate_bundles(['some-bundle'], greenwave_config)
    mock_gkbn.assert_called_once_with('some-bundle')
    mock_requests.assert_called_once()


@pytest.mark.parametrize(
    'greenwave_request_success, greenwave_json_rv, error_msg',
    (
        (
            False,
            {'message': 'Koji build unavailable'},
            'Gating check failed for some-bundle: Koji build unavailable',
        ),
        (
            True,
            {'Random Greenwave error': 'Response Changed'},
            'Key "policies_satisfied" missing in Greenwave response for some-bundle',
        ),
        (
            True,
            {'policies_satisfied': False},
            (
                'Unsatisfied Greenwave policy for some-bundle '
                'with decision_context: iib_cvp_redhat_operator, '
                'product_version: cvp, and subject_type: koji_build'
            ),
        ),
    ),
)
@mock.patch('iib.workers.greenwave._get_koji_build_nvr')
@mock.patch('iib.workers.greenwave.requests.post')
def test_gate_bundles_failure(
    mock_requests, mock_gkbn, greenwave_request_success, greenwave_json_rv, error_msg
):
    mock_gkbn.return_value = 'n-v-r'
    mock_requests.return_value.ok = greenwave_request_success
    mock_requests.return_value.json.return_value = greenwave_json_rv

    greenwave_config = {
        'subject_type': 'koji_build',
        'decision_context': 'iib_cvp_redhat_operator',
        'product_version': 'cvp',
    }
    with pytest.raises(IIBError, match=error_msg):
        greenwave.gate_bundles(['some-bundle'], greenwave_config)
    mock_gkbn.assert_called_once_with('some-bundle')
    mock_requests.assert_called_once()


@mock.patch('iib.workers.greenwave._get_koji_build_nvr')
@mock.patch('iib.workers.greenwave.requests.post')
def test_gate_bundles_invalid_json(mock_requests, mock_gkbn):
    mock_gkbn.return_value = 'n-v-r'
    mock_requests.return_value.ok = True
    mock_requests.return_value.json.side_effect = json.JSONDecodeError("error", "\n\n", 1)

    greenwave_config = {
        'subject_type': 'koji_build',
        'decision_context': 'iib_cvp_redhat_operator',
        'product_version': 'cvp',
    }
    error_msg = 'Key "policies_satisfied" missing in Greenwave response for some-bundle'
    with pytest.raises(IIBError, match=error_msg):
        greenwave.gate_bundles(['some-bundle'], greenwave_config)
    mock_gkbn.assert_called_once_with('some-bundle')
    mock_requests.assert_called_once()


@mock.patch('iib.workers.greenwave.get_image_labels')
def test_get_koji_build_nvr(mock_gil):
    mock_gil.return_value = {'com.redhat.component': 'name', 'version': 1, 'release': '32'}
    assert greenwave._get_koji_build_nvr('some-image:latest') == 'name-1-32'


def test_verify_greenwave_config_failure():
    error_msg = 'IIB is not configured to handle gating of bundles'
    greenwave_config = {'subject_type': 'koji_build'}
    with pytest.raises(IIBError, match=error_msg):
        greenwave._validate_greenwave_params_and_config({}, greenwave_config)
