# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.web.app import load_config, validate_api_config
from iib.exceptions import ConfigError


@mock.patch('iib.web.app.os.getenv')
@mock.patch('iib.web.app.os.path.isfile')
def test_load_config_dev(mock_isfile, mock_getenv):
    mock_app = mock.Mock()

    def new_getenv(key, default_value):
        return {'IIB_DEV': 'true'}.get(key, default_value)

    mock_getenv.side_effect = new_getenv
    load_config(mock_app)

    mock_app.config.from_object.assert_called_once_with('iib.web.config.DevelopmentConfig')
    mock_isfile.assert_not_called()


@mock.patch('iib.web.app.os.getenv')
@mock.patch('iib.web.app.os.path.isfile')
def test_load_config_prod(mock_isfile, mock_getenv):
    mock_isfile.return_value = True
    mock_app = mock.Mock()

    load_config(mock_app)

    mock_app.config.from_object.assert_called_once_with('iib.web.config.ProductionConfig')
    mock_isfile.assert_called_once()
    mock_app.config.from_pyfile.assert_called_once_with('/etc/iib/settings.py')


@pytest.mark.parametrize(
    'config, error_msg',
    (
        (
            {
                'IIB_GREENWAVE_CONFIG': {
                    'patriots': {'subject_type': 'st', 'product_version': 'pv'},
                    None: {'subject_type': 'st', 'product_version': 'pv'},
                },
                'IIB_USER_TO_QUEUE': {'tbrady': 'not-patriots'},
            },
            'The following queues are invalid in "IIB_GREENWAVE_CONFIG": patriots',
        ),
        (
            {
                'IIB_GREENWAVE_CONFIG': {
                    'iib-user': {'subject_type': 'st', 'product_version': 'pv'}
                },
                'IIB_USER_TO_QUEUE': {'msdhoni': 'iib-user'},
            },
            'Missing required params decision_context for queue iib-user in "IIB_GREENWAVE_CONFIG"',
        ),
        (
            {
                'IIB_GREENWAVE_CONFIG': {
                    'iib-user': {
                        'subject_type': 'st',
                        'product_version': 'pv',
                        'decision_context': 'dc',
                        'malicious': 'mal',
                    },
                },
                'IIB_USER_TO_QUEUE': {'msdhoni': 'iib-user'},
            },
            'Invalid params malicious for queue iib-user in "IIB_GREENWAVE_CONFIG"',
        ),
        (
            {
                'IIB_GREENWAVE_CONFIG': {
                    'iib-user': {
                        'subject_type': 'not_koji_build',
                        'product_version': 'pv',
                        'decision_context': 'dc',
                    },
                },
                'IIB_USER_TO_QUEUE': {'msdhoni': 'iib-user'},
            },
            (
                'IIB only supports gating for subject_type "koji_build". Invalid subject_type '
                'not_koji_build defined for queue iib-user in "IIB_GREENWAVE_CONFIG"'
            ),
        ),
    ),
)
def test_validate_api_config_failure_greenwave_params(config, error_msg):
    with pytest.raises(ConfigError, match=error_msg):
        validate_api_config(config)


@pytest.mark.parametrize(
    'config, error_msg',
    (
        (
            {'IIB_BINARY_IMAGE_CONFIG': {'tom-brady': {}}, 'IIB_GREENWAVE_CONFIG': {}},
            (
                'distribution_scope values must be one of the following'
                ' "prod", "stage" or "dev" strings.'
            ),
        ),
        (
            {'IIB_BINARY_IMAGE_CONFIG': {'prod': []}, 'IIB_GREENWAVE_CONFIG': {}},
            (
                'Value for distribution_scope keys must be a dict mapping'
                ' ocp_version to binary_image'
            ),
        ),
        (
            {'IIB_BINARY_IMAGE_CONFIG': {'prod': {'v4.5': 2}}, 'IIB_GREENWAVE_CONFIG': {}},
            'All ocp_version and binary_image values must be strings.',
        ),
        (
            {'IIB_BINARY_IMAGE_CONFIG': ['something'], 'IIB_GREENWAVE_CONFIG': {}},
            (
                'IIB_BINARY_IMAGE_CONFIG must be a dict mapping distribution_scope to '
                'another dict mapping ocp_version to binary_image'
            ),
        ),
    ),
)
def test_validate_api_config_failure_binary_image_params(config, error_msg):
    with pytest.raises(ConfigError, match=error_msg):
        validate_api_config(config)
