# SPDX-License-Identifier: GPL-3.0-or-later
from unittest.mock import patch
from io import BytesIO
import os

import celery
import pytest

from iib.exceptions import ConfigError
from iib.workers.config import configure_celery, validate_celery_config


@patch('os.path.isfile', return_value=False)
def test_configure_celery_with_classes(mock_isfile):
    celery_app = celery.Celery()
    assert celery_app.conf.task_default_queue == 'celery'
    configure_celery(celery_app)
    assert celery_app.conf.task_default_queue == 'iib'


@patch('os.getenv')
@patch('os.path.isfile', return_value=True)
@patch('iib.workers.config.open')
def test_configure_celery_with_classes_and_files(mock_open, mock_isfile, mock_getenv):
    mock_getenv.return_value = ''
    mock_open.return_value = BytesIO(
        b'task_default_queue = "not-iib"\ntimezone="America/New_York"\n'
    )
    celery_app = celery.Celery()
    assert celery_app.conf.task_default_queue == 'celery'
    assert celery_app.conf.timezone is None
    configure_celery(celery_app)
    assert celery_app.conf.task_default_queue == 'not-iib'
    assert celery_app.conf.timezone == 'America/New_York'


def test_validate_celery_config():
    validate_celery_config(
        {
            'iib_api_url': 'http://localhost:8080/api/v1/',
            'iib_organization_customizations': {},
            'iib_registry': 'registry',
            'iib_required_labels': {},
        }
    )


@pytest.mark.parametrize('missing_key', ('iib_api_url', 'iib_registry'))
def test_validate_celery_config_failure(missing_key):
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
    }
    conf.pop(missing_key)
    with pytest.raises(ConfigError, match=f'{missing_key} must be set'):
        validate_celery_config(conf)


def test_validate_celery_config_iib_required_labels_not_dict():
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': 123,
    }
    with pytest.raises(ConfigError, match='iib_required_labels must be a dictionary'):
        validate_celery_config(conf)


@pytest.mark.parametrize(
    'config, error',
    (
        ('Do or do not. There is no try.', 'iib_organization_customizations must be a dictionary'),
        ({123: {}}, 'The keys in iib_organization_customizations must be strings'),
        (
            {'company-marketplace': 123},
            'The values in iib_organization_customizations must be dictionaries',
        ),
        (
            {'company-marketplace': {'Yoda': 'Do or do not. There is no try.'}},
            'The following keys set on iib_organization_customizations are invalid: Yoda',
        ),
        (
            {
                'company-marketplace': {
                    'csv_annotations': {
                        123: (
                            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
                        ),
                    },
                }
            },
            (
                'The keys in iib_organization_customizations.company-marketplace.csv_annotations '
                'must be strings'
            ),
        ),
        (
            {
                'company-marketplace': {
                    'registry_replacements': {123: 'registry.marketplace.company.com/cm'},
                }
            },
            (
                'The keys in iib_organization_customizations.company-marketplace.'
                'registry_replacements must be strings'
            ),
        ),
        (
            {
                'company-marketplace': {
                    'csv_annotations': {'marketplace.company.io/remote-workflow': 123},
                }
            },
            (
                'The values in iib_organization_customizations.company-marketplace.'
                'csv_annotations must be strings'
            ),
        ),
        (
            {
                'company-marketplace': {
                    'registry_replacements': {'registry.access.company.com': 123}
                }
            },
            (
                'The values in iib_organization_customizations.company-marketplace.'
                'registry_replacements must be strings'
            ),
        ),
        (
            {'company-marketplace': {'package_name_suffix': 123}},
            (
                'The value of iib_organization_customizations.company-marketplace.'
                'package_name_suffix must be a string'
            ),
        ),
    ),
)
def test_validate_celery_config_invalid_organization_customizations(config, error):
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_organization_customizations': config,
        'iib_registry': 'registry',
        'iib_required_labels': {},
    }
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(conf)


@pytest.mark.parametrize(
    'file_type, access, error',
    (
        ('file', True, 'iib_request_logs_dir, {logs_dir}, must exist and be a directory'),
        (None, True, 'iib_request_logs_dir, {logs_dir}, must exist and be a directory'),
        ('dir', False, 'iib_request_logs_dir, {logs_dir}, is not writable!'),
    ),
)
def test_validate_celery_config_request_logs_dir_misconfigured(tmpdir, file_type, access, error):
    iib_request_logs_dir = tmpdir.join('logs')

    if file_type == 'file':
        iib_request_logs_dir.write('')
    elif file_type == 'dir':
        iib_request_logs_dir.mkdir()
    elif file_type is None:
        # Skip creating the file or directory altogether
        pass
    else:
        raise ValueError(f'Bad file_type {file_type}')

    if not access:
        if os.getuid() == 0:
            pytest.skip('Cannot restrict the root user from writing to any file')
        iib_request_logs_dir.chmod(mode=0o555)

    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_organization_customizations': {},
        'iib_request_logs_dir': iib_request_logs_dir,
        'iib_registry': 'registry',
        'iib_required_labels': {},
    }
    error = error.format(logs_dir=iib_request_logs_dir)
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(conf)
