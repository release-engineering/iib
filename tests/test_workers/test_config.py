# SPDX-License-Identifier: GPL-3.0-or-later
from unittest.mock import patch
from unittest import mock
from io import BytesIO
import os
import re

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
    assert celery_app.conf.broker_connection_max_retries == 10


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
    assert celery_app.conf.broker_connection_max_retries == 10


@mock.patch.dict(os.environ, {'IIB_OTEL_TRACING': 'some-str'})
@patch('os.path.isdir', return_value=True)
@patch('os.access', return_value=True)
def test_validate_celery_config(mock_isdir, mock_isaccess):
    validate_celery_config(
        {
            'iib_api_url': 'http://localhost:8080/api/v1/',
            'iib_organization_customizations': {},
            'iib_registry': 'registry',
            'iib_required_labels': {},
            'iib_request_recursive_related_bundles_dir': 'some-dire',
            'iib_ocp_opm_mapping': {},
            'iib_default_opm': 'opm',
        }
    )


@pytest.mark.parametrize('missing_key', ('iib_api_url', 'iib_registry'))
def test_validate_celery_config_failure(missing_key):
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    conf.pop(missing_key)
    with pytest.raises(ConfigError, match=f'{missing_key} must be set'):
        validate_celery_config(conf)


def test_validate_celery_config_iib_required_labels_not_dict():
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': 123,
        'iib_default_opm': 'opm',
        'iib_ocp_opm_mapping': {},
    }
    with pytest.raises(ConfigError, match='iib_required_labels must be a dictionary'):
        validate_celery_config(conf)


def test_validate_celery_config_iib_replace_registry_not_dict():
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_related_image_registry_replacement': 123,
        'iib_default_opm': 'opm',
        'iib_ocp_opm_mapping': {},
        'iib_required_labels': {},
    }
    with pytest.raises(
        ConfigError, match='iib_related_image_registry_replacement must be a dictionary'
    ):
        validate_celery_config(conf)


@pytest.mark.parametrize(
    'config, error',
    (
        ('Do or do not. There is no try.', 'iib_organization_customizations must be a dictionary'),
        ({123: []}, 'The org keys in iib_organization_customizations must be strings'),
        (
            {'company-marketplace': 123},
            'The org values in iib_organization_customizations must be a list',
        ),
        (
            {'company-marketplace': [['OathBreaker']]},
            'Every customization for an org in iib_organization_customizations must be dictionary',
        ),
        (
            {'company-marketplace': [{'Yoda': 'Do or do not. There is no try.'}]},
            (
                'Invalid customization in iib_organization_customizations '
                '{\'Yoda\': \'Do or do not. There is no try.\'}'
            ),
        ),
        (
            {
                'company-marketplace': [
                    {
                        'type': 'csv_annotations',
                        'annotations': {
                            123: (
                                'https://marketplace.company.com/en-us'
                                '/operators/{package_name}/pricing'
                            ),
                        },
                        'invalid_key': 'Something invalid is here.',
                    }
                ]
            },
            re.escape(
                'The keys {\'invalid_key\'} in iib_organization_customizations'
                '.company-marketplace[0] are invalid.'
            ),
        ),
        (
            {
                'company-marketplace': [
                    {
                        'type': 'csv_annotations',
                        'annotations': {
                            123: (
                                'https://marketplace.company.com/en-us/operators'
                                '/{package_name}/pricing'
                            ),
                        },
                    }
                ]
            },
            re.escape(
                'The keys in iib_organization_customizations.company-marketplace[0].annotations'
                ' must be strings'
            ),
        ),
        (
            {
                'company-marketplace': [
                    {
                        'type': 'registry_replacements',
                        'replacements': {123: 'registry.marketplace.company.com/cm'},
                    }
                ]
            },
            re.escape(
                'The keys in iib_organization_customizations.company-marketplace[0].'
                'replacements must be strings'
            ),
        ),
        (
            {
                'company-marketplace': [
                    {'type': 'registry_replacements', 'replacements': {'something': 123}}
                ]
            },
            re.escape(
                'The values in iib_organization_customizations.company-marketplace[0].'
                'replacements must be strings'
            ),
        ),
        (
            {
                'company-marketplace': [
                    {'type': 'package_name_suffix', 'suffix': {'something': 123}}
                ]
            },
            re.escape(
                'The value of iib_organization_customizations.company-marketplace[0].'
                'suffix must be a string'
            ),
        ),
        (
            {'company-marketplace': [{'type': 'image_name_from_labels', 'template': 12345}]},
            re.escape(
                'The value of iib_organization_customizations.company-marketplace[0].'
                'template must be a string'
            ),
        ),
        (
            {
                'company-marketplace': [
                    {'type': 'enclose_repo', 'namespace': 'something', 'enclosure_glue': 123}
                ]
            },
            re.escape(
                'The value of iib_organization_customizations.company-marketplace[0].'
                'enclosure_glue must be a string'
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
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(conf)


@pytest.mark.parametrize(
    'file_type, access, error',
    (
        ('file', True, 'iib_request_logs_dir must exist and be a directory'),
        (None, True, 'iib_request_logs_dir must exist and be a directory'),
        ('dir', False, 'iib_request_logs_dir, is not writable!'),
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
        'iib_request_recursive_related_bundles_dir': 'some-dir',
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    error = error.format(logs_dir=iib_request_logs_dir)
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(conf)


@pytest.mark.parametrize(
    'config, error',
    (
        (
            {'iib_aws_s3_bucket_name': 'bucket'},
            (
                '"iib_request_logs_dir", "iib_request_related_bundles_dir" and '
                '"iib_request_recursive_related_bundles_dir" must be set when '
                'iib_aws_s3_bucket_name is set.'
            ),
        ),
        (
            {'iib_aws_s3_bucket_name': 123, 'iib_request_logs_dir': 'some-dir'},
            (
                '"iib_aws_s3_bucket_name" must be set to a valid string. '
                'This is used for read/write access to the s3 bucket by IIB'
            ),
        ),
    ),
)
def test_validate_celery_config_invalid_s3_config(config, error):
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_organization_customizations': {},
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    worker_config = {**conf, **config}
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(worker_config)


@patch.dict(os.environ, {'AWS_ACCESS_KEY_ID': 'some_key', 'AWS_SECRET_ACCESS_KEY': 'some_secret'})
def test_validate_celery_config_invalid_s3_env_vars():
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_organization_customizations': {},
        'iib_aws_s3_bucket_name': 'bucket',
        'iib_request_logs_dir': 'some-dir',
        'iib_request_related_bundles_dir': 'some-other-dir',
        'iib_request_recursive_related_bundles_dir': 'yet-antoher-dir',
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    error = (
        '"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY" and "AWS_DEFAULT_REGION" '
        'environment variables must be set to valid strings when'
        '"iib_aws_s3_bucket_name" is set. '
        'These are used for read/write access to the s3 bucket by IIB'
    )
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(conf)


@mock.patch.dict(os.environ, {'IIB_OTEL_TRACING': 'True'})
def test_validate_celery_config_invalid_otel_config(tmpdir):
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_organization_customizations': {},
        'iib_request_recursive_related_bundles_dir': tmpdir.join('some-dir'),
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    error = (
        '"OTEL_EXPORTER_OTLP_ENDPOINT" and "OTEL_SERVICE_NAME" environment '
        'variables must be set to valid strings when "IIB_OTEL_TRACING" is set to True.'
    )
    iib_request_recursive_related_bundles_dir = conf['iib_request_recursive_related_bundles_dir']
    iib_request_recursive_related_bundles_dir.mkdir()
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(conf)


def test_validate_celery_config_invalid_recursive_related_bundles_config():
    worker_config = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_organization_customizations': {},
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }
    error = (
        '"iib_request_recursive_related_bundles_dir" must be set when'
        ' "iib_aws_s3_bucket_name" is not set'
    )
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(worker_config)


def test_validate_celery_config_invalid_iib_no_ocp_label_allow_list():
    worker_config = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_no_ocp_label_allow_list': [''],
        'iib_ocp_opm_mapping': {},
        'iib_default_opm': 'opm',
    }

    error = 'Empty string is not allowed in iib_no_ocp_label_allow_list'
    with pytest.raises(ConfigError, match=error):
        validate_celery_config(worker_config)


def test_validate_celery_config_iib_opm_ocp_mapping_incorrect_type():
    worker_config = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_ocp_opm_mapping': 'incorrect_value',
        'iib_default_opm': 'opm',
    }
    with pytest.raises(ConfigError, match='iib_ocp_opm_mapping must be a dictionary'):
        validate_celery_config(worker_config)


@patch('shutil.which', return_value=None)
def test_validate_celery_config_iib_opm_ocp_mapping_opm_not_exist(mock_pe, tmpdir):
    worker_config = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_required_labels': {},
        'iib_organization_customizations': {},
        'iib_default_opm': 'opm',
        'iib_ocp_opm_mapping': {
            'v4.14': 'opm-not-exist',
        },
    }
    with pytest.raises(ConfigError, match='opm-not-exist is not installed'):
        validate_celery_config(worker_config)
