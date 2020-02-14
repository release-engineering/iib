# SPDX-License-Identifier: GPL-3.0-or-later
from unittest.mock import patch
from io import BytesIO

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
            'iib_registry': 'registry',
            'iib_registry_credentials': 'username:password',
        }
    )


@pytest.mark.parametrize('missing_key', ('iib_api_url', 'iib_registry', 'iib_registry_credentials'))
def test_validate_celery_config_failure(missing_key):
    conf = {
        'iib_api_url': 'http://localhost:8080/api/v1/',
        'iib_registry': 'registry',
        'iib_registry_credentials': 'username:password',
    }
    conf.pop(missing_key)
    with pytest.raises(ConfigError, match=f'{missing_key} must be set'):
        validate_celery_config(conf)
