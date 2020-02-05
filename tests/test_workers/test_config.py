# SPDX-License-Identifier: GPL-3.0-or-later
from unittest.mock import patch
from io import BytesIO

import celery

from iib.workers.config import configure_celery


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
