# SPDX-License-Identifier: GPL-3.0-or-later
import logging
from typing import Any

import celery.app.task

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.celery import app
from iib.workers.tasks.utils import request_logger

__all__ = ['failed_request_callback', 'set_request_state']

log = logging.getLogger(__name__)


@app.task
@request_logger
def failed_request_callback(
    context: celery.app.task.Context,
    exc: Exception,
    traceback: Any,
    request_id: int,
) -> None:
    """
    Wrap set_request_state for task error callbacks.

    :param celery.app.task.Context context: the context of the task failure
    :param Exception exc: the exception that caused the task failure
    :param int request_id: the ID of the IIB request
    """
    if isinstance(exc, IIBError):
        msg = str(exc)
    else:
        msg = 'An unknown error occurred. See logs for details'
        log.error(msg, exc_info=exc)

    set_request_state(request_id, 'failed', msg)
