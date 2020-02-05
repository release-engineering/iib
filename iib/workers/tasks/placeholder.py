# SPDX-License-Identifier: GPL-3.0-or-later
import logging

from iib.workers.tasks.celery import app


__all__ = ['ping']
log = logging.getLogger(__name__)


@app.task
def ping():
    log.debug('Ping')
