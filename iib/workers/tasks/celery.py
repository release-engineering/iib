# SPDX-License-Identifier: GPL-3.0-or-later
import celery
from celery.signals import celeryd_init

from iib.workers.config import configure_celery, validate_celery_config

app = celery.Celery()
configure_celery(app)
celeryd_init.connect(validate_celery_config)
