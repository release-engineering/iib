# SPDX-License-Identifier: GPL-3.0-or-later
import celery
from celery.signals import celeryd_init

from iib.workers.config import configure_celery, validate_celery_config
# Celery instrumentation
from opentelemetry.instrumentation.celery import CeleryInstrumentor
# Import this file
from celery.signals import worker_process_init

# Add the init_celery_tracing method with its annotation
@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    CeleryInstrumentor().instrument()

app = celery.Celery()
configure_celery(app)
celeryd_init.connect(validate_celery_config)
