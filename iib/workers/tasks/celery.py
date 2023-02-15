# SPDX-License-Identifier: GPL-3.0-or-later
import celery
from celery.signals import celeryd_init
from iib.workers.config import configure_celery, validate_celery_config
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from celery.signals import worker_process_init
from iib.common.tracing import TracingWrapper

tracerWrapper = TracingWrapper()


app = celery.Celery()
configure_celery(app)
celeryd_init.connect(validate_celery_config)

RequestsInstrumentor().instrument(trace_provider=tracerWrapper.provider)


# Add the init_celery_tracing method with its annotation
@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    """Initialize the tracing for celery."""
    CeleryInstrumentor().instrument(trace_provider=tracerWrapper.provider)
