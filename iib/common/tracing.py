# SPDX-License-Identifier: GPL-3.0-or-later

"""Configures the Global Tracer Provider and exports the traces to the OpenTelemetry Collector.

The OpenTelemetry Collector is configured to receive traces via OTLP over HTTP.
The OTLP exporter is configured to use the environment variables defined in the ansible playbook.

Usage:
    @instrument_tracing()
      def func():
          pass

"""
import json
import os
import functools
import getpass
import logging
import socket
from copy import deepcopy
from typing import Any, Dict


from flask import Response
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation import (
    set_span_in_context,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.util.types import Attributes


log = logging.getLogger(__name__)
propagator = TraceContextTextMapPropagator()


def normalize_data_for_span(data: Dict[str, Any]) -> Attributes:
    """
    Normalize any dictionary to a open-telemetry usable dictionary.

    :param Dict[str, Any] data: The dictionary to be converted.
    :return: Normalized dictionary.
    :rtype: Attributes
    """
    span_data = deepcopy(data)
    for key, value in span_data.items():
        if type(value) in [type(None), dict, list]:
            span_data[key] = str(value)
    return span_data


class TracingWrapper:
    """Wrapper class that will wrap all methods of calls with the instrument_tracing decorator."""

    __instance = None

    def __new__(cls):
        """Create a new instance if one does not exist."""
        if not os.getenv('IIB_OTEL_TRACING', '').lower() == 'true':
            return None

        if TracingWrapper.__instance is None:
            log.info('Creating TracingWrapper instance')
            cls.__instance = super().__new__(cls)
            otlp_exporter = OTLPSpanExporter(
                endpoint=f"{os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT')}/v1/traces",
            )
            cls.provider = TracerProvider(
                resource=Resource.create({SERVICE_NAME: os.getenv('OTEL_SERVICE_NAME')})
            )
            cls.processor = BatchSpanProcessor(otlp_exporter)
            cls.provider.add_span_processor(cls.processor)
            trace.set_tracer_provider(cls.provider)
            set_global_textmap(propagator)
            cls.tracer = trace.get_tracer(__name__)
        return cls.__instance


def instrument_tracing(
    span_name: str = '',
    attributes: Dict = {},
):
    """
    Instrument tracing for a function.

    :param span_name: The name of the span to be created.
    :param attributes: The attributes to be added to the span.
    :return: The decorated function or class.
    """

    def decorator_instrument_tracing(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not os.getenv('IIB_OTEL_TRACING', '').lower() == 'true':
                return func(*args, **kwargs)

            log.info('Instrumenting span for %s', span_name)
            tracer = trace.get_tracer(__name__)
            if trace.get_current_span():
                context = trace.get_current_span().get_span_context()
            else:
                context = propagator.extract(carrier={})

            log.debug('Context inside %s: %s', span_name, context)
            if kwargs.get('traceparent'):
                log.debug('traceparent is %s' % str(kwargs.get('traceparent')))
                carrier = {'traceparent': kwargs.get('traceparent')}
                trace_context = propagator.extract(carrier)
                log.debug('Context is %s', trace_context)
            with tracer.start_as_current_span(
                span_name or func.__name__, kind=SpanKind.SERVER
            ) as span:
                for attr in attributes:
                    span.set_attribute(attr, attributes[attr])
                span.set_attribute('host', socket.getfqdn())
                span.set_attribute('user', getpass.getuser())

                if func.__name__:  # If the function has a name
                    log.debug('function_name %s', func.__name__)
                    span.set_attribute('function_name', func.__name__)
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, dict):
                        span_result = normalize_data_for_span(result)
                    elif isinstance(result, tuple) and isinstance(result[0], Response):
                        response = json.dumps(result[0].json)
                        code = result[1]
                        span_result = {'response': response, 'http_code': code}
                    else:
                        # If the returned result is not of type dict, create one
                        span_result = {'result': str(result) or 'success'}
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    raise
                else:
                    if span_result:
                        log.debug('result %s', span_result)
                        span.set_attributes(span_result)
                    if kwargs:
                        # Need to handle all the types of kwargs
                        if "task_id" in kwargs:
                            log.debug('task_id is %s' % kwargs['task_id'])
                            span.set_attribute('task_id', kwargs['task_id'])
                        if "task_name" in kwargs:
                            log.debug('task_name is %s' % kwargs['task_name'])
                            span.set_attribute('task_name', kwargs['task_name'])
                        if "task_type" in kwargs:
                            log.debug('task_type is %s' % kwargs['task_type'])
                            span.set_attribute('task_type', kwargs['task_type'])
                    span.add_event(f'{func.__name__} executed', span_result)
                    span.set_status(Status(StatusCode.OK))
                finally:
                    # Add the span context from the current span to the link
                    span_id = span.get_span_context().span_id
                    trace_id = span.get_span_context().trace_id
                    # Syntax of traceparent is f'00-{trace_id}-{span_id}-01'
                    traceparent = f'00-{trace_id}-{span_id}-01'
                    headers = {'traceparent': traceparent}
                    propagator.inject(headers)
                    log.debug('Headers are: %s', headers)
                    set_span_in_context(span)

                return result

        return wrapper

    return decorator_instrument_tracing
