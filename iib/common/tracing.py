# SPDX-License-Identifier: GPL-3.0-or-later

"""Configures the Global Tracer Provider and exports the traces to the OpenTelemetry Collector.

The OpenTelemetry Collector is configured to receive traces via OTLP over HTTP.
The OTLP exporter is configured to use the environment variables
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT and OTEL_EXPORTER_OTLP_TRACES_HEADERS to
configure the endpoint and headers for the OTLP exporter.
The OTLP* environment variables are configured in the docker-compose.yaml
and podman-compose.yaml files for iib workers and api.

Usage:
    @instrument_tracing()
    def func():
        pass

    @instrument_tracing()
    class MyClass:
        def func1():
            pass
        def _func2():
            pass

"""
import functools
import inspect
import logging
import os
from typing import Dict
from opentelemetry import trace
from opentelemetry.trace import Tracer
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


log = logging.getLogger(__name__)
os.environ["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = "http/protobuf"
propagator = TraceContextTextMapPropagator()


class TracingWrapper:
    """Wrapper class that will wrap all methods of a calls with the instrument_tracing decorator."""

    __instance = None

    def __new__(cls):
        """Create a new instance if one does not exist."""
        if TracingWrapper.__instance is None:
            log.info("Creating TracingWrapper instance")
            cls.__instance = super().__new__(cls)
            otlp_exporter = OTLPSpanExporter(
                endpoint="http://otel-collector-http-traces.apps.int.spoke.prod.us-east-1.aws.paas.redhat.com/v1/traces",  # noqa: E501
            )
            cls.provider = TracerProvider(
                resource=Resource.create({SERVICE_NAME: "iib-auto-manual"})
            )
            cls.processor = BatchSpanProcessor(otlp_exporter)
            cls.provider.add_span_processor(cls.processor)
            trace.set_tracer_provider(cls.provider)
            set_global_textmap(propagator)
            cls.tracer = trace.get_tracer(__name__)
        return cls.__instance


def instrument_tracing(
    func=None,
    *,
    service_name: str = "",
    span_name: str = "",
    ignoreTracing=False,
    attributes: Dict = {},
    existing_tracer: Tracer = None,
    is_class=False,
):
    """Instrument tracing for a function or class.

    :param func_or_class: The function or class to be decorated.
    :param service_name: The name of the service to be used.
    :param span_name: The name of the span to be created.
    :param ignoreTracing: If True, the function will not be traced.
    :param attributes: The attributes to be added to the span.
    :param existing_tracer: The tracer to be used.
    :return: The decorated function or class.
    """

    def instrument_class(cls):
        """Instruments class and filters out all the methods of a class that are to be instrumented.

        :param cls: The class to be decorated.
        :return: The decorated class.
        """
        for name, method in cls.__dict__.items():
            if (
                callable(method)
                and not method.__name__.startswith("_")
                and not inspect.isclass(method)
            ):
                setattr(cls, name, instrument_tracing(method))
        return cls

    def instrument_span(func):
        log.info(f"Instrumenting span for {span_name}")
        tracer = trace.get_tracer(__name__)
        context = None
        if trace.get_current_span():
            context = trace.get_current_span().get_span_context()
        else:
            context = propagator.extract(carrier={})

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log.info(f"Context inside {span_name}: {context}")
            if kwargs.get("traceparent"):
                log.info(f"traceparent is {kwargs.get('traceparent')}")
                carrier = {"traceparent": kwargs.get("traceparent")}
                trace_context = propagator.extract(carrier)
                log.info(f"Context is {trace_context}")
            with tracer.start_as_current_span(
                span_name or func.__name__, kind=SpanKind.SERVER
            ) as span:
                # span.set_attribute("function_name", func.__name__)
                if func.__name__:  # If the function has a name
                    log.info(f"function_name {func.__name__}")
                    span.set_attribute("function_name", func.__name__)
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    raise
                else:
                    if result:
                        log.info(f"result {result}")
                        span.set_attribute("result_attributes", result)
                    if args:
                        log.info(f"arguments {args}")
                        span.set_attribute("arguments", args)
                    if kwargs:
                        # Need to handle all the types of kwargs
                        if type(kwargs) == dict:
                            for keys, values in kwargs.items():
                                if keys == 'context':
                                    continue
                                if type(values) is dict:
                                    for key, value in values.items():
                                        log.info(f"Values is dict {key}, {value}")
                                        span.set_attribute(key, value)
                                elif type(values) is list:
                                    for value in values:
                                        if type(value) is dict:
                                            for k, v in value.items():
                                                log.info(
                                                    f"Values is list, and value is dict => {k}, {v}"
                                                )
                                                span.set_attribute(k, v)
                                        else:
                                            log.info(f"Values is list, else => {value}")
                                            span.set_attribute(keys, value)
                                else:
                                    span.set_attribute(keys, values)
                        else:
                            if kwargs["task_id"]:
                                log.info(f"task_id {kwargs['task_id']}")
                                span.set_attribute("task_id", kwargs["task_id"])
                            if kwargs["task_name"]:
                                log.info(f"task_name {kwargs['task_name']}")
                                span.set_attribute("task_name", kwargs["task_name"])
                            if kwargs["task_type"]:
                                log.info(f"task_type {kwargs['task_type']}")
                                span.set_attribute("task_type", kwargs["task_type"])
                    if func.__doc__:
                        log.info(f"Description is  {func.__doc__}")
                        span.set_attribute("description", func.__doc__)
                    span.add_event(f"{func.__name__} executed", {"result": result or "success"})
                    span.set_status(Status(StatusCode.OK))
                finally:
                    # Add the span context from the current span to the link
                    span_id = span.get_span_context().span_id
                    trace_id = span.get_span_context().trace_id
                    # Syntax of traceparent is f"00-{trace_id}-{span_id}-01"
                    traceparent = f"00-{trace_id}-{span_id}-01"
                    headers = {'traceparent': traceparent}
                    propagator.inject(headers)
                    log.info("Headers are: %s", headers)
                    set_span_in_context(span)

                return result

        wrapper = wrapper
        return wrapper

    if ignoreTracing:
        return func

    if is_class:
        # The decorator is being used to decorate a function
        return instrument_class
    else:
        # The decorator is being used to decorate a class
        return instrument_span
