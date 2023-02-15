"""
Configures the Global Tracer Provider and exports the traces to the OpenTelemetry Collector.s
This is a decorator to instruments a function or class.
It takes a function as input and returns a wrapped version of the function
or a class as an input and returns a wrapped class with all the methods except the
private methods wrapped with the instrument_span decorator.
When the wrapped function is called, it creates a new span as the current span
and any child spans created inside the wrapped function will be created as a children of the parent
span using the OpenTelemetry API.

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
import os
from typing import Dict
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.context import attach, detach, set_value
from opentelemetry.propagate import extract, inject
from flask import Flask, request
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Create OTLP span exporter
otlp_exporter = OTLPSpanExporter(
    endpoint="http://otel-collector-http-traces.apps.int.spoke.prod.us-east-1.aws.paas.redhat.com/v1/traces",
)
os.environ["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = "http/protobuf"


def instrument_tracing(
    service_name: str = "",
    span_name: str = "",
    tracer=None,
    ignoreTracing=False,
    attributes: Dict = None,
    is_class=False,
):
    """
    Decorator to instrument a function or class with tracing.
    :param service_name: The name of the service to be used.
    :param span_name: The name of the span to be created.
    :param tracer: The tracer to be used.
    :param ignoreTracing: If True, the function will not be traced.
    :param attributes: The attributes to be added to the span.
    :return: The decorated function or class.
    """

    def instrument_class(cls):
        """
        Filters out all the methods that are to be instrumented
        for a class with tracing.

        :param cls: The class to be decorated.
        :return: The decorated class.
        """
        # methods_to_decorate = []
        for name, method in cls.__dict__.items():
            if (
                callable(method)
                and not method.__name__.startswith("_")
                and not inspect.isclass(method)
            ):
                # methods_to_decorate.append(method)
                setattr(cls, name, instrument_tracing(method, span_name=name))
        return cls

    def instrument_span(func, service_name=None, span_name=None, tracer=None):
        if tracer is None:
            span_exporter = otlp_exporter
            provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name or "iib"}))
            processor = BatchSpanProcessor(span_exporter)
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)
            tracer = trace.get_tracer(__name__)

        # TODO - Extract the trace context from the incoming request
        propagator = TraceContextTextMapPropagator()
        traceparent = os.environ.get("TRACEPARENT")
        context = propagator.extract(carrier={"traceparent": traceparent})

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(
                span_name or func.__name__, kind=SpanKind.SERVER, context=context
            ) as span:
                span.set_attribute("function_name", func.__name__)
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                # result = func(*args, **kwargs)
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    raise
                else:
                    span.set_status(Status(StatusCode.OK))
                finally:
                    # Add the span context from the current span to the link
                    span_id = span.get_span_context().span_id
                    trace_id = span.get_span_context().trace_id
                    traceparent = f"{trace_id}-{span_id}-00"
                    headers = {'traceparent': traceparent}
                    propagator.inject(span.get_span_context(), headers)
                return result

        wrapper = wrapper
        return wrapper

    if ignoreTracing:
        return func_class

    if is_class:
        # The decorator is being used to decorate a function
        return instrument_class
    else:
        # The decorator is being used to decorate a class
        return instrument_span
