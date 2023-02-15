# SPDX-License-Identifier: GPL-3.0-or-later
# Add requests instrumentation
from opentelemetry.instrumentation.requests import RequestsInstrumentor

RequestsInstrumentor().instrument()