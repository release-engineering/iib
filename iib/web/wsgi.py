# SPDX-License-Identifier: GPL-3.0-or-later
from iib.web.app import create_app
from flask import Flask
#from opentelemetry.instrumentation.wsgi import OpenTelemetryMiddleware
#from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
#from opentelemetry.trace import get_tracer_provider, set_tracer_provider
#from iib.common.tracing import instrument_tracing, get_tracer_provider

#from opentelemetry.instrumentation.flask import FlaskInstrumentor

app = create_app()
#FlaskInstrumentor().instrument_app(app)
#app.wsgi_app = OpenTelemetryMiddleware(app.wsgi_app)
#SQLAlchemyInstrumentor().instrument(enable_commenter=True, commenter_options={})
