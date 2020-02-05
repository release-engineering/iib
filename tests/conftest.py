# SPDX-License-Identifier: GPL-3.0-or-later
import pytest

from iib.web.app import create_app


def _make_app(request, config):
    """Helper method to create an application for the given config name"""
    app = create_app(config)
    return app


@pytest.fixture()
def app(request):
    """Return Flask application for the pytest session."""
    return _make_app(request, 'iib.web.config.TestingConfig')


@pytest.fixture()
def client(app):
    """Return Flask application client for the pytest session."""
    return app.test_client()
