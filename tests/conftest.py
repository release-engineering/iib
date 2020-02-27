# SPDX-License-Identifier: GPL-3.0-or-later
import os

import flask_migrate
import pytest

from iib.web.app import create_app, db as _db
from iib.web.config import TEST_DB_FILE


@pytest.fixture()
def app(request):
    """Return Flask application."""
    return _make_app(request, 'iib.web.config.TestingConfig')


@pytest.fixture()
def app_no_auth(request):
    """Return Flask application without authentication."""
    return _make_app(request, 'iib.web.config.TestingConfigNoAuth')


def _make_app(request, config):
    """Helper method to create an application for the given config name"""
    app = create_app(config)
    # Establish an application context before running the tests. This allows the use of
    # Flask-SQLAlchemy in the test setup.
    ctx = app.app_context()
    ctx.push()

    def teardown():
        ctx.pop()

    request.addfinalizer(teardown)
    return app


@pytest.fixture(scope='session')
def auth_env():
    return {'REMOTE_USER': 'tbrady@DOMAIN.LOCAL'}


@pytest.fixture()
def db(app, tmpdir):
    """Yields a DB with required app tables but with no records."""
    # Clear the database for each test to ensure tests are idempotent.
    try:
        os.remove(TEST_DB_FILE)
    except FileNotFoundError:
        pass

    with app.app_context():
        flask_migrate.upgrade()

    return _db


@pytest.fixture()
def client(app):
    """Return Flask application client for the pytest session."""
    return app.test_client()


@pytest.fixture(scope='session')
def worker_auth_env():
    return {'REMOTE_USER': 'worker@DOMAIN.LOCAL'}


@pytest.fixture(scope='session')
def worker_forbidden_env():
    return {'REMOTE_USER': 'vkohli@DOMAIN.LOCAL'}
