# SPDX-License-Identifier: GPL-3.0-or-later
import os

import flask_migrate
import pytest
import tenacity
from unittest import mock

from iib.web import models
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
    """Create an application for the given config name."""
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
    """Yield a DB with required app tables but with no records."""
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


@pytest.fixture(params=['add', 'rm', 'regenerate-bundle'])
def minimal_request(
    request,
    minimal_request_add,
    minimal_request_rm,
    minimal_request_regenerate_bundle,
    minimal_request_recursive_related_bundles,
):
    """
    Create and return an instance of each support request class.

    The request instance will have the minimal set of required attributes set,
    and it'll be committed to the database.

    :param _pytest.fixtures.SubRequest request: the Request subclass to instantiate
    :param pytest.fixture minimal_request_add: instance of RequestAdd
    :param pytest.fixture minimal_request_rm: instance of RequestRm
    :param pytest.fixture minimal_request_regenerate_bundle: instance of RequestRegenerateBundle
    :param pytest.fixture minimal_request_recursive_related_bundles: instance of
        RequestRecursiveRelatedBundles
    :return: yield each request instance
    :rtype: Request
    """
    # A pytest fixture should not be called directly. So we use the fixtures for each
    # request class in this fixture, but then only return one at a time.
    request_instances = {
        'add': minimal_request_add,
        'rm': minimal_request_rm,
        'regenerate-bundle': minimal_request_regenerate_bundle,
        'recursive-related-bundles': minimal_request_recursive_related_bundles,
    }
    return request_instances[request.param]


@pytest.fixture()
def minimal_request_add(db):
    """
    Create and return an instance of the RequestAdd class.

    The request instance will have the minimal set of required attributes set,
    and it'll be committed to the database.

    :param flask_sqlalchemy.SQLAlchemy db: the connection to the database
    :return: the newly created request object
    :rtype: RequestAdd
    """
    binary_image = models.Image(pull_specification='quay.io/add/binary-image:latest')
    db.session.add(binary_image)
    batch = models.Batch()
    db.session.add(batch)
    request = models.RequestAdd(batch=batch, binary_image=binary_image)
    db.session.add(request)
    db.session.commit()
    return request


@pytest.fixture()
def minimal_request_rm(db):
    """
    Create and return an instance of the RequestRm class.

    The request instance will have the minimal set of required attributes set,
    and it'll be committed to the database.

    :param flask_sqlalchemy.SQLAlchemy db: the connection to the database
    :return: the newly created request object
    :rtype: RequestRm
    """
    binary_image = models.Image(pull_specification='quay.io/rm/binary-image:latest')
    db.session.add(binary_image)
    from_index_image = models.Image(pull_specification='quay.io/rm/index-image:latest')
    db.session.add(from_index_image)
    operator = models.Operator(name='operator')
    db.session.add(operator)
    batch = models.Batch()
    db.session.add(batch)
    request = models.RequestRm(
        batch=batch, binary_image=binary_image, from_index=from_index_image, operators=[operator]
    )
    db.session.add(request)
    db.session.commit()
    return request


@pytest.fixture()
def minimal_request_regenerate_bundle(db):
    """
    Create and return an instance of the RequestRegenerateBundle class.

    The request instance will have the minimal set of required attributes set,
    and it'll be committed to the database.

    :param flask_sqlalchemy.SQLAlchemy db: the connection to the database
    :return: the newly created request object
    :rtype: RequestRegenerateBundle
    """
    from_bundle_image = models.Image(pull_specification='quay.io/regen-bundle/bundle-image:latest')
    db.session.add(from_bundle_image)
    batch = models.Batch()
    db.session.add(batch)
    request = models.RequestRegenerateBundle(batch=batch, from_bundle_image=from_bundle_image)
    db.session.add(request)
    db.session.commit()
    return request


@pytest.fixture()
def minimal_request_recursive_related_bundles(db):
    """
    Create and return an instance of the RequestRecursiveRelatedBundles class.

    The request instance will have the minimal set of required attributes set,
    and it'll be committed to the database.

    :param flask_sqlalchemy.SQLAlchemy db: the connection to the database
    :return: the newly created request object
    :rtype: RequestRecursiveRelatedBundles
    """
    parent_bundle_image = models.Image(
        pull_specification='quay.io/parent-bundle/bundle-image:latest'
    )
    db.session.add(parent_bundle_image)
    batch = models.Batch()
    db.session.add(batch)
    request = models.RequestRecursiveRelatedBundles(
        batch=batch, parent_bundle_image=parent_bundle_image
    )
    db.session.add(request)
    db.session.commit()
    return request


@pytest.fixture(scope='session', autouse=True)
def patch_retry():
    with mock.patch.object(tenacity.nap.time, "sleep"):
        yield
