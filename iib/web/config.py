# SPDX-License-Identifier: GPL-3.0-or-later
import os
import tempfile

TEST_DB_FILE = os.path.join(tempfile.gettempdir(), 'iib.db')


class Config(object):
    """The base IIB Flask configuration."""

    # Additional loggers to set to the level defined in IIB_LOG_LEVEL
    IIB_ADDITIONAL_LOGGERS = []
    IIB_FORCE_OVERWRITE_FROM_INDEX = False
    IIB_GREENWAVE_CONFIG = {}
    IIB_LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s'
    # This sets the level of the "flask.app" logger, which is accessed from current_app.logger
    IIB_LOG_LEVEL = 'INFO'
    IIB_MAX_PER_PAGE = 20
    IIB_MESSAGING_CA = '/etc/pki/tls/certs/ca-bundle.crt'
    IIB_MESSAGING_CERT = '/etc/iib/messaging.crt'
    IIB_MESSAGING_DURABLE = True
    IIB_MESSAGING_KEY = '/etc/iib/messaging.key'
    IIB_MESSAGING_TIMEOUT = 30
    IIB_PRIVILEGED_USERNAMES = []
    IIB_REQUEST_LOGS_DIR = None
    IIB_REQUEST_LOGS_DAYS_TO_LIVE = 3
    IIB_USER_TO_QUEUE = {}
    IIB_WORKER_USERNAMES = []
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    """The production IIB Flask configuration."""

    DEBUG = False


class DevelopmentConfig(Config):
    """The development IIB Flask configuration."""

    IIB_LOG_LEVEL = 'DEBUG'
    IIB_MESSAGING_BATCH_STATE_DESTINATION = 'topic://VirtualTopic.eng.iib.batch.state'
    IIB_MESSAGING_BUILD_STATE_DESTINATION = 'topic://VirtualTopic.eng.iib.build.state'
    IIB_MESSAGING_CA = '/etc/iib/messaging-ca.crt'
    IIB_MESSAGING_URLS = ['amqps://message-broker:5671']
    IIB_REQUEST_LOGS_DIR = '/var/log/iib/requests'
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://iib:iib@db:5432/iib'
    LOGIN_DISABLED = True


class TestingConfig(DevelopmentConfig):
    """The testing IIB Flask configuration."""

    DEBUG = True
    IIB_PRIVILEGED_USERNAMES = ['tbrady@DOMAIN.LOCAL']
    IIB_WORKER_USERNAMES = ['worker@DOMAIN.LOCAL']
    # IMPORTANT: don't use in-memory sqlite. Alembic migrations will create a new
    # connection producing a new instance of the database which is deleted immediately
    # after the migration completes...
    #   https://github.com/miguelgrinberg/Flask-Migrate/issues/153
    SQLALCHEMY_DATABASE_URI = f'sqlite:///{TEST_DB_FILE}'
    LOGIN_DISABLED = False


class TestingConfigNoAuth(TestingConfig):
    """The testing IIB Flask configuration without authentication."""

    # This is needed because Flask seems to read the LOGIN_DISABLED setting
    # and configure the relevant extensions at app creation time. Changing this
    # during a test run still leaves login enabled. This behavior also applies
    # to ENV and DEBUG config values:
    #   https://flask.palletsprojects.com/en/1.1.x/config/#environment-and-debug-features
    LOGIN_DISABLED = True
