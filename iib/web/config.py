# SPDX-License-Identifier: GPL-3.0-or-later
import os
import tempfile

TEST_DB_FILE = os.path.join(tempfile.gettempdir(), 'iib.db')


class Config(object):
    """The base IIB Flask configuration."""

    # Additional loggers to set to the level defined in IIB_LOG_LEVEL
    IIB_ADDITIONAL_LOGGERS = []
    IIB_LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s'
    # This sets the level of the "flask.app" logger, which is accessed from current_app.logger
    IIB_LOG_LEVEL = 'INFO'
    IIB_MAX_PER_PAGE = 20
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    """The production IIB Flask configuration."""

    DEBUG = False


class DevelopmentConfig(Config):
    """The development IIB Flask configuration."""

    IIB_LOG_LEVEL = 'DEBUG'
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://iib:iib@db:5432/iib'
    LOGIN_DISABLED = True


class TestingConfig(DevelopmentConfig):
    """The testing IIB Flask configuration."""

    DEBUG = True
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
