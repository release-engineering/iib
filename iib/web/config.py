# SPDX-License-Identifier: GPL-3.0-or-later
import os
import tempfile
from typing import Dict, List, Optional, Union

TEST_DB_FILE = os.path.join(tempfile.gettempdir(), 'iib_recursive.db')


def _get_empty_dict_str_str() -> Dict[str, str]:
    # solving mypy error: Incompatible types in assignment
    # (expression has type "Dict[<nothing>, <nothing>]",
    # variable has type "Union[Dict[str, str], Dict[str, Dict[str, str]]]")
    return {}


class Config(object):
    """The base IIB Flask configuration."""

    # Additional loggers to set to the level defined in IIB_LOG_LEVEL
    IIB_ADDITIONAL_LOGGERS: List[str] = []
    IIB_AWS_S3_BUCKET_NAME: Optional[str] = None
    IIB_BINARY_IMAGE_CONFIG: Dict[str, Dict[str, str]] = {}
    IIB_GRAPH_MODE_INDEX_ALLOW_LIST: List[str] = []
    IIB_GRAPH_MODE_OPTIONS: List[str] = ['replaces', 'semver', 'semver-skippatch']
    IIB_GREENWAVE_CONFIG: Dict[str, str] = {}
    IIB_LOG_FORMAT: str = '%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s'
    # This sets the level of the "flask.app" logger, which is accessed from current_app.logger
    IIB_LOG_LEVEL: str = 'INFO'
    IIB_MAX_PER_PAGE: int = 20
    IIB_MESSAGING_CA: str = '/etc/pki/tls/certs/ca-bundle.crt'
    IIB_MESSAGING_CERT: str = '/etc/iib/messaging.crt'
    IIB_MESSAGING_DURABLE: bool = True
    IIB_MESSAGING_KEY: str = '/etc/iib/messaging.key'
    IIB_MESSAGING_TIMEOUT: int = 30
    IIB_REQUEST_DATA_DAYS_TO_LIVE: int = 3
    IIB_REQUEST_LOGS_DIR: Optional[str] = None
    IIB_REQUEST_RELATED_BUNDLES_DIR: Optional[str] = None
    IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR: Optional[str] = None
    IIB_USER_TO_QUEUE: Union[Dict[str, str], Dict[str, Dict[str, str]]] = _get_empty_dict_str_str()
    IIB_WORKER_USERNAMES: List[str] = []
    SQLALCHEMY_TRACK_MODIFICATIONS: bool = False


class ProductionConfig(Config):
    """The production IIB Flask configuration."""

    DEBUG: bool = False


class DevelopmentConfig(Config):
    """The development IIB Flask configuration."""

    IIB_LOG_LEVEL: str = 'DEBUG'
    IIB_MESSAGING_BATCH_STATE_DESTINATION: str = 'topic://VirtualTopic.eng.iib.batch.state'
    IIB_MESSAGING_BUILD_STATE_DESTINATION: str = 'topic://VirtualTopic.eng.iib.build.state'
    IIB_MESSAGING_CA: str = '/etc/iib/messaging-ca.crt'
    IIB_MESSAGING_URLS: List[str] = ['amqps://message-broker:5671']
    IIB_REQUEST_LOGS_DIR: str = '/var/log/iib/requests'
    IIB_REQUEST_RELATED_BUNDLES_DIR: str = '/var/lib/requests/related_bundles'
    IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR: str = '/var/lib/requests/recursive_related_bundles'
    SQLALCHEMY_DATABASE_URI: str = 'postgresql+psycopg2://iib:iib@db:5432/iib'
    LOGIN_DISABLED: bool = True


class TestingConfig(DevelopmentConfig):
    """The testing IIB Flask configuration."""

    DEBUG: bool = True
    IIB_WORKER_USERNAMES: List[str] = ['worker@DOMAIN.LOCAL']
    # IMPORTANT: don't use in-memory sqlite. Alembic migrations will create a new
    # connection producing a new instance of the database which is deleted immediately
    # after the migration completes...
    #   https://github.com/miguelgrinberg/Flask-Migrate/issues/153
    SQLALCHEMY_DATABASE_URI: str = f'sqlite:///{TEST_DB_FILE}'
    LOGIN_DISABLED: bool = False


class TestingConfigNoAuth(TestingConfig):
    """The testing IIB Flask configuration without authentication."""

    # This is needed because Flask seems to read the LOGIN_DISABLED setting
    # and configure the relevant extensions at app creation time. Changing this
    # during a test run still leaves login enabled. This behavior also applies
    # to ENV and DEBUG config values:
    #   https://flask.palletsprojects.com/en/1.1.x/config/#environment-and-debug-features
    LOGIN_DISABLED: bool = True
