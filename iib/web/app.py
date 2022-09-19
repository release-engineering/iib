# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os

from flask import Flask
from flask.logging import default_handler
from flask_login import LoginManager
from flask_migrate import Migrate
import kombu.exceptions
from kombu import Queue
from werkzeug.exceptions import default_exceptions

from iib.exceptions import ConfigError, IIBError, ValidationError
from iib.web import db
from iib.web.api_v1 import api_v1
from iib.web.auth import user_loader, load_user_from_request
from iib.web.docs import docs
from iib.web.errors import json_error

# Import the models here so that Alembic will be guaranteed to detect them
import iib.web.models  # noqa: F401


def load_config(app):
    """
    Determine the correct configuration to use and apply it.

    :param flask.Flask app: a Flask application object
    """
    config_file = None
    if os.getenv('IIB_DEV', '').lower() == 'true':
        default_config_obj = 'iib.web.config.DevelopmentConfig'
    elif os.getenv('IIB_TESTING', '').lower() == 'true':
        default_config_obj = 'iib.web.config.TestingConfig'
    else:
        default_config_obj = 'iib.web.config.ProductionConfig'
        config_file = '/etc/iib/settings.py'
    app.config.from_object(default_config_obj)

    if config_file and os.path.isfile(config_file):
        app.config.from_pyfile(config_file)

    if app.config['IIB_SAC_QUEUES']:
        app.config.conf.task_queues = [
            Queue(qname, queue_arguments={'x-single-active-consumer': True})
            for qname in app.config['IIB_SAC_QUEUES']
        ]


def validate_api_config(config):
    """
    Determine if the configuration is valid.

    :param dict config: the dict containing the IIB REST API config
    :raises ConfigError: if the config is invalid
    """
    if config['IIB_GREENWAVE_CONFIG']:
        defined_queue_names = set(config['IIB_USER_TO_QUEUE'].values())
        invalid_greenwave_queues = set(config['IIB_GREENWAVE_CONFIG'].keys()) - defined_queue_names
        # The queue_name `None` is the configuration for the default Celery queue
        invalid_greenwave_queues.discard(None)
        if invalid_greenwave_queues:
            raise ConfigError(
                f'The following queues are invalid in "IIB_GREENWAVE_CONFIG"'
                f': {", ".join(invalid_greenwave_queues)}'
            )

        required_params = {'decision_context', 'product_version', 'subject_type'}
        for queue_name, greenwave_config in config['IIB_GREENWAVE_CONFIG'].items():
            defined_params = set(greenwave_config.keys())

            missing_params = required_params - defined_params
            if missing_params:
                raise ConfigError(
                    f'Missing required params {", ".join(missing_params)} for queue {queue_name} '
                    'in "IIB_GREENWAVE_CONFIG"'
                )

            invalid_params = defined_params - required_params
            if invalid_params:
                raise ConfigError(
                    f'Invalid params {", ".join(invalid_params)} for queue {queue_name} '
                    'in "IIB_GREENWAVE_CONFIG"'
                )

            if greenwave_config['subject_type'] != 'koji_build':
                raise ConfigError(
                    'IIB only supports gating for subject_type "koji_build". Invalid subject_type '
                    f'{greenwave_config["subject_type"]} defined for queue '
                    f'{queue_name} in "IIB_GREENWAVE_CONFIG"'
                )

    if config['IIB_BINARY_IMAGE_CONFIG']:
        if not isinstance(config['IIB_BINARY_IMAGE_CONFIG'], dict):
            raise ConfigError(
                'IIB_BINARY_IMAGE_CONFIG must be a dict mapping distribution_scope to '
                'another dict mapping ocp_version to binary_image'
            )
        for distribution_scope, value_dict in config['IIB_BINARY_IMAGE_CONFIG'].items():
            if not isinstance(distribution_scope, str) or distribution_scope not in (
                'dev',
                'stage',
                'prod',
            ):
                raise ConfigError(
                    'distribution_scope values must be one of the following'
                    ' "prod", "stage" or "dev" strings.'
                )
            if not isinstance(value_dict, dict):
                raise ConfigError(
                    'Value for distribution_scope keys must be a dict mapping'
                    ' ocp_version to binary_image'
                )
            for ocp_version, binary_image_value in value_dict.items():
                if not isinstance(ocp_version, str) or not isinstance(binary_image_value, str):
                    raise ConfigError('All ocp_version and binary_image values must be strings.')

    if (
        not config['IIB_AWS_S3_BUCKET_NAME']
        and not config['IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR']
    ):
        raise ConfigError(
            'One of "IIB_AWS_S3_BUCKET_NAME" or "IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR"'
            ' must be set'
        )

    if config['IIB_AWS_S3_BUCKET_NAME'] and config['IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR']:
        raise ConfigError(
            'S3 bucket and local artifacts directories cannot be set together.'
            ' Either S3 bucket should be configured or "IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR"'
            ' must be set.'
        )

    if config['IIB_AWS_S3_BUCKET_NAME'] and (
        config['IIB_REQUEST_LOGS_DIR'] or config['IIB_REQUEST_RELATED_BUNDLES_DIR']
    ):
        raise ConfigError(
            'S3 bucket and local artifacts directories cannot be set together.'
            ' Either S3 bucket should be configured or "IIB_REQUEST_LOGS_DIR" and '
            '"IIB_REQUEST_RELATED_BUNDLES_DIR" must be set. Or "IIB_AWS_S3_BUCKET_NAME"'
            '"IIB_REQUEST_LOGS_DIR" and "IIB_REQUEST_RELATED_BUNDLES_DIR" must not be set'
        )
    if config['IIB_AWS_S3_BUCKET_NAME']:
        if not isinstance(config['IIB_AWS_S3_BUCKET_NAME'], str):
            raise ConfigError(
                '"IIB_AWS_S3_BUCKET_NAME" must be set to a valid string. '
                'This is used for read/write access to the s3 bucket by IIB'
            )
        if (
            not os.getenv('AWS_ACCESS_KEY_ID')
            or not os.getenv('AWS_SECRET_ACCESS_KEY')
            or not os.getenv('AWS_DEFAULT_REGION')
        ):
            raise ConfigError(
                '"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY" and "AWS_DEFAULT_REGION" '
                'environment variables must be set to valid strings when'
                '"IIB_AWS_S3_BUCKET_NAME" is set. '
                'These are used for read/write access to the s3 bucket by IIB'
            )


# See app factory pattern:
#   http://flask.pocoo.org/docs/0.12/patterns/appfactories/
def create_app(config_obj=None):  # pragma: no cover
    """
    Create a Flask application object.

    :param str config_obj: the path to the configuration object to use instead of calling
        load_config
    :return: a Flask application object
    :rtype: flask.Flask
    """
    app = Flask(__name__)
    if config_obj:
        app.config.from_object(config_obj)
    else:
        load_config(app)

    # Validate the config
    validate_api_config(app.config)

    # Configure logging
    default_handler.setFormatter(
        logging.Formatter(fmt=app.config['IIB_LOG_FORMAT'], datefmt='%Y-%m-%d %H:%M:%S')
    )
    app.logger.setLevel(app.config['IIB_LOG_LEVEL'])
    for logger_name in app.config['IIB_ADDITIONAL_LOGGERS']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(app.config['IIB_LOG_LEVEL'])
        # Add the Flask handler that streams to WSGI stderr
        logger.addHandler(default_handler)

    # Initialize the database
    db.init_app(app)
    # Initialize the database migrations
    migrations_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'migrations')
    Migrate(app, db, directory=migrations_dir)
    # Initialize Flask Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.user_loader(user_loader)
    login_manager.request_loader(load_user_from_request)

    app.register_blueprint(docs)
    app.register_blueprint(api_v1, url_prefix='/api/v1')
    for code in default_exceptions.keys():
        app.register_error_handler(code, json_error)
    app.register_error_handler(IIBError, json_error)
    app.register_error_handler(ValidationError, json_error)
    app.register_error_handler(kombu.exceptions.KombuError, json_error)

    return app
