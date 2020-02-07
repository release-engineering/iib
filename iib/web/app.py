# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os

from flask import Flask
from flask.logging import default_handler
from flask_migrate import Migrate

from iib.web import db
from iib.web.api_v1 import api_v1
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
    else:
        default_config_obj = 'iib.web.config.ProductionConfig'
        config_file = '/etc/iib/settings.py'
    app.config.from_object(default_config_obj)

    if config_file and os.path.isfile(config_file):
        app.config.from_pyfile(config_file)


# See app factory pattern:
#   http://flask.pocoo.org/docs/0.12/patterns/appfactories/
def create_app(config_obj=None):
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
    app.register_blueprint(api_v1, url_prefix='/api/v1')

    return app
