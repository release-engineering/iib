# SPDX-License-Identifier: GPL-3.0-or-later
class Config(object):
    """The base IIB Flask configuration."""
    # Additional loggers to set to the level defined in IIB_LOG_LEVEL
    IIB_ADDITIONAL_LOGGERS = []
    IIB_LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s'
    # This sets the level of the "flask.app" logger, which is accessed from current_app.logger
    IIB_LOG_LEVEL = 'INFO'


class ProductionConfig(Config):
    """The production IIB Flask configuration."""
    DEBUG = False


class DevelopmentConfig(Config):
    """The development IIB Flask configuration."""
    IIB_LOG_LEVEL = 'DEBUG'


class TestingConfig(DevelopmentConfig):
    """The testing IIB Flask configuration."""
    DEBUG = True
