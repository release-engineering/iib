# SPDX-License-Identifier: GPL-3.0-or-later
import os
import logging
import types

from iib.exceptions import ConfigError


class Config(object):
    """The base IIB Celery configuration."""

    # When publishing a message, don't continuously retry or else the HTTP connection times out
    broker_transport_options = {'max_retries': 10}
    iib_api_timeout = 30
    iib_docker_config_template = os.path.join(
        os.path.expanduser('~'), '.docker', 'config.json.template'
    )
    iib_greenwave_url = None
    iib_grpc_init_wait_time = 3
    iib_grpc_max_port_tries = 100
    iib_grpc_max_tries = 5
    iib_grpc_start_port = 50051
    iib_image_push_template = '{registry}/iib-build:{request_id}'
    iib_index_image_output_registry = None
    iib_log_level = 'INFO'
    iib_organization_customizations = {}
    iib_request_logs_dir = None
    iib_request_logs_format = (
        '%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s'
    )
    iib_request_logs_level = 'DEBUG'
    iib_required_labels = {}
    iib_skopeo_timeout = '300s'
    iib_total_attempts = 5
    include = [
        'iib.workers.tasks.build',
        'iib.workers.tasks.build_merge_index_image',
        'iib.workers.tasks.general',
    ]
    # The task messages will be acknowledged after the task has been executed,
    # instead of just before
    task_acks_late = True
    # Don't use the default 'celery' queue and routing key
    task_default_queue = 'iib'
    task_default_routing_key = 'iib'
    # Requeue the message if the worker abruptly exits or is signaled
    task_reject_on_worker_lost = True
    # For now, only allow a single process so that all tasks are processed serially
    worker_concurrency = 1
    # Don't allow the worker to fetch more messages than it can handle at a time. This is so that
    # other tasks aren't starved. This will only be useful once more workers are enabled.
    worker_prefetch_multiplier = 1


class ProductionConfig(Config):
    """The production IIB Celery configuration."""


class DevelopmentConfig(Config):
    """The development IIB Celery configuration."""

    broker_url = 'amqp://iib:iib@rabbitmq:5672//'
    iib_api_url = 'http://iib-api:8080/api/v1/'
    iib_log_level = 'DEBUG'
    iib_organization_customizations = {
        'company-marketplace': {
            'csv_annotations': {
                'marketplace.company.io/remote-workflow': (
                    'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
                ),
                'marketplace.company.io/support-workflow': (
                    'https://marketplace.company.com/en-us/operators/{package_name}/support'
                ),
            },
            'package_name_suffix': '-cmp',
            'registry_replacements': {
                'registry.access.company.com': 'registry.marketplace.company.com/cm',
            },
        }
    }
    iib_registry = 'registry:8443'
    iib_request_logs_dir = '/var/log/iib/requests'


class TestingConfig(DevelopmentConfig):
    """The testing IIB Celery configuration."""

    iib_docker_config_template = '/home/iib-worker/.docker/config.json.template'
    iib_greenwave_url = 'some_url'
    iib_omps_url = 'some_url'
    iib_request_logs_dir = None


def configure_celery(celery_app):
    """
    Configure the Celery application instance.

    :param celery.Celery celery: the Celery application instance to configure
    """
    config = ProductionConfig
    prod_config_file_path = os.getenv('IIB_CELERY_CONFIG', '/etc/iib/celery.py')
    if os.getenv('IIB_DEV', '').lower() == 'true':
        config = DevelopmentConfig
    elif os.getenv('IIB_TESTING', 'false').lower() == 'true':
        config = TestingConfig
    elif os.path.isfile(prod_config_file_path):
        # Celery doesn't support importing config files that aren't part of a Python path. This is
        # a hack taken from flask.config.from_pyfile.
        _user_config = {}
        with open(prod_config_file_path, mode='rb') as config_file:
            exec(compile(config_file.read(), prod_config_file_path, 'exec'), _user_config)

        # Celery doesn't support configuring from multiple objects, so this is a way for
        # the configuration in prod_config_file_path to override the defaults in ProductionConfig
        config = ProductionConfig()
        for key, value in _user_config.items():
            # The _user_config dictionary will contain the __builtins__ key, which we need to skip.
            # Additionally, if any modules were imported to define Celery queues, they will
            # be ignored.
            if not key.startswith('__') and not isinstance(value, types.ModuleType):
                setattr(config, key, value)

    celery_app.config_from_object(config, force=True)
    logging.getLogger('iib.workers').setLevel(celery_app.conf.iib_log_level)


def validate_celery_config(conf, **kwargs):
    """
    Perform basic validatation on the Celery configuration when the worker is initialized.

    :param celery.app.utils.Settings conf: the Celery application configuration to validate
    :raises iib.exceptions.ConfigError: if the configuration is invalid
    """
    if not conf.get('iib_registry'):
        raise ConfigError('iib_registry must be set to the destination container registry')

    if not conf.get('iib_api_url'):
        raise ConfigError('iib_api_url must be set')

    if not isinstance(conf['iib_required_labels'], dict):
        raise ConfigError('iib_required_labels must be a dictionary')

    if not isinstance(conf['iib_organization_customizations'], dict):
        raise ConfigError('iib_organization_customizations must be a dictionary')

    for org, org_config in conf['iib_organization_customizations'].items():
        if not isinstance(org, str):
            raise ConfigError('The keys in iib_organization_customizations must be strings')

        if not isinstance(org_config, dict):
            raise ConfigError('The values in iib_organization_customizations must be dictionaries')

        invalid_keys = org_config.keys() - {
            'csv_annotations',
            'package_name_suffix',
            'registry_replacements',
        }
        if invalid_keys:
            raise ConfigError(
                'The following keys set on iib_organization_customizations are '
                f'invalid: {", ".join(sorted(invalid_keys))}'
            )

        for key in ('csv_annotations', 'registry_replacements'):
            value = org_config.get(key)
            if not value:
                continue

            for k, v in value.items():
                if not isinstance(k, str):
                    raise ConfigError(
                        f'The keys in iib_organization_customizations.{org}.{key} must be strings'
                    )

                if not isinstance(v, str):
                    raise ConfigError(
                        f'The values in iib_organization_customizations.{org}.{key} must be strings'
                    )

        if not isinstance(org_config.get('package_name_suffix', ''), str):
            raise ConfigError(
                f'The value of iib_organization_customizations.{org}.package_name_suffix '
                'must be a string'
            )

    iib_request_logs_dir = conf.get('iib_request_logs_dir')
    if iib_request_logs_dir:
        if not os.path.isdir(iib_request_logs_dir):
            raise ConfigError(
                f'iib_request_logs_dir, {iib_request_logs_dir}, must exist and be a directory'
            )
        if not os.access(iib_request_logs_dir, os.W_OK):
            raise ConfigError(f'iib_request_logs_dir, {iib_request_logs_dir}, is not writable!')


def get_worker_config():
    """Return the Celery configuration."""
    # Import this here to avoid a circular import
    import iib.workers.tasks.celery

    return iib.workers.tasks.celery.app.conf
