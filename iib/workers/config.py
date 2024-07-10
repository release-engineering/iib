# SPDX-License-Identifier: GPL-3.0-or-later
import os
import logging
import shutil
import types
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from kombu import Queue
from celery import Celery, app

from iib.exceptions import ConfigError
from iib.workers.tasks.iib_static_types import (
    IIBOrganizationCustomizations,
    CSVAnnotations,
    PackageNameSuffix,
    ImageNameFromLabels,
    RegistryReplacements,
    EncloseRepo,
    iib_organization_customizations_type,
)


class Config(object):
    """The base IIB Celery configuration."""

    # When publishing a message, don't continuously retry or else the HTTP connection times out
    broker_transport_options: Dict[str, int] = {'max_retries': 10}
    # Avoid infinite Celery retries when the broker is offline.
    broker_connection_max_retries: int = 10
    iib_aws_s3_bucket_name: Optional[str] = None
    iib_api_timeout: int = 120
    iib_docker_config_template: str = os.path.join(
        os.path.expanduser('~'), '.docker', 'config.json.template'
    )
    iib_greenwave_url: Optional[str] = None
    iib_grpc_init_wait_time: int = 100
    iib_grpc_max_tries: int = 5
    # size of both ranges, needs to be the same, ranges neeeds to be exclusive
    iib_opm_port_ranges: Dict[str, Tuple[int, int]] = {
        "opm_port": (50051, 50151),
        "opm_pprof_port": (50151, 50251),
    }
    iib_opm_pprof_lock_required_min_version = "1.29.0"
    iib_image_push_template: str = '{registry}/iib-build:{request_id}'
    iib_index_image_output_registry: Optional[str] = None
    iib_log_level: str = 'INFO'
    iib_max_recursive_related_bundles = 15
    # list of index images to which we can add bundles without "com.redhat.openshift.versions" label
    iib_no_ocp_label_allow_list: List[str] = []
    iib_organization_customizations: iib_organization_customizations_type = {}
    iib_sac_queues: List[str] = []
    iib_request_logs_dir: Optional[str] = None
    iib_request_logs_format: str = (
        '%(asctime)s %(name)s %(processName)s {request_id} '
        '%(levelname)s %(module)s.%(funcName)s %(message)s'
    )
    iib_request_logs_level: str = 'DEBUG'
    iib_required_labels: Dict[str, str] = {}
    iib_request_related_bundles_dir: Optional[str] = None
    # Configuration for dogpile.cache
    # Disabled by default (by using 'dogpile.cache.null').
    # To enable caching set 'dogpile.cache.memcached' as backend.
    iib_dogpile_backend: str = 'dogpile.cache.null'
    iib_dogpile_expiration_time: int = 600
    iib_dogpile_arguments: Dict[str, List[str]] = {'url': ['127.0.0.1']}
    iib_skopeo_timeout: str = '300s'
    iib_total_attempts: int = 5
    iib_retry_delay: int = 10
    iib_retry_jitter: int = 10
    iib_retry_multiplier: int = 5
    iib_supported_archs: dict = {
        "amd64": "x86_64",
        "arm64": "aarch64",
        "s390x": "s390x",
        "ppc64le": "ppc64le",
    }
    iib_default_opm: str = 'opm'
    iib_related_image_registry_replacement: Optional[Dict[str, Dict[str, str]]] = {}
    include: List[str] = [
        'iib.workers.tasks.build',
        'iib.workers.tasks.build_merge_index_image',
        'iib.workers.tasks.build_recursive_related_bundles',
        'iib.workers.tasks.build_regenerate_bundle',
        'iib.workers.tasks.build_create_empty_index',
        'iib.workers.tasks.build_fbc_operations',
        'iib.workers.tasks.general',
    ]
    # Path to hidden location of SQLite database
    hidden_index_db_path: str = '/var/lib/iib/_hidden/do.not.edit.db'
    # path where catalog resides in fbc_fragment
    # might need to be changed, currently based on test fbc-fragment
    fbc_fragment_catalog_path: str = '/configs'
    # The task messages will be acknowledged after the task has been executed,
    # instead of just before
    task_acks_late: bool = True
    # Don't use the default 'celery' queue and routing key
    task_default_queue: str = 'iib'
    task_default_routing_key: str = 'iib'
    # Requeue the message if the worker abruptly exits or is signaled
    task_reject_on_worker_lost: bool = True
    # Path to index.db in our temp directories used in IIB code
    temp_index_db_path: str = 'database/index.db'
    # Path to fbc_fragment's catalog in our temp directories
    temp_fbc_fragment_path = 'fbc-fragment'
    # For now, only allow a single process so that all tasks are processed serially
    worker_concurrency: int = 1
    # Before each task execution, instruct the worker to check if this task is a duplicate message.
    # Deduplication occurs only with tasks that have the same identifier,
    # enabled late acknowledgment, were redelivered by the message broker
    # and their state is SUCCESS in the result backend.
    worker_deduplicate_successful_tasks: bool = True
    # The result_backend and result_persistent must be set when
    # worker_deduplicate_successful_tasks is set to True
    result_backend: str = 'rpc://'
    result_persistent: bool = False
    # Don't allow the worker to fetch more messages than it can handle at a time. This is so that
    # other tasks aren't starved. This will only be useful once more workers are enabled.
    worker_prefetch_multiplier: int = 1
    # Enable send events to the broker. This is needed for celery promethues exporter
    worker_send_task_events: bool = True
    task_send_sent_event: bool = True


class ProductionConfig(Config):
    """The production IIB Celery configuration."""


class DevelopmentConfig(Config):
    """The development IIB Celery configuration."""

    broker_url: str = 'amqp://iib:iib@rabbitmq:5673//'
    iib_api_url: str = 'http://iib-api:8080/api/v1/'
    iib_log_level: str = 'DEBUG'
    iib_organization_customizations: iib_organization_customizations_type = {
        'company-marketplace': [
            IIBOrganizationCustomizations({'type': 'resolve_image_pullspecs'}),
            IIBOrganizationCustomizations({'type': 'related_bundles'}),
            IIBOrganizationCustomizations({'type': 'perform_bundle_replacements'}),
            CSVAnnotations(
                {
                    'type': 'csv_annotations',
                    'annotations': {
                        'marketplace.company.io/remote-workflow': (
                            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
                        ),
                        'marketplace.company.io/support-workflow': (
                            'https://marketplace.company.com/en-us/operators/{package_name}/support'
                        ),
                    },
                }
            ),
            PackageNameSuffix({'type': 'package_name_suffix', 'suffix': '-cmp'}),
            RegistryReplacements(
                {
                    'type': 'registry_replacements',
                    'replacements': {
                        'registry.access.company.com': 'registry.marketplace.company.com/cm',
                    },
                }
            ),
            ImageNameFromLabels(
                {'type': 'image_name_from_labels', 'template': '{name}-{version}-final'}
            ),
        ],
        'company-managed': [
            PackageNameSuffix({'type': 'package_name_suffix', 'suffix': '-cmp'}),
            CSVAnnotations(
                {
                    'type': 'csv_annotations',
                    'annotations': {
                        'marketplace.company.io/remote-workflow': (
                            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
                        ),
                        'marketplace.company.io/support-workflow': (
                            'https://marketplace.company.com/en-us/operators/{package_name}/support'
                        ),
                    },
                }
            ),
            RegistryReplacements(
                {
                    'type': 'registry_replacements',
                    'replacements': {
                        'registry.access.company.com': 'registry.koji.company.com',
                        'quay.io': 'registry.koji.company.com',
                    },
                }
            ),
            IIBOrganizationCustomizations({'type': 'related_bundles'}),
            ImageNameFromLabels({'type': 'image_name_from_labels', 'template': '{name}-{version}'}),
            EncloseRepo(
                {'type': 'enclose_repo', 'enclosure_glue': '----', 'namespace': "company-pending"}
            ),
            RegistryReplacements(
                {
                    'type': 'registry_replacements',
                    'replacements': {'registry.koji.company.com': 'quaaay.com'},
                }
            ),
        ],
        'company-managed-recursive': [
            RegistryReplacements(
                {
                    'type': 'registry_replacements',
                    'replacements': {
                        'registry.redhat.io': 'brew.registry.redhat.io',
                    },
                }
            ),
            IIBOrganizationCustomizations({'type': 'related_bundles'}),
            RegistryReplacements(
                {
                    'type': 'registry_replacements',
                    'replacements': {
                        'brew.registry.redhat.io': 'quay.io',
                    },
                }
            ),
        ],
    }
    iib_registry: str = 'registry:8443'
    iib_request_logs_dir: Optional[str] = '/var/log/iib/requests'
    iib_request_related_bundles_dir: Optional[str] = '/var/lib/requests/related_bundles'
    iib_request_recursive_related_bundles_dir: Optional[
        str
    ] = '/var/lib/requests/recursive_related_bundles'
    iib_dogpile_backend: str = 'dogpile.cache.memcached'
    iib_ocp_opm_mapping: dict = {
        "v4.6": "opm-v1.26.4",
        "v4.7": "opm-v1.26.4",
        "v4.8": "opm-v1.26.4",
        "v4.9": "opm-v1.26.4",
        "v4.10": "opm-v1.26.4",
        "v4.11": "opm-v1.26.4",
        "v4.12": "opm-v1.26.4",
        "v4.13": "opm-v1.26.4",
        "v4.14": "opm-v1.26.4",
        "v4.15": "opm-v1.26.4",
        "v4.16": "opm-v1.40.0",
    }


class TestingConfig(DevelopmentConfig):
    """The testing IIB Celery configuration."""

    iib_docker_config_template: str = '/home/iib-worker/.docker/config.json.template'
    iib_greenwave_url: str = 'some_url'
    iib_omps_url: str = 'some_url'
    iib_request_logs_dir: Optional[str] = None
    iib_request_related_bundles_dir: Optional[str] = None
    # disable dogpile cache for tests
    iib_dogpile_backend: str = 'dogpile.cache.null'


def configure_celery(celery_app: Celery) -> None:
    """
    Configure the Celery application instance.

    :param celery.Celery celery: the Celery application instance to configure
    """
    config: Union[Type[Config], Config] = ProductionConfig
    prod_config_file_path = os.getenv('IIB_CELERY_CONFIG', '/etc/iib/celery.py')
    if os.getenv('IIB_DEV', '').lower() == 'true':
        config = DevelopmentConfig
    elif os.getenv('IIB_TESTING', 'false').lower() == 'true':
        config = TestingConfig
    elif os.path.isfile(prod_config_file_path):
        # Celery doesn't support importing config files that aren't part of a Python path. This is
        # a hack taken from flask.config.from_pyfile.
        _user_config: Dict[str, Any] = {}
        with open(prod_config_file_path, mode='rb') as config_file:
            exec(compile(config_file.read(), prod_config_file_path, 'exec'), _user_config)  # nosec

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

    if config.iib_sac_queues:
        celery_app.conf.task_queues = [
            Queue(qname, queue_arguments={'x-single-active-consumer': True})
            for qname in config.iib_sac_queues
        ]

    logging.getLogger('iib.workers').setLevel(celery_app.conf.iib_log_level)


def validate_celery_config(conf: app.utils.Settings, **kwargs) -> None:
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

    if conf.get('iib_no_ocp_label_allow_list'):
        if any(not index for index in conf['iib_no_ocp_label_allow_list']):
            raise ConfigError('Empty string is not allowed in iib_no_ocp_label_allow_list')

    if conf.get('iib_related_image_registry_replacement') and not isinstance(
        conf['iib_related_image_registry_replacement'], dict
    ):
        raise ConfigError('iib_related_image_registry_replacement must be a dictionary')

    _validate_multiple_opm_mapping(conf['iib_ocp_opm_mapping'])
    _validate_iib_org_customizations(conf['iib_organization_customizations'])

    if conf.get('iib_aws_s3_bucket_name'):
        if not isinstance(conf['iib_aws_s3_bucket_name'], str):
            raise ConfigError(
                '"iib_aws_s3_bucket_name" must be set to a valid string. '
                'This is used for read/write access to the s3 bucket by IIB'
            )
        if (
            not conf.get('iib_request_logs_dir')
            or not conf.get('iib_request_related_bundles_dir')
            or not conf.get('iib_request_recursive_related_bundles_dir')
        ):
            raise ConfigError(
                '"iib_request_logs_dir", "iib_request_related_bundles_dir" and '
                '"iib_request_recursive_related_bundles_dir" must be set '
                'when iib_aws_s3_bucket_name is set.'
            )
        if (
            not os.getenv('AWS_ACCESS_KEY_ID')
            or not os.getenv('AWS_SECRET_ACCESS_KEY')
            or not os.getenv('AWS_DEFAULT_REGION')
        ):
            raise ConfigError(
                '"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY" and "AWS_DEFAULT_REGION" '
                'environment variables must be set to valid strings when'
                '"iib_aws_s3_bucket_name" is set. '
                'These are used for read/write access to the s3 bucket by IIB'
            )
    else:
        if not conf.get('iib_request_recursive_related_bundles_dir'):
            raise ConfigError(
                '"iib_request_recursive_related_bundles_dir" must be set when'
                ' "iib_aws_s3_bucket_name" is not set'
            )

    for directory in (
        'iib_request_logs_dir',
        'iib_request_related_bundles_dir',
        'iib_request_recursive_related_bundles_dir',
    ):
        iib_request_temp_data_dir = conf.get(directory)
        if iib_request_temp_data_dir:
            if not os.path.isdir(iib_request_temp_data_dir):
                raise ConfigError(f'{directory} must exist and be a directory')
            if not os.access(iib_request_temp_data_dir, os.W_OK):
                raise ConfigError(f'{directory}, is not writable!')

    if os.getenv('IIB_OTEL_TRACING', '').lower() == 'true':
        if not os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT') or not os.getenv('OTEL_SERVICE_NAME'):
            raise ConfigError(
                '"OTEL_EXPORTER_OTLP_ENDPOINT" and "OTEL_SERVICE_NAME" environment '
                'variables must be set to valid strings when "IIB_OTEL_TRACING" is set to True.'
            )


def _validate_multiple_opm_mapping(iib_ocp_opm_mapping: Dict[str, str]) -> None:
    """
    Validate iib_ocp_opm_mapping config variable.

    :param dict iib_ocp_opm_mapping: the value of iib_ocp_opm_mapping variable
    :raises iib.exceptions.ConfigError: if the configuration is invalid
    """
    if iib_ocp_opm_mapping is not None:
        if not isinstance(iib_ocp_opm_mapping, dict):
            raise ConfigError('iib_ocp_opm_mapping must be a dictionary')
        opms_defined = set(iib_ocp_opm_mapping.values())
        for opm_version in opms_defined:
            if shutil.which(opm_version) is None:
                raise ConfigError(f'{opm_version} is not installed')


def _validate_iib_org_customizations(
    iib_org_customizations: iib_organization_customizations_type,
) -> None:
    """
    Validate ``iib_organization_customizations`` celery config variable.

    :param dict iib_org_customizations: the value of iib_organization_customizations config
        variable
    :raises iib.exceptions.ConfigError: if the configuration is invalid
    """
    if not isinstance(iib_org_customizations, dict):
        raise ConfigError('iib_organization_customizations must be a dictionary')

    valid_customizations = {
        'csv_annotations': {'annotations'},
        'package_name_suffix': {'suffix'},
        'registry_replacements': {'replacements'},
        'image_name_from_labels': {'template'},
        'enclose_repo': {'enclosure_glue', 'namespace'},
    }

    for org, org_config in iib_org_customizations.items():
        if not isinstance(org, str):
            raise ConfigError('The org keys in iib_organization_customizations must be strings')

        if not isinstance(org_config, list):
            raise ConfigError('The org values in iib_organization_customizations must be a list')

        for customization in org_config:
            if not isinstance(customization, dict):
                raise ConfigError(
                    'Every customization for an org in '
                    'iib_organization_customizations must be dictionary'
                )

            customization_type = str(customization.get('type'))
            if customization_type not in valid_customizations.keys():
                raise ConfigError(
                    f'Invalid customization in iib_organization_customizations {customization}'
                )

            invalid_customization_keys = (
                customization.keys() - valid_customizations[customization_type] - {'type'}
            )
            if invalid_customization_keys:
                raise ConfigError(
                    f'The keys {invalid_customization_keys} in iib_organization_customizations'
                    f'.{org}[{org_config.index(customization)}] are invalid.'
                )

            if customization_type in ('csv_annotations', 'registry_replacements'):
                for valid_key in valid_customizations[customization_type]:
                    #  MYPY error: TypedDict key must be a string literal; expected one of ("type")
                    if not customization[valid_key]:  # type: ignore
                        continue

                    #  MYPY error: TypedDict key must be a string literal; expected one of ("type")
                    for k, v in customization[valid_key].items():  # type: ignore
                        if not isinstance(k, str):
                            raise ConfigError(
                                f'The keys in iib_organization_customizations.{org}'
                                f'[{org_config.index(customization)}].{valid_key} must be strings'
                            )

                        if not isinstance(v, str):
                            raise ConfigError(
                                f'The values in iib_organization_customizations.{org}'
                                f'[{org_config.index(customization)}].{valid_key} must be strings'
                            )

            if customization_type in (
                'package_name_suffix',
                'image_name_from_labels',
                'enclose_repo',
            ):
                for valid_key in valid_customizations[customization_type]:
                    #  MYPY error: TypedDict key must be a string literal; expected one of ("type")
                    if not isinstance(customization[valid_key], str):  # type: ignore
                        raise ConfigError(
                            f'The value of iib_organization_customizations.{org}'
                            f'[{org_config.index(customization)}].{valid_key} must be a string'
                        )


def get_worker_config() -> app.utils.Settings:
    """Return the Celery configuration."""
    # Import this here to avoid a circular import
    import iib.workers.tasks.celery

    return iib.workers.tasks.celery.app.conf
