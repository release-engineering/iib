# SPDX-License-Identifier: GPL-3.0-or-later
"""
IIB Worker Configuration for Containerized Workflow.

This configuration is used when running IIB in containerized mode where builds
are executed in an external Konflux cluster instead of locally in the worker.
"""
import json
import os
from typing import Optional

from iib.workers.config import DevelopmentConfig


class ContainerizedConfig(DevelopmentConfig):
    """Configuration for IIB worker in containerized mode."""

    # ===================================================================
    # Konflux Cluster Configuration
    # ===================================================================
    # These are read from environment variables set in .env.containerized
    iib_konflux_cluster_url: Optional[str] = os.getenv('IIB_KONFLUX_CLUSTER_URL')
    iib_konflux_cluster_token: Optional[str] = os.getenv('IIB_KONFLUX_CLUSTER_TOKEN')
    iib_konflux_cluster_ca_cert: Optional[str] = os.getenv(
        'IIB_KONFLUX_CLUSTER_CA_CERT', '/etc/iib/konflux-ca.crt'
    )
    iib_konflux_namespace: Optional[str] = os.getenv('IIB_KONFLUX_NAMESPACE')
    iib_konflux_pipeline_timeout: int = int(os.getenv('IIB_KONFLUX_PIPELINE_TIMEOUT', '1800'))

    # ===================================================================
    # GitLab Configuration
    # ===================================================================
    # Parse GitLab tokens from environment variable
    _gitlab_tokens_str = os.getenv('IIB_INDEX_CONFIGS_GITLAB_TOKENS_MAP')
    iib_index_configs_gitlab_tokens_map = (
        json.loads(_gitlab_tokens_str) if _gitlab_tokens_str else None
    )

    # ===================================================================
    # Registry Configuration
    # ===================================================================
    iib_registry: str = os.getenv('IIB_REGISTRY', 'registry:8443')
    iib_image_push_template: str = os.getenv(
        'IIB_IMAGE_PUSH_TEMPLATE', '{registry}/iib-build:{request_id}'
    )
    # Docker config template for reset_docker_config()
    # Points to the mounted auth config so symlink creation works correctly
    iib_docker_config_template: str = '/etc/containers/auth.json'

    # ===================================================================
    # Index DB Artifact Configuration
    # ===================================================================
    iib_index_db_artifact_registry: Optional[str] = os.getenv('IIB_INDEX_DB_ARTIFACT_REGISTRY')
    iib_index_db_imagestream_registry: Optional[str] = os.getenv(
        'IIB_INDEX_DB_IMAGESTREAM_REGISTRY'
    )
    iib_index_db_artifact_template: str = os.getenv(
        'IIB_INDEX_DB_ARTIFACT_TEMPLATE', '{registry}/index-db:{tag}'
    )

    # ===================================================================
    # Task Routing Configuration
    # ===================================================================
    # Include containerized task modules
    include = DevelopmentConfig.include + [
        'iib.workers.tasks.build_containerized_rm',
    ]

    # ===================================================================
    # Logging Configuration
    # ===================================================================
    iib_log_level: str = os.getenv('IIB_LOG_LEVEL', 'DEBUG')
    iib_request_logs_dir: Optional[str] = os.getenv(
        'IIB_REQUEST_LOGS_DIR', '/var/log/iib/requests'
    )

    # ===================================================================
    # Optional Configuration
    # ===================================================================
    iib_aws_s3_bucket_name: Optional[str] = os.getenv('IIB_AWS_S3_BUCKET_NAME')
    iib_greenwave_url: Optional[str] = os.getenv('IIB_GREENWAVE_URL')
    iib_skopeo_timeout: str = os.getenv('IIB_SKOPEO_TIMEOUT', '300s')
    iib_total_attempts: int = int(os.getenv('IIB_TOTAL_ATTEMPTS', '5'))
    iib_retry_delay: int = int(os.getenv('IIB_RETRY_DELAY', '10'))
    iib_retry_jitter: int = int(os.getenv('IIB_RETRY_JITTER', '10'))
    iib_retry_multiplier: int = int(os.getenv('IIB_RETRY_MULTIPLIER', '5'))

    # ===================================================================
    # Validation
    # ===================================================================
    @classmethod
    def validate(cls):
        """
        Validate that required configuration is present.

        :raises ValueError: If required configuration is missing
        """
        required_configs = {
            'iib_konflux_cluster_url': cls.iib_konflux_cluster_url,
            'iib_konflux_cluster_token': cls.iib_konflux_cluster_token,
            'iib_konflux_cluster_ca_cert': cls.iib_konflux_cluster_ca_cert,
            'iib_konflux_namespace': cls.iib_konflux_namespace,
        }

        missing = [name for name, value in required_configs.items() if not value]

        if missing:
            raise ValueError(
                f"Missing required Konflux configuration: {', '.join(missing)}. "
                "Please set these in your .env.containerized file."
            )


# Validate configuration on import
ContainerizedConfig.validate()

# Export config as module-level variables for Celery to pick up
# This is required because Celery's exec() loading expects module-level vars, not a class
_config = ContainerizedConfig()
for _attr in dir(_config):
    if not _attr.startswith('_') and _attr not in globals():
        globals()[_attr] = getattr(_config, _attr)
