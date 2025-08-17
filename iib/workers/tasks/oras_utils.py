# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains functions for ORAS (OCI Registry As Storage) operations."""
import logging
import os
import shutil
import tempfile
from typing import Dict, Optional, Any

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.tasks.utils import run_cmd, set_registry_auths

log = logging.getLogger(__name__)


@instrument_tracing(span_name="workers.tasks.oras_utils.get_oras_artifact")
def get_oras_artifact(
    artifact_ref: str,
    base_dir: str,
    registry_auths: Optional[Dict[str, Any]] = None,
    temp_dir_prefix: str = "iib-oras-",
) -> str:
    """
    Pull an OCI artifact from a registry to a temporary directory.

    This function is equivalent to: `oras pull {artifact_ref} -o {temp_dir}`

    :param str artifact_ref: OCI artifact reference (e.g., 'quay.io/repo/repo:tag')
    :param str base_dir: Base directory where the temporary subdirectory will be created.
        Can be an absolute or relative path. If relative, the directory must exist.
        The function always returns an absolute path regardless of the base_dir type.
    :param dict registry_auths: Optional dockerconfig.json auth information for private registries
    :param str temp_dir_prefix: Prefix for the temporary directory name
    :return: Path to the temporary directory containing the artifact (always absolute)
    :rtype: str
    :raises IIBError: If the pull operation fails
    """
    log.info('Pulling OCI artifact %s to temporary directory', artifact_ref)

    # Create a subdirectory within the provided base_dir
    temp_dir = tempfile.mkdtemp(prefix=temp_dir_prefix, dir=base_dir)

    # Use namespace-specific registry authentication if provided
    with set_registry_auths(registry_auths, use_empty_config=True):
        try:
            run_cmd(
                ['oras', 'pull', artifact_ref, '-o', temp_dir],
                exc_msg=f'Failed to pull OCI artifact {artifact_ref}',
            )
            log.info('Successfully pulled OCI artifact %s to %s', artifact_ref, temp_dir)
            return temp_dir
        except Exception as e:
            # Clean up temp directory on failure
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise IIBError(f'Failed to pull OCI artifact {artifact_ref}: {e}')


@instrument_tracing(span_name="workers.tasks.oras_utils.push_oras_artifact")
def push_oras_artifact(
    artifact_ref: str,
    local_path: str,
    artifact_type: str = "application/vnd.sqlite",
    registry_auths: Optional[Dict[str, Any]] = None,
    annotations: Optional[Dict[str, str]] = None,
) -> None:
    """
    Push a local artifact to an OCI registry using ORAS.

    This function is equivalent to: `oras push {artifact_ref} {local_path}:{artifact_type}`

    :param str artifact_ref: OCI artifact reference to push to (e.g., 'quay.io/repo/repo:tag')
    :param str local_path: Local path to the artifact file. Can be an absolute or relative path.
        If an absolute path is provided, the --disable-path-validation flag will be
        automatically added.
    :param str artifact_type: MIME type of the artifact (default: 'application/vnd.sqlite')
    :param dict registry_auths: Optional dockerconfig.json auth information for private registries
    :param dict annotations: Optional annotations to add to the artifact
    :raises IIBError: If the push operation fails
    """
    log.info('Pushing artifact from %s to %s with type %s', local_path, artifact_ref, artifact_type)

    if not os.path.exists(local_path):
        raise IIBError(f'Local artifact path does not exist: {local_path}')

    # Build ORAS push command
    cmd = ['oras', 'push', artifact_ref, f'{local_path}:{artifact_type}']

    # Add --disable-path-validation flag for absolute paths
    if os.path.isabs(local_path):
        cmd.append('--disable-path-validation')

    # Add annotations if provided
    if annotations:
        for key, value in annotations.items():
            cmd.extend(['--annotation', f'{key}={value}'])

    # Use namespace-specific registry authentication if provided
    with set_registry_auths(registry_auths, use_empty_config=True):
        try:
            run_cmd(cmd, exc_msg=f'Failed to push OCI artifact to {artifact_ref}')
            log.info('Successfully pushed OCI artifact to %s', artifact_ref)
        except Exception as e:
            raise IIBError(f'Failed to push OCI artifact to {artifact_ref}: {e}')
