# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains functions for ORAS (OCI Registry As Storage) operations."""
import logging
import os
import shutil
import tempfile
from typing import Dict, Optional, Any

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.tasks.utils import run_cmd, set_registry_auths, get_image_digest

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


def get_image_stream_digest(
    tag: str,
):
    """
    Retrieve the image digest from the OpenShift ImageStream.

    This function queries the `index-db-cache` ImageStream to get the
    SHA256 digest for a specific tag.

    :param tag: The image tag to check.
    :return: The image digest (e.g., "sha256:...").
    :rtype: str
    """
    # This JSONPath expression navigates the ImageStream JSON structure to extract the image digest:
    # - .status.tags: Access the 'tags' array within the 'status' object
    # - [?(@.tag=="{tag}")]: Filter to find the tag object where the 'tag' field equals
    #   the specified tag (@. refers to the current item in the array being filtered)
    # - .items[0]: From the matched tag object, access the first item in its 'items' array
    # - .image: Extract the 'image' field, which contains the SHA256 digest
    jsonpath = f'\'{{.status.tags[?(@.tag=="{tag}")].items[0].image}}\''
    return run_cmd(
        ['oc', 'get', 'imagestream', 'index-db-cache', '-o', f'jsonpath={jsonpath}'],
        exc_msg=f'Failed to get digest for ImageStream tag {tag}.',
    )


def verify_indexdb_cache_sync(tag: str) -> bool:
    """
    Compare the digest of the ImageStream with the digest of the image in repository.

    This function verifies if the local ImageStream cache is up to date with
    the latest image in the remote registry.

    :param tag: The image tag to verify.
    :return: True if the digests match (cache is synced), False otherwise.
    :rtype: bool
    """
    # TODO - This is EXAMPLE location - final one should be loaded from config variable
    repository = "quay.io/exd-guild-hello-operator/example-repository"

    quay_digest = get_image_digest(f"{repository}:{tag}")
    is_digest = get_image_stream_digest(tag)

    return quay_digest == is_digest


def refresh_indexdb_cache(
    tag: str,
    registry_auths: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Force a synchronization of the ImageStream with the remote registry.

    This function imports the specified image from Quay.io into the `index-db-cache`
    ImageStream, ensuring the local cache is up-to-date.

    :param tag: The container image tag to refresh.
    :param registry_auths: Optional authentication data for the registry.
    """
    log.info('Refreshing OCI artifact cache: %s', tag)

    # TODO - This is EXAMPLE location - final one should be loaded from config variable
    repository = "quay.io/exd-guild-hello-operator/example-repository"

    # Use namespace-specific registry authentication if provided
    with set_registry_auths(registry_auths, use_empty_config=True):
        run_cmd(
            [
                'oc',
                'import-image',
                f'index-db-cache:{tag}',
                f'--from={repository}:{tag}',
                '--confirm',
            ],
            exc_msg=f'Failed to refresh OCI artifact {tag}.',
        )
