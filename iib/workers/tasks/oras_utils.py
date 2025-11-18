# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains functions for ORAS (OCI Registry As Storage) operations."""
import logging
import os
import re
import shutil
import tempfile
from typing import Dict, Optional, Any, Tuple

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import run_cmd, set_registry_auths, get_image_digest

log = logging.getLogger(__name__)


def _get_name_and_tag_from_pullspec(image_pullspec: str) -> Tuple[str, str]:
    """
    Parse a container image pullspec (URL) to extract the index name and tag.

    :param str image_pullspec: The full image pullspec string (registry/path/name:tag[@digest]).
    :returns Tuple[str, str]: The extracted index name and tag (e.g., 'iib-pub-pending', 'v4.17').
    :raises IIBError: If the pullspec is missing the required tag delimiter (':')
                      or if the name:tag structure cannot be parsed.
    """
    # Regex to capture the image name and tag, ignoring an optional digest.
    # r'/([^/:]+):([^@]+)(@.*)?$'
    # Group 1: ([^/:]+) -> Image Name (the last path segment before the colon)
    # Group 2: ([^@]+)  -> Tag (the part after the colon, before '@' or end of string)
    # Group 3: (@.*)?   -> Optional digest part, which is ignored
    regex = re.compile(r'/([^/:]+):([^@]+)(@.*)?$')
    match = regex.search(image_pullspec)

    if not match:
        # Check for the most common error: missing the tag delimiter (:)
        if ':' not in image_pullspec:
            raise IIBError(
                f"Invalid pullspec format: '{image_pullspec}'. Missing tag (':') delimiter."
            )

        # Raise a general error if the regex failed for other reasons
        raise IIBError(
            f"Invalid pullspec format: '{image_pullspec}'. Could not parse name:tag structure."
        )

    # Group 1: Image Name (e.g., 'iib-pub-pending')
    index_name = match.group(1)

    # Group 2: Tag (e.g., 'v4.17')
    tag = match.group(2)

    # Final check to ensure the tag isn't empty (e.g., 'image:')
    if not tag:
        raise IIBError(f"Invalid pullspec format: '{image_pullspec}'. Tag is present but empty.")

    return index_name, tag


def _get_artifact_combined_tag(image_name: str, tag: str) -> str:
    """
    Generate a combined artifact tag for the given image name and tag.

    This function generates a unique combined tag for an image by using a template
    string defined in the worker configuration and replacing placeholders with the
    provided image name and tag.

    :param str image_name: The name of the image.
    :param str tag: The version or identifier tag to be combined.
    :return: A formatted string representing the combined artifact tag.
    :rtype: str
    """
    return get_worker_config()['iib_index_db_artifact_tag_template'].format(
        image_name=image_name, tag=tag
    )


def get_indexdb_artifact_pullspec(from_index: str) -> str:
    """
    Construct the full pullspec for index_db artifact.

    :param str from_index: The original full pullspec of the index image.

    :raises IIBError: If the pullspec parsing fails within the helper function.
    :returns str: The full, formatted pullspec for the internal index DB artifact.
    :rtype: str
    """
    conf = get_worker_config()
    image_name, tag = _get_name_and_tag_from_pullspec(from_index)

    return conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'],
        tag=_get_artifact_combined_tag(image_name, tag),
    )


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
    cwd: Optional[str] = None,
) -> None:
    """
    Push a local artifact to an OCI registry using ORAS.

    This function is equivalent to: `oras push {artifact_ref} {local_path}:{artifact_type}`

    :param str artifact_ref: OCI artifact reference to push to (e.g., 'quay.io/repo/repo:tag')
    :param str local_path: Local path to the artifact file. Should be a relative path.
        When using cwd, this should be a relative path (typically just
        the filename) relative to the cwd directory.
    :param str artifact_type: MIME type of the artifact (default: 'application/vnd.sqlite')
    :param dict registry_auths: Optional dockerconfig.json auth information for private registries
    :param dict annotations: Optional annotations to add to the artifact
    :param str cwd: Optional working directory for the ORAS command. When provided, local_path
        should be relative to this directory (e.g., just the filename).
    :raises IIBError: If the push operation fails
    """
    log.info('Pushing artifact from %s to %s with type %s', local_path, artifact_ref, artifact_type)
    if cwd:
        log.info('Using working directory: %s', cwd)

    # Construct the full path for validation
    full_path = os.path.join(cwd, local_path) if cwd else local_path
    if not os.path.exists(full_path):
        raise IIBError(f'Local artifact path does not exist: {full_path}')

    # Build ORAS push command
    cmd = ['oras', 'push', artifact_ref, f'{local_path}:{artifact_type}']

    # Do not allow absolute paths.
    # Absolute paths are extracted to the same place (full path) which might cause collisions.
    if os.path.isabs(local_path):
        log.error('Local artifact path must be relative: %s', local_path)
        raise IIBError(f'Local artifact path must be relative: {local_path}')

    # Add annotations if provided
    if annotations:
        for key, value in annotations.items():
            cmd.extend(['--annotation', f'{key}={value}'])

    # Use namespace-specific registry authentication if provided
    with set_registry_auths(registry_auths, use_empty_config=True):
        try:
            # Only pass params if cwd is provided
            if cwd:
                run_cmd(
                    cmd,
                    params={'cwd': cwd},
                    exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
                )
            else:
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
    conf = get_worker_config()
    artifact_pullspec = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'], tag=tag
    )

    quay_digest = get_image_digest(artifact_pullspec)
    is_digest = get_image_stream_digest(tag)

    return quay_digest == is_digest


def verify_indexdb_cache_for_image(index_image_pullspec: str) -> bool:
    """
    Verify the synchronization state of the index database cache for a given container image.

    This function extracts the image name and tag from the specified image
    pullspec, generates an artifact combined tag, and verifies whether the
    database cache for the image is synchronized.

    :param str index_image_pullspec: The pull specification string of the container image.
    :return: The result of the cache synchronization verification process.
    :rtype: str
    """
    index_name, tag = _get_name_and_tag_from_pullspec(index_image_pullspec)
    return verify_indexdb_cache_sync(_get_artifact_combined_tag(index_name, tag))


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

    conf = get_worker_config()
    artifact_pullspec = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'], tag=tag
    )

    # Use namespace-specific registry authentication if provided
    with set_registry_auths(registry_auths, use_empty_config=True):
        run_cmd(
            [
                'oc',
                'import-image',
                f'index-db-cache:{tag}',
                f'--from={artifact_pullspec}',
                '--confirm',
            ],
            exc_msg=f'Failed to refresh OCI artifact {tag}.',
        )


def refresh_indexdb_cache_for_image(index_image_pullspec: str) -> None:
    """
    Refresh the cached data for an index database, associating it with the given image pullspec.

    This function extracts the name and tag from the specified image pullspec,
    and refreshes the associated index database cache.

    :param str index_image_pullspec: The pull specification of the index image to cache.
    :return: A formatted string combining the index name and tag.
    :rtype: str
    """
    index_name, tag = _get_name_and_tag_from_pullspec(index_image_pullspec)
    refresh_indexdb_cache(_get_artifact_combined_tag(index_name, tag))


def get_imagestream_artifact_pullspec(from_index: str) -> str:
    """
    Get the ImageStream pullspec for the index.db artifact.

    This function constructs the internal OpenShift ImageStream pullspec that can be used
    to pull the index.db artifact from the cached ImageStream instead of directly from Quay.

    :param str from_index: The from_index pullspec
    :return: ImageStream pullspec for the artifact
    :rtype: str
    """
    conf = get_worker_config()
    image_name, tag = _get_name_and_tag_from_pullspec(from_index)
    combined_tag = _get_artifact_combined_tag(image_name, tag)

    # ImageStream pullspec format:
    # image-registry.openshift-image-registry.svc:5000/{namespace}/index-db:{combined_tag}
    imagestream_pullspec = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_imagestream_registry'], tag=combined_tag
    )
    return imagestream_pullspec
