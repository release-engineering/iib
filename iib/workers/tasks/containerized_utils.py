# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains utility functions for containerized IIB operations."""
import json
import logging
import os
import queue
import threading
from typing import Dict, List, Optional

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.iib_static_types import BundleImage
from iib.workers.tasks.oras_utils import (
    _get_artifact_combined_tag,
    _get_name_and_tag_from_pullspec,
    get_image_digest,
    get_indexdb_artifact_pullspec,
    get_imagestream_artifact_pullspec,
    get_oras_artifact,
    push_oras_artifact,
    refresh_indexdb_cache_for_image,
    verify_indexdb_cache_for_image,
)
from iib.workers.tasks.utils import run_cmd, skopeo_inspect

log = logging.getLogger(__name__)


class ValidateBundlesThread(threading.Thread):
    """Thread to validate whether the bundle pullspecs are present in the registry."""

    def __init__(self, bundles_queue: queue.Queue) -> None:
        """
        Initialize the thread to validate whether the bundle pullspecs are present in the registry.

        :param queue.Queue bundles_queue: the queue of bundles to validate
        """
        super().__init__()
        self.bundles_queue = bundles_queue
        self.exception: Optional[Exception] = None
        self.bundle: Optional[str] = None

    def run(self) -> None:
        """Execute the validation of the bundle pullspecs."""
        bundle = None
        try:
            while not self.bundles_queue.empty():
                bundle = self.bundles_queue.get()
                skopeo_inspect(f'docker://{bundle}', '--raw', return_json=False)
        except IIBError as e:
            self.bundle = bundle
            log.error(f"Error validating bundle {bundle}: {e}")
            self.exception = e
        finally:
            while not self.bundles_queue.empty():
                self.bundles_queue.task_done()


def wait_for_bundle_validation_threads(validation_threads: List[ValidateBundlesThread]) -> None:
    """
    Wait for all bundle validation threads to complete.

    :param list threads: the list of threads to wait for
    """
    for t in validation_threads:
        t.join()
        if t.exception:
            bundle_str = str(t.bundle) if t.bundle else "unknown"
            log.error(f"Error validating bundle {bundle_str}: {t.exception}")
            raise IIBError(f"Error validating bundle {bundle_str}: {t.exception}")


def validate_bundles_in_parallel(
    bundles: List[BundleImage], threads=5, wait=True
) -> Optional[List[ValidateBundlesThread]]:
    """
    Validate bundles in parallel.

    :param list bundles: the list of bundles to validate
    :param int threads: the number of threads to use
    :param bool wait: whether to wait for all threads to complete
    :return: the list of threads if not waiting, None otherwise
    :rtype: Optional[List[ValidateBundlesThread]]
    """
    bundles_queue: queue.Queue[BundleImage] = queue.Queue()

    for bundle in bundles:
        bundles_queue.put(bundle)

    validation_threads: List[ValidateBundlesThread] = []
    for _ in range(threads):
        validation_thread = ValidateBundlesThread(bundles_queue)
        validation_threads.append(validation_thread)
        validation_thread.start()

    if wait:
        wait_for_bundle_validation_threads(validation_threads)
    else:
        return validation_threads
    return None


def pull_index_db_artifact(from_index: str, temp_dir: str) -> str:
    """
    Pull index.db artifact from registry, using ImageStream cache if available.

    This function determines whether to use OpenShift ImageStream cache or pull directly
    from the registry based on the iib_use_imagestream_cache configuration.

    :param str from_index: The from_index pullspec
    :param str temp_dir: Temporary directory where the artifact will be extracted
    :return: Path to the directory containing the extracted artifact
    :rtype: str
    :raises IIBError: If the pull operation fails
    """
    conf = get_worker_config()
    if conf.get('iib_use_imagestream_cache', False):
        # Verify index.db cache is synced. Refresh if not.
        log.info('ImageStream cache is enabled. Checking cache sync status.')
        if verify_indexdb_cache_for_image(from_index):
            log.info('Index.db cache is synced. Pulling from ImageStream.')
            # Pull from ImageStream when digests match
            imagestream_ref = get_imagestream_artifact_pullspec(from_index)
            artifact_dir = get_oras_artifact(
                imagestream_ref,
                temp_dir,
            )
        else:
            log.info('Index.db cache is not synced. Refreshing and pulling from Quay.')
            refresh_indexdb_cache_for_image(from_index)
            # Pull directly from Quay after triggering refresh
            artifact_ref = get_indexdb_artifact_pullspec(from_index)
            artifact_dir = get_oras_artifact(
                artifact_ref,
                temp_dir,
            )
    else:
        # Pull directly from Quay without ImageStream cache
        log.info('ImageStream cache is disabled. Pulling index.db artifact directly from registry.')
        artifact_ref = get_indexdb_artifact_pullspec(from_index)
        artifact_dir = get_oras_artifact(
            artifact_ref,
            temp_dir,
        )

    return artifact_dir


def write_build_metadata(
    local_repo_path: str,
    opm_version: str,
    ocp_version: str,
    distribution_scope: str,
    binary_image: str,
    request_id: int,
) -> None:
    """
    Write build metadata file for Konflux build task.

    This function creates a JSON metadata file that contains information needed by the
    Konflux build task, including OPM version, labels, binary image, and request ID.

    :param str local_repo_path: Path to local Git repository
    :param str opm_version: OPM version string (e.g., "opm-1.40.0")
    :param str ocp_version: OCP version (e.g., "v4.19")
    :param str distribution_scope: Distribution scope (e.g., "PROD")
    :param str binary_image: Binary image pullspec
    :param int request_id: Request ID
    """
    metadata = {
        'opm_version': opm_version,
        'labels': {
            'com.redhat.index.delivery.version': ocp_version,
            'com.redhat.index.delivery.distribution_scope': distribution_scope,
        },
        'binary_image': binary_image,
        'request_id': request_id,
    }

    metadata_path = os.path.join(local_repo_path, '.iib-build-metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    log.info('Written build metadata to %s', metadata_path)


def get_list_of_output_pullspec(
    request_id: int, build_tags: Optional[List[str]] = None
) -> List[str]:
    """
    Build list of output pull specifications for index images.

    Creates pull specs for the request ID and any additional build tags,
    using the worker configuration template.

    :param int request_id: The IIB request ID
    :param Optional[List[str]] build_tags: Additional tags to create pull specs for
    :return: List of output pull specifications
    :rtype: List[str]
    """
    _tags = [str(request_id)]
    if build_tags:
        _tags.extend(build_tags)
    conf = get_worker_config()
    output_pull_specs = []
    for tag in _tags:
        output_pull_spec = conf['iib_image_push_template'].format(
            registry=conf['iib_registry'], request_id=tag
        )
        output_pull_specs.append(output_pull_spec)
    return output_pull_specs


def push_index_db_artifact(
    request_id: int,
    from_index: str,
    index_db_path: str,
    operators: List[str],
    operators_in_db: set,
    overwrite_from_index: bool = False,
    request_type: str = 'rm',
) -> Optional[str]:
    """
    Push updated index.db artifact to registry with appropriate tags.

    This function pushes the index.db file to the artifact registry with a request-specific
    tag and optionally to the v4.x tag if overwrite_from_index is True. It captures
    the original digest of the v4.x tag before overwriting for potential rollback.

    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param str index_db_path: Path to the index.db file to push
    :param List[str] operators: List of operators involved in the operation
    :param set operators_in_db: Set of operators that were in the database
    :param bool overwrite_from_index: Whether to overwrite the from_index
    :param str request_type: Type of request (e.g., 'rm', 'add')
    :return: Original digest of v4.x tag if captured, None otherwise
    :rtype: Optional[str]
    """
    original_index_db_digest = None

    if operators_in_db and index_db_path and os.path.exists(index_db_path):
        # Get directory and filename separately to push only the filename
        # This ensures ORAS extracts the file as just "index.db" without
        # directory structure
        index_db_dir = os.path.dirname(index_db_path)
        index_db_filename = os.path.basename(index_db_path)
        log.info('Pushing from directory: %s, filename: %s', index_db_dir, index_db_filename)

        # Push with request_id tag irrespective of overwrite_from_index
        set_request_state(request_id, 'in_progress', 'Pushing updated index database')
        image_name, tag = _get_name_and_tag_from_pullspec(from_index)
        conf = get_worker_config()
        request_artifact_ref = conf['iib_index_db_artifact_template'].format(
            registry=conf['iib_index_db_artifact_registry'],
            tag=f"{_get_artifact_combined_tag(image_name, tag)}-{request_id}",
        )
        artifact_refs = [request_artifact_ref]
        if overwrite_from_index:
            # Get the current digest of v4.x tag before overwriting it
            # This allows us to restore it if anything fails after the push
            v4x_artifact_ref = get_indexdb_artifact_pullspec(from_index)
            log.info('Capturing original digest of %s for potential rollback', v4x_artifact_ref)
            original_index_db_digest = get_image_digest(v4x_artifact_ref)
            log.info('Original index.db digest: %s', original_index_db_digest)
            artifact_refs.append(v4x_artifact_ref)

        for artifact_ref in artifact_refs:
            push_oras_artifact(
                artifact_ref=artifact_ref,
                local_path=index_db_filename,
                cwd=index_db_dir,
                annotations={
                    'request_id': str(request_id),
                    'request_type': request_type,
                    'operators': ','.join(operators),
                },
            )
            log.info('Pushed %s to registry', artifact_ref)

    return original_index_db_digest


def cleanup_on_failure(
    mr_details: Optional[Dict[str, str]],
    last_commit_sha: Optional[str],
    index_git_repo: Optional[str],
    overwrite_from_index: bool,
    request_id: int,
    from_index: str,
    index_repo_map: Dict[str, str],
    original_index_db_digest: Optional[str] = None,
    reason: str = "error",
) -> None:
    """
    Clean up Git changes and index.db artifacts on failure.

    If a merge request was created, it will be closed (since the commit is only in a
    feature branch). If changes were pushed directly to the main branch, the commit
    will be reverted. If the index.db artifact was pushed to the v4.x tag, it will be
    restored to the original digest.

    :param Optional[Dict[str, str]] mr_details: Details of the merge request if one was created
    :param Optional[str] last_commit_sha: The SHA of the last commit
    :param Optional[str] index_git_repo: URL of the Git repository
    :param bool overwrite_from_index: Whether to overwrite the from_index
    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param Dict[str, str] index_repo_map: Mapping of index images to Git repositories
    :param Optional[str] original_index_db_digest: Original digest of index.db before overwrite
    :param str reason: Reason for the cleanup (used in log messages)
    """
    if mr_details and index_git_repo:
        # If we created an MR, just close it (commit is only in feature branch)
        log.info("Closing merge request due to %s", reason)
        try:
            from iib.workers.tasks.git_utils import close_mr

            close_mr(mr_details, index_git_repo)
            log.info("Closed merge request: %s", mr_details.get('mr_url'))
        except Exception as close_error:
            log.warning("Failed to close merge request: %s", close_error)
    elif overwrite_from_index and last_commit_sha:
        # If we pushed directly, revert the commit
        log.error("Reverting commit due to %s", reason)
        try:
            from iib.workers.tasks.git_utils import revert_last_commit

            revert_last_commit(
                request_id=request_id,
                from_index=from_index,
                index_repo_map=index_repo_map,
            )
        except Exception as revert_error:
            log.error("Failed to revert commit: %s", revert_error)
    else:
        log.error("Neither MR nor commit to revert. No cleanup needed for %s", reason)

    # Restore index.db artifact to original digest if it was overwritten
    if original_index_db_digest:
        log.info("Restoring index.db artifact to original digest due to %s", reason)
        try:
            # Get the v4.x artifact reference
            v4x_artifact_ref = get_indexdb_artifact_pullspec(from_index)

            # Extract registry and repository from the pullspec
            # Format: quay.io/namespace/repo:tag -> we need quay.io/namespace/repo
            artifact_name = v4x_artifact_ref.rsplit(':', 1)[0]

            # Use oras copy to restore the old digest to v4.x tag
            # This is a registry-to-registry copy, no download needed
            source_ref = f'{artifact_name}@{original_index_db_digest}'
            log.info("Restoring %s from %s", v4x_artifact_ref, source_ref)

            run_cmd(
                ['oras', 'copy', source_ref, v4x_artifact_ref],
                exc_msg=f'Failed to restore index.db artifact '
                f'from {source_ref} to {v4x_artifact_ref}',
            )
            log.info("Successfully restored index.db artifact to original digest")
        except Exception as restore_error:
            log.error("Failed to restore index.db artifact: %s", restore_error)
