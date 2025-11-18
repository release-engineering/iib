# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains utility functions for containerized IIB operations."""
import json
import logging
import os
from typing import Dict, Optional

from iib.workers.config import get_worker_config
from iib.workers.tasks.oras_utils import (
    get_indexdb_artifact_pullspec,
    get_imagestream_artifact_pullspec,
    get_oras_artifact,
    refresh_indexdb_cache_for_image,
    verify_indexdb_cache_for_image,
)
from iib.workers.tasks.utils import run_cmd

log = logging.getLogger(__name__)


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
