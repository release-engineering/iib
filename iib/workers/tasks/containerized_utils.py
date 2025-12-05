# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains utility functions for containerized IIB operations."""
import json
import logging
import os
import queue
import threading
from typing import Dict, List, Optional, Tuple, Union

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.iib_static_types import BundleImage
from iib.workers.tasks.build import _skopeo_copy
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    close_mr,
    commit_and_push,
    create_mr,
    get_git_token,
    get_last_commit_sha,
    resolve_git_url,
)
from iib.workers.tasks.konflux_utils import (
    find_pipelinerun,
    get_pipelinerun_image_url,
    wait_for_pipeline_completion,
)
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
                b_path = str(bundle["bundlePath"]) if isinstance(bundle, dict) else str(bundle)
                skopeo_inspect(f'docker://{b_path}', '--raw', return_json=False)
        except IIBError as e:
            self.bundle = bundle
            bundle_str = (
                bundle["bundlePath"]
                if bundle and isinstance(bundle, dict) and "bundlePath" in bundle
                else bundle
            )
            log.error(f"Error validating bundle {bundle_str}: {e}")
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
            if t.bundle and isinstance(t.bundle, dict) and "bundlePath" in t.bundle:
                bundle_str = t.bundle["bundlePath"]
            else:
                bundle_str = str(t.bundle) if t.bundle else "unknown"
            log.error(f"Error validating bundle {bundle_str}: {t.exception}")
            raise IIBError(f"Error validating bundle {bundle_str}: {t.exception}")


def validate_bundles_in_parallel(
    bundles: Union[List[BundleImage], List[str]], threads=5, wait=True
) -> Optional[List[ValidateBundlesThread]]:
    """
    Validate bundles in parallel.

    :param list bundles: the list of bundles or bundle pullspecsto validate
    :param int threads: the number of threads to use
    :param bool wait: whether to wait for all threads to complete
    :return: the list of threads if not waiting, None otherwise
    :rtype: Optional[List[ValidateBundlesThread]]
    """
    bundles_queue: queue.Queue[Union[BundleImage, str]] = queue.Queue()

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
    arches: set,
) -> None:
    """
    Write build metadata file for Konflux build task.

    This function creates a JSON metadata file that contains information needed by the
    Konflux build task, including OPM version, labels, binary image, request ID, and arches.

    :param str local_repo_path: Path to local Git repository
    :param str opm_version: OPM version string (e.g., "opm-1.40.0")
    :param str ocp_version: OCP version (e.g., "v4.19")
    :param str distribution_scope: Distribution scope (e.g., "PROD")
    :param str binary_image: Binary image pullspec
    :param int request_id: Request ID
    :param set arches: Set of architectures (e.g., {'amd64', 's390x'})
    """
    metadata = {
        'opm_version': opm_version,
        'labels': {
            'com.redhat.index.delivery.version': ocp_version,
            'com.redhat.index.delivery.distribution_scope': distribution_scope,
        },
        'binary_image': binary_image,
        'request_id': request_id,
        'arches': sorted(list(arches)),
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
    :param bool overwrite_from_index: Whether to overwrite the from_index
    :param str request_type: Type of request (e.g., 'rm', 'add')
    :return: Original digest of v4.x tag if captured, None otherwise
    :rtype: Optional[str]
    """
    original_index_db_digest = None

    if index_db_path and os.path.exists(index_db_path):
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

        # Build annotations - only include operators if not empty
        annotations = {
            'request_id': str(request_id),
            'request_type': request_type,
        }
        if operators:
            annotations['operators'] = ','.join(operators)

        for artifact_ref in artifact_refs:
            push_oras_artifact(
                artifact_ref=artifact_ref,
                local_path=index_db_filename,
                cwd=index_db_dir,
                annotations=annotations.copy(),
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


def prepare_git_repository_for_build(
    request_id: int,
    from_index: str,
    temp_dir: str,
    branch: str,
    index_to_gitlab_push_map: Dict[str, str],
) -> Tuple[str, str, str]:
    """
    Set up and clone Git repository for containerized build.

    This function resolves the Git repository URL from the from_index,
    gets the Git token, clones the repository, and verifies the configs directory exists.

    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param str temp_dir: Temporary directory where repository will be cloned
    :param str branch: Git branch to clone
    :param Dict[str, str] index_to_gitlab_push_map: Mapping of index images to Git repositories
    :return: Tuple of (index_git_repo, local_git_repo_path, localized_git_catalog_path)
    :rtype: Tuple[str, str, str]
    :raises IIBError: If Git repository cannot be resolved or configs directory not found
    """
    # Get Git repository information
    index_git_repo = resolve_git_url(from_index=from_index, index_repo_map=index_to_gitlab_push_map)
    if not index_git_repo:
        raise IIBError(
            f"Git repository mapping not found for from_index: {from_index}. "
            "index_to_gitlab_push_map is required."
        )
    log.info("Git repo for %s: %s", from_index, index_git_repo)

    token_name, git_token = get_git_token(index_git_repo)

    # Clone Git repository
    set_request_state(request_id, 'in_progress', 'Cloning Git repository')
    local_git_repo_path = os.path.join(temp_dir, 'git', branch)
    os.makedirs(local_git_repo_path, exist_ok=True)

    clone_git_repo(index_git_repo, branch, token_name, git_token, local_git_repo_path)

    localized_git_catalog_path = os.path.join(local_git_repo_path, 'configs')
    if not os.path.exists(localized_git_catalog_path):
        raise IIBError(f"Catalogs directory not found in {local_git_repo_path}")

    return index_git_repo, local_git_repo_path, localized_git_catalog_path


def fetch_and_verify_index_db_artifact(
    from_index: str,
    temp_dir: str,
) -> str:
    """
    Pull index.db artifact and verify it exists.

    This function pulls the index.db artifact from the registry and verifies
    that the file exists in the expected location.

    :param str from_index: The from_index pullspec
    :param str temp_dir: Temporary directory where artifact will be extracted
    :return: Path to the index.db file
    :rtype: str
    :raises IIBError: If index.db file not found after pulling
    """
    artifact_dir = pull_index_db_artifact(from_index, temp_dir)
    artifact_index_db_file = os.path.join(artifact_dir, "index.db")

    log.debug("Artifact DB path %s", artifact_index_db_file)
    if not os.path.exists(artifact_index_db_file):
        log.error("Index.db file not found at %s", artifact_index_db_file)
        raise IIBError(f"Index.db file not found at {artifact_index_db_file}")

    return artifact_index_db_file


def git_commit_and_create_mr_or_push(
    request_id: int,
    local_git_repo_path: str,
    index_git_repo: str,
    branch: str,
    commit_message: str,
    overwrite_from_index: bool = False,
) -> Tuple[Optional[Dict[str, str]], str]:
    """
    Commit changes and trigger Konflux pipeline by creating MR or pushing directly.

    If overwrite_from_index is False, creates a merge request (for throw-away
    requests). Otherwise, pushes directly to the branch. Returns the merge request details
    and last commit SHA.

    :param int request_id: The IIB request ID
    :param str local_git_repo_path: Path to local Git repository
    :param str index_git_repo: URL of the Git repository
    :param str branch: Git branch name
    :param str commit_message: Commit message to use
    :param bool overwrite_from_index: Whether to overwrite from_index (push directly vs MR)
    :return: Tuple of (mr_details, last_commit_sha)
    :rtype: Tuple[Optional[Dict[str, str]], str]
    """
    set_request_state(request_id, 'in_progress', 'Committing changes to Git repository')
    log.info("Committing changes to Git repository. Triggering KONFLUX pipeline.")

    mr_details = None
    # Determine if this is a throw-away request (no overwrite_from_index)
    if not overwrite_from_index:
        # Create MR for throw-away requests
        mr_details = create_mr(
            request_id=request_id,
            local_repo_path=local_git_repo_path,
            repo_url=index_git_repo,
            branch=branch,
            commit_message=commit_message,
        )
        log.info("Created merge request: %s", mr_details.get('mr_url'))
    else:
        # Push directly to the branch
        commit_and_push(
            request_id=request_id,
            local_repo_path=local_git_repo_path,
            repo_url=index_git_repo,
            branch=branch,
            commit_message=commit_message,
        )

    # Get commit SHA before waiting for the pipeline (while the temp directory still exists)
    last_commit_sha = get_last_commit_sha(local_repo_path=local_git_repo_path)

    return mr_details, last_commit_sha


def monitor_pipeline_and_extract_image(request_id: int, last_commit_sha: str) -> str:
    """
    Wait for Konflux pipeline to complete and return the built image URL.

    This function finds the pipelinerun associated with the commit SHA,
    waits for it to complete, and extracts the built image URL from the results.

    :param int request_id: The IIB request ID
    :param str last_commit_sha: SHA of the last commit that triggered the pipeline
    :return: URL of the built image
    :rtype: str
    :raises IIBError: If pipelinerun not found or pipeline fails
    """
    # Wait for Konflux pipeline
    set_request_state(request_id, 'in_progress', 'Waiting on KONFLUX build')

    # find_pipelinerun has retry decorator to handle delays in pipelinerun creation
    pipelines = find_pipelinerun(last_commit_sha)

    # Get the first pipelinerun (should typically be only one)
    pipelinerun = pipelines[0]
    pipelinerun_name = pipelinerun.get('metadata', {}).get('name')
    if not pipelinerun_name:
        raise IIBError("Pipelinerun name not found in pipeline metadata")

    run = wait_for_pipeline_completion(pipelinerun_name)

    return get_pipelinerun_image_url(pipelinerun_name, run)


def replicate_image_to_tagged_destinations(
    request_id: int,
    image_url: str,
    build_tags: Optional[List[str]] = None,
) -> List[str]:
    """
    Copy built index from Konflux to IIB registry with all required tags.

    This function builds the list of output pull specs and copies the built
    image from Konflux to each spec using skopeo.

    :param int request_id: The IIB request ID
    :param str image_url: URL of the built image from Konflux
    :param Optional[List[str]] build_tags: Additional tags to apply
    :return: List of output pull specifications that were copied to
    :rtype: List[str]
    """
    set_request_state(request_id, 'in_progress', 'Copying built index to IIB registry')

    output_pull_specs = get_list_of_output_pullspec(request_id, build_tags)

    # Copy the built index from Konflux to all output pull specs
    for spec in output_pull_specs:
        _skopeo_copy(
            source=f'docker://{image_url}',
            destination=f'docker://{spec}',
            copy_all=True,
            exc_msg=f'Failed to copy built index from Konflux to {spec}',
        )
        log.info("Successfully copied image to %s", spec)

    return output_pull_specs


def cleanup_merge_request_if_exists(
    mr_details: Optional[Dict[str, str]],
    index_git_repo: Optional[str],
) -> None:
    """
    Close merge request if it was created.

    This function attempts to close a merge request and logs a warning
    if the operation fails.

    :param Optional[Dict[str, str]] mr_details: Details of the merge request
    :param Optional[str] index_git_repo: URL of the Git repository
    """
    if mr_details and index_git_repo:
        try:
            close_mr(mr_details, index_git_repo)
            log.info("Closed merge request: %s", mr_details.get('mr_url'))
        except IIBError as e:
            log.warning("Failed to close merge request: %s", e)
