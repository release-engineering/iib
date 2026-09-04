# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains utility functions for containerized IIB operations."""
import json
import logging
import queue
import shutil
import tarfile
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from iib.exceptions import IIBError, FileNotFoundInImageError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.iib_static_types import BundleImage
from iib.workers.tasks.build import _skopeo_copy
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    close_mr,
    create_mr,
    get_git_token,
    get_last_commit_sha,
    merge_mr,
    remote_branch_exists,
    resolve_git_url,
    revert_last_commit,
)
from iib.workers.tasks.konflux_utils import (
    find_pipelinerun,
    get_pipelinerun_image_url,
    wait_for_pipeline_completion,
)
from iib.workers.tasks.oras_utils import (
    _get_index_digest,
    get_index_tag,
    get_indexdb_artifact_pullspec,
    get_imagestream_artifact_pullspec,
    get_oras_artifact,
    push_oras_artifact,
    refresh_indexdb_cache_for_image,
    verify_indexdb_cache_for_image,
)
from iib.workers.tasks.utils import get_image_label, skopeo_inspect

log = logging.getLogger(__name__)


def extract_files_from_image_non_privileged(image: str, src_path: str, dest_path: str) -> None:
    """
    Extract files from container image without podman/docker runtime.

    This function uses skopeo to download the image as OCI layout, then extracts
    the requested path from the image layers. This approach works in non-privileged
    environments without container runtime access.

    :param str image: the pull specification of the container image
    :param str src_path: the full path within the container image to copy from
    :param str dest_path: the full path on the local host to copy into
    :raises IIBError: if the extraction fails or src_path is not found
    """
    # Create temporary directory for OCI layout
    with tempfile.TemporaryDirectory(prefix='iib-extract-') as temp_dir:
        temp_path = Path(temp_dir)
        oci_dir = temp_path / 'oci'
        oci_dir.mkdir(parents=True, exist_ok=True)

        # Download image as OCI layout using skopeo
        log.info('Downloading image %s as OCI layout', image)
        _skopeo_copy(
            source=f'docker://{image}',
            destination=f'oci:{oci_dir}',
            copy_all=False,
            exc_msg=f'Failed to download image {image} as OCI layout',
        )

        # Read OCI index to find the manifest
        index_path = oci_dir / 'index.json'
        if not index_path.exists():
            raise IIBError(f'OCI index.json not found at {index_path}')

        with open(index_path, 'r') as f:
            index = json.load(f)

        # Get the manifest digest from the index
        manifests = index.get('manifests', [])
        if not manifests:
            raise IIBError(f'No manifests found in OCI index for image {image}')

        manifest_digest = manifests[0]['digest'].replace('sha256:', '')
        manifest_path = oci_dir / 'blobs' / 'sha256' / manifest_digest

        # Read manifest to get layer information
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        layers = manifest.get('layers', [])
        if not layers:
            raise IIBError(f'No layers found in manifest for image {image}')

        # Create extraction directory to build the filesystem
        extract_dir = temp_path / 'rootfs'
        extract_dir.mkdir(parents=True, exist_ok=True)

        # Extract each layer in order to build the complete filesystem
        log.info('Extracting %d layers from image %s', len(layers), image)
        for layer in layers:
            layer_digest = layer['digest'].replace('sha256:', '')
            layer_path = oci_dir / 'blobs' / 'sha256' / layer_digest

            if not layer_path.exists():
                raise IIBError(f'Layer blob not found at {layer_path}')

            # Extract layer tar.gz to build filesystem
            try:
                with tarfile.open(layer_path, 'r:gz') as tar:
                    # Extract all members safely with path traversal protection
                    tar.extractall(path=extract_dir, filter='data')
            except Exception as e:
                raise IIBError(f'Failed to extract layer {layer_digest}: {e}')

        # Normalize src_path (remove leading slash for filesystem access)
        normalized_src = src_path.lstrip('/')
        source_full_path = extract_dir / normalized_src

        # Verify the requested path exists in the extracted filesystem.
        # Raise the specific FileNotFoundInImageError (a subclass of IIBError) so
        # callers can distinguish a genuinely absent path from a real extraction
        # failure (registry, OCI parsing, layer, or tar error), which all raise
        # plain IIBError above.
        if not source_full_path.exists():
            raise FileNotFoundInImageError(
                f'Path {src_path} not found in image {image}. '
                f'Looked for {source_full_path} in extracted filesystem.'
            )

        # Copy the requested path to destination
        dest = Path(dest_path)
        log.info('Copying %s from image to %s', src_path, dest_path)
        if source_full_path.is_dir():
            # If source is a directory, copy its contents
            shutil.copytree(source_full_path, dest, dirs_exist_ok=True)
        else:
            # If source is a file, copy the file
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_full_path, dest)

        log.info('Successfully extracted %s from image %s to %s', src_path, image, dest_path)


def extract_catalog_and_db_from_image(from_index_resolved: str, temp_dir: str) -> Tuple[str, str]:
    """
    Extract FBC configs and index.db from an index image, unprivileged.

    Used on the divergent-tag path where no git branch / ORAS artifact exists yet.
    The image is the source of truth for its own content.

    The caller must pass the digest-resolved pullspec (``from_index_resolved``),
    not the mutable tag, so the extracted content matches the image the request
    already inspected during prebuild (OPM version, build metadata) and so the
    repeated image reads here cannot disagree with each other.

    Only the FBC configs and the hidden index.db are extracted. The hidden db is
    the sole source of truth for the SQLite index; there is no fallback to the
    labeled database path and no synthesised empty db. An image that carries no
    hidden index.db has not been onboarded to the containerized build flow and
    the request is failed so the image can be onboarded first.

    :param str from_index_resolved: The digest-resolved from_index image pullspec.
    :param str temp_dir: Base temp directory for extraction.
    :return: Tuple of (configs_dir_path, index_db_path).
    :rtype: Tuple[str, str]
    :raises IIBError: If the image has no FBC configs label, if it carries no
        hidden index.db, or if the hidden-db extraction fails for any other
        reason (e.g. a registry, OCI parsing, layer, or tar error).
    """
    configs_label = get_image_label(
        from_index_resolved, 'operators.operatorframework.io.index.configs.v1'
    )
    if not configs_label:
        raise IIBError(f"Index image {from_index_resolved} does not contain a file-based catalog.")

    configs_dir = str(Path(temp_dir) / 'extracted_configs')
    log.info(
        'Extracting FBC configs from %s (label path %s) to %s',
        from_index_resolved,
        configs_label,
        configs_dir,
    )
    extract_files_from_image_non_privileged(from_index_resolved, configs_label, configs_dir)

    index_db_path = str(Path(temp_dir) / 'extracted_index.db')
    conf = get_worker_config()
    hidden_db_path = conf['hidden_index_db_path']

    try:
        # The hidden db is the only source of truth for the SQLite index.
        log.info(
            'Extracting hidden index.db from %s (image path %s) to %s',
            from_index_resolved,
            hidden_db_path,
            index_db_path,
        )
        extract_files_from_image_non_privileged(from_index_resolved, hidden_db_path, index_db_path)
    except FileNotFoundInImageError:
        raise IIBError(
            f"No index.db found in image {from_index_resolved} at hidden path "
            f"{hidden_db_path}. Onboard the image to build."
        )

    log.info('Extracted FBC configs to %s and index.db to %s', configs_dir, index_db_path)
    return configs_dir, index_db_path


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
        log.info('ImageStream cache is enabled. Checking cache sync status.')
        # Only ImageStream-specific operations are inside the try/except.
        # If the ImageStream pull succeeds, return early. Otherwise fall
        # through to the Quay pull below so that Quay failures are never
        # masked as ImageStream cache errors.
        try:
            if verify_indexdb_cache_for_image(from_index):
                log.info('Index.db cache is synced. Pulling from ImageStream.')
                imagestream_ref = get_imagestream_artifact_pullspec(from_index)
                artifact_dir = get_oras_artifact(
                    imagestream_ref,
                    temp_dir,
                )
                return artifact_dir
            else:
                # Cache is stale — refresh it for future requests
                log.info('Index.db cache is not synced. Refreshing cache.')
                refresh_indexdb_cache_for_image(from_index)
        except IIBError as e:
            log.warning('ImageStream cache access failed, falling back to Quay: %s', e)
    else:
        log.info('ImageStream cache is disabled. Pulling index.db artifact directly from registry.')

    # Pull directly from Quay — either cache is disabled, stale, or unavailable
    artifact_ref = get_indexdb_artifact_pullspec(from_index)
    log.info('Pulling index.db artifact %s into %s', artifact_ref, temp_dir)
    try:
        return get_oras_artifact(
            artifact_ref,
            temp_dir,
        )
    except IIBError:
        log.error('index.db artifact %s not found for image %s', artifact_ref, from_index)
        raise IIBError(
            f"No index.db found for the image {from_index} (artifact {artifact_ref}). "
            "Onboard the image to build."
        )


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

    metadata_path = Path(local_repo_path) / '.iib-build-metadata.json'
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    log.info('Written build metadata to %s', str(metadata_path))


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
    output_image: str,
    overwrite_from_index: bool = False,
    request_type: str = 'rm',
) -> None:
    """
    Push the updated index.db artifact, keyed on the built OUTPUT image digest.

    A per-request tag is always pushed for traceability. For overwrite requests
    (which advance the from_index tag to the output image) the current artifact
    ``idb-<output-digest>`` is also pushed (warm-push), so the next request that
    resolves the tag to that digest finds it cached. Content keys are never
    overwritten in place, so no original-digest rollback is required.

    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param str index_db_path: Path to the index.db file to push
    :param List[str] operators: List of operators involved in the operation
    :param str output_image: The built output image pullspec, used to derive the content key
    :param bool overwrite_from_index: Whether to overwrite the from_index
    :param str request_type: Type of request (e.g., 'rm', 'add')
    """
    if not (index_db_path and Path(index_db_path).exists()):
        return

    index_db_file = Path(index_db_path)
    index_db_dir = str(index_db_file.parent)
    index_db_filename = index_db_file.name
    log.info('Pushing from directory: %s, filename: %s', index_db_dir, index_db_filename)

    set_request_state(request_id, 'in_progress', 'Pushing updated index database')
    conf = get_worker_config()
    output_tag = f'idb-{_get_index_digest(output_image)}'

    request_artifact_ref = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'],
        tag=f'{output_tag}-{request_id}',
    )
    artifact_refs = [request_artifact_ref]
    if overwrite_from_index:
        current_artifact_ref = conf['iib_index_db_artifact_template'].format(
            registry=conf['iib_index_db_artifact_registry'],
            tag=output_tag,
        )
        artifact_refs.append(current_artifact_ref)

    annotations = {
        'request_id': str(request_id),
        'request_type': request_type,
        'from_index': from_index,
        'output_image': output_image,
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


def cleanup_on_failure(
    mr_details: Optional[Dict[str, str]],
    last_commit_sha: Optional[str],
    index_git_repo: Optional[str],
    overwrite_from_index: bool,
    request_id: int,
    from_index: str,
    index_repo_map: Dict[str, str],
    reason: str = "error",
) -> None:
    """
    Clean up Git changes on failure.

    If a merge request was created, it will be closed (since the commit is only in a
    feature branch). If changes were pushed directly to the main branch, the commit
    will be reverted. Content-addressed index.db artifacts are immutable, so there is
    nothing to roll back on the artifact side.

    :param Optional[Dict[str, str]] mr_details: Details of the merge request if one was created
    :param Optional[str] last_commit_sha: The SHA of the last commit
    :param Optional[str] index_git_repo: URL of the Git repository
    :param bool overwrite_from_index: Whether to overwrite the from_index
    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param Dict[str, str] index_repo_map: Mapping of index images to Git repositories
    :param str reason: Reason for the cleanup (used in log messages)
    """
    if mr_details and index_git_repo:
        # If we created an MR, just close it (commit is only in feature branch)
        log.info("Closing merge request due to %s", reason)
        try:
            close_mr(mr_details, index_git_repo)
            log.info("Closed merge request: %s", mr_details.get('mr_url'))
        except Exception as close_error:
            log.warning("Failed to close merge request: %s", close_error)
    elif overwrite_from_index and last_commit_sha:
        log.warning(
            "Unexpected: overwrite_from_index cleanup without mr_details. "
            "This code path should not be reached with the unified MR flow."
        )
        log.error("Reverting commit due to %s", reason)
        try:
            revert_last_commit(
                request_id=request_id,
                from_index=from_index,
                index_repo_map=index_repo_map,
            )
        except Exception as revert_error:
            log.error("Failed to revert commit: %s", revert_error)
    else:
        log.error("Neither MR nor commit to revert. No cleanup needed for %s", reason)


@dataclass
class BuildSources:
    """Resolved inputs for a containerized build."""

    index_git_repo: str
    local_git_repo_path: str
    localized_git_catalog_path: str
    index_db_path: Optional[str]  # None => pull from ORAS; set => use extracted db
    target_branch: str
    is_divergent: bool


def prepare_build_sources(
    request_id: int,
    from_index: str,
    from_index_resolved: str,
    temp_dir: str,
    ocp_version: str,
    index_to_gitlab_push_map: Dict[str, str],
    overwrite_from_index: bool,
) -> BuildSources:
    """
    Resolve git repo + branch and decide the normal vs divergent build path.

    Normal path: a branch named after the image tag exists -> build against it
    (overwrite allowed). Divergent path: no branch for the tag -> reject
    overwrite, reuse the base OCP branch's Konflux Component, and seed content
    by extracting configs+index.db from the image.

    Branch selection keys on the mutable tag (``from_index``), but divergent
    content is extracted from the digest-resolved pullspec
    (``from_index_resolved``) so it matches the image the request already
    inspected during prebuild and cannot drift if the tag moves mid-request.

    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec (tag form, used for branch selection)
    :param str from_index_resolved: The digest-resolved from_index pullspec (content source)
    :param str temp_dir: Temporary directory to clone into / extract to
    :param str ocp_version: Base OCP version branch, e.g. "v4.19"
    :param Dict[str, str] index_to_gitlab_push_map: Mapping of index images to Git repositories
    :param bool overwrite_from_index: Whether the request wants to overwrite from_index
    :return: The resolved build sources
    :rtype: BuildSources
    :raises IIBError: if the git mapping is missing, overwrite is requested on
        the divergent path, or the base OCP branch is not onboarded.
    """
    index_git_repo = resolve_git_url(from_index=from_index, index_repo_map=index_to_gitlab_push_map)
    if not index_git_repo:
        raise IIBError(
            f"Git repository mapping not found for from_index: {from_index}. "
            "index_to_gitlab_push_map is required."
        )
    token_name, git_token = get_git_token(index_git_repo)

    tag = get_index_tag(from_index)
    set_request_state(request_id, 'in_progress', 'Cloning Git repository')

    if remote_branch_exists(index_git_repo, tag, token_name, git_token):
        # Normal path — build against the tag branch, pull index.db from ORAS.
        target_branch = tag
        local_git_repo_path = Path(temp_dir) / 'git' / target_branch
        local_git_repo_path.mkdir(parents=True, exist_ok=True)
        clone_git_repo(
            index_git_repo, target_branch, token_name, git_token, str(local_git_repo_path)
        )
        catalog_path = local_git_repo_path / 'configs'
        if not catalog_path.exists():
            raise IIBError(f"Catalogs directory not found in {local_git_repo_path}")
        return BuildSources(
            index_git_repo=index_git_repo,
            local_git_repo_path=str(local_git_repo_path),
            localized_git_catalog_path=str(catalog_path),
            index_db_path=None,
            target_branch=target_branch,
            is_divergent=False,
        )

    # Divergent path.
    if overwrite_from_index:
        raise IIBError(
            f"Cannot overwrite tag '{tag}' of {from_index}: no onboarded branch/Component "
            f"exists for it. Onboard a '{tag}' branch to enable overwrite."
        )

    target_branch = ocp_version
    if not remote_branch_exists(index_git_repo, target_branch, token_name, git_token):
        raise IIBError(
            f"Base OCP branch '{target_branch}' is not onboarded for {index_git_repo}. "
            "Onboard the index before building divergent tags."
        )

    local_git_repo_path = Path(temp_dir) / 'git' / target_branch
    local_git_repo_path.mkdir(parents=True, exist_ok=True)
    clone_git_repo(index_git_repo, target_branch, token_name, git_token, str(local_git_repo_path))

    # Content from the image (source of truth), scaffolding from the OCP branch.
    # Extract from the digest-resolved pullspec, not the mutable tag.
    extracted_configs, extracted_db = extract_catalog_and_db_from_image(
        from_index_resolved, temp_dir
    )
    catalog_path = local_git_repo_path / 'configs'
    if catalog_path.exists():
        shutil.rmtree(catalog_path)
    shutil.copytree(extracted_configs, catalog_path)

    return BuildSources(
        index_git_repo=index_git_repo,
        local_git_repo_path=str(local_git_repo_path),
        localized_git_catalog_path=str(catalog_path),
        index_db_path=extracted_db,
        target_branch=target_branch,
        is_divergent=True,
    )


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
    local_git_repo_path = Path(temp_dir) / 'git' / branch
    local_git_repo_path.mkdir(parents=True, exist_ok=True)

    clone_git_repo(index_git_repo, branch, token_name, git_token, str(local_git_repo_path))

    localized_git_catalog_path = local_git_repo_path / 'configs'
    if not localized_git_catalog_path.exists():
        raise IIBError(f"Catalogs directory not found in {local_git_repo_path}")

    return index_git_repo, str(local_git_repo_path), str(localized_git_catalog_path)


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
    artifact_index_db_file = Path(artifact_dir) / "index.db"

    log.debug("Artifact DB path %s", artifact_index_db_file)
    if not artifact_index_db_file.exists():
        log.error("Index.db file not found at %s", artifact_index_db_file)
        raise IIBError(f"Index.db file not found at {artifact_index_db_file}")

    return str(artifact_index_db_file)


def git_commit_and_create_mr(
    request_id: int,
    local_git_repo_path: str,
    index_git_repo: str,
    branch: str,
    commit_message: str,
) -> Tuple[Dict[str, str], str]:
    """
    Commit changes and trigger Konflux pipeline by creating an MR.

    All requests (both overwrite and throw-away) go through the MR path.
    The environment name from worker config is used to prefix the source branch,
    enabling PaC CEL expression routing to the correct Konflux tenant.

    :param int request_id: The IIB request ID
    :param str local_git_repo_path: Path to local Git repository
    :param str index_git_repo: URL of the Git repository
    :param str branch: Git branch name
    :param str commit_message: Commit message to use
    :return: Tuple of (mr_details, last_commit_sha)
    :rtype: Tuple[Dict[str, str], str]
    """
    set_request_state(request_id, 'in_progress', 'Committing changes to Git repository')
    log.info("Committing changes to Git repository. Triggering KONFLUX pipeline.")

    conf = get_worker_config()
    environment_name = conf.get('iib_environment_name')

    mr_details = create_mr(
        request_id=request_id,
        local_repo_path=local_git_repo_path,
        repo_url=index_git_repo,
        branch=branch,
        commit_message=commit_message,
        environment_name=environment_name,
    )
    log.info("Created merge request: %s", mr_details.get('mr_url'))

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


def merge_mr_after_build(
    mr_details: Dict[str, str],
    index_git_repo: str,
) -> str:
    """
    Merge the MR after a successful Konflux build (overwrite flow).

    On merge failure, closes the MR and raises IIBError so the request
    is marked as failed.

    :param Dict[str, str] mr_details: Details of the merge request
    :param str index_git_repo: URL of the Git repository
    :return: The merge commit SHA
    :rtype: str
    :raises IIBError: If the merge fails
    """
    try:
        merge_commit_sha = merge_mr(mr_details, index_git_repo)
        log.info("Successfully merged MR %s, commit: %s", mr_details.get('mr_id'), merge_commit_sha)
        return merge_commit_sha
    except IIBError as e:
        log.error("Failed to merge MR %s after build: %s", mr_details.get('mr_id'), e)
        try:
            close_mr(mr_details, index_git_repo)
            log.info("Closed MR %s after merge failure", mr_details.get('mr_id'))
        except Exception as close_error:
            log.warning("Failed to close MR after merge failure: %s", close_error)
        raise IIBError(f"Failed to merge MR after successful build: {e}")
