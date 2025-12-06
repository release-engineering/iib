# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Optional

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.build import (
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    cleanup_merge_request_if_exists,
    cleanup_on_failure,
    fetch_and_verify_index_db_artifact,
    git_commit_and_create_mr_or_push,
    monitor_pipeline_and_extract_image,
    prepare_git_repository_for_build,
    push_index_db_artifact,
    replicate_image_to_tagged_destinations,
    write_build_metadata,
)
from iib.workers.tasks.opm_operations import (
    Opm,
    _opm_registry_rm,
    get_operator_package_list,
    opm_validate,
)
from iib.workers.tasks.oras_utils import (
    _get_artifact_combined_tag,
    _get_name_and_tag_from_pullspec,
    get_oras_artifact,
)
from iib.workers.tasks.utils import (
    prepare_request_for_build,
    request_logger,
    reset_docker_config,
    RequestConfigCreateIndexImage,
)

__all__ = ['handle_containerized_create_empty_index_request']

log = logging.getLogger(__name__)


def _create_empty_index_db_from_source(
    request_id: int,
    from_index: str,
    temp_dir: str,
) -> Path:
    """
    Create an empty index.db by fetching from from_index and removing all operators.

    This is a fallback path when the pre-built empty index.db artifact is not available.

    :param int request_id: The IIB request ID
    :param str from_index: The from_index pullspec
    :param str temp_dir: Temporary directory for operations
    :return: Path to the created empty index.db file
    :rtype: Path
    :raises IIBError: If the process fails
    """
    set_request_state(request_id, 'in_progress', 'Creating empty index database from from_index')

    # Fetch the index.db from from_index
    log.info('Fetching index.db from %s', from_index)
    index_db_path = Path(
        fetch_and_verify_index_db_artifact(
            from_index=from_index,
            temp_dir=temp_dir,
        )
    )

    # Get all operator packages from the index.db
    log.info('Extracting all operator packages from index.db')
    operators_in_db = get_operator_package_list(str(index_db_path), temp_dir)

    if operators_in_db:
        log.info('Removing all operators from index.db: %s', operators_in_db)
        # Remove all operators from index.db to create an empty one
        try:
            _opm_registry_rm(
                index_db_path=str(index_db_path),
                operators=operators_in_db,
                base_dir=temp_dir,
            )
        except IIBError as e:
            if 'Error deleting packages from database' in str(e):
                log.info('Enable permissive mode for opm registry rm')
                _opm_registry_rm(
                    index_db_path=str(index_db_path),
                    operators=operators_in_db,
                    base_dir=temp_dir,
                    permissive=True,
                )
            else:
                raise
        log.info('Successfully created empty index.db by removing all operators')
    else:
        log.info('Index.db is already empty, no operators to remove')

    return index_db_path


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.build.handle_containerized_create_empty_index_request",
    attributes=get_binary_versions(),
)
def handle_containerized_create_empty_index_request(
    from_index: str,
    request_id: int,
    binary_image: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
) -> None:
    """
    Coordinate the work needed to create empty index using containerized workflow.

    This function uses Git-based workflows and Konflux pipelines instead of local builds.
    The index.db is expected to already be tagged with the 'empty' tag in the registry.

    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param dict labels: the dict of labels required to be added to a new index image
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to push their catalogs into GitLab.
    :raises IIBError: if the index image build fails or empty index.db tag not found.
    """
    reset_docker_config()
    set_request_state(request_id, 'in_progress', 'Preparing request for build')

    # Prepare request
    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigCreateIndexImage(
            _binary_image=binary_image,
            from_index=from_index,
            binary_image_config=binary_image_config,
        ),
    )

    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']
    ocp_version = prebuild_info['ocp_version']
    distribution_scope = prebuild_info['distribution_scope']
    arches = prebuild_info['arches']

    # Set OPM version
    Opm.set_opm_version(from_index_resolved)
    opm_version = Opm.opm_version

    # Add labels to prebuild_info
    prebuild_info['labels'] = labels

    _update_index_image_build_state(request_id, prebuild_info)

    mr_details: Optional[Dict[str, str]] = None
    local_git_repo_path: Optional[str] = None
    index_git_repo: Optional[str] = None
    last_commit_sha: Optional[str] = None
    output_pull_spec: Optional[str] = None
    original_index_db_digest: Optional[str] = None

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        branch = ocp_version

        # Set up and clone Git repository
        (
            index_git_repo,
            local_git_repo_path,
            localized_git_catalog_path,
        ) = prepare_git_repository_for_build(
            request_id=request_id,
            from_index=from_index,
            temp_dir=temp_dir,
            branch=branch,
            index_to_gitlab_push_map=index_to_gitlab_push_map or {},
        )

        # Fetch empty index.db artifact tagged with 'empty'
        set_request_state(request_id, 'in_progress', 'Fetching empty index database')
        conf = get_worker_config()
        empty_tag = conf.get('iib_empty_index_db_tag', 'empty')

        # Construct the pullspec for the empty index.db artifact
        image_name, _ = _get_name_and_tag_from_pullspec(from_index)
        empty_artifact_ref = conf['iib_index_db_artifact_template'].format(
            registry=conf['iib_index_db_artifact_registry'],
            tag=_get_artifact_combined_tag(image_name, empty_tag),
        )

        log.info('Fetching empty index.db from %s', empty_artifact_ref)

        try:
            artifact_dir = get_oras_artifact(
                empty_artifact_ref,
                temp_dir,
            )
            index_db_path = Path(artifact_dir) / "index.db"
            if not index_db_path.is_file():
                raise IIBError(
                    f"Empty index.db file not found at {index_db_path} "
                    f"after fetching from {empty_artifact_ref}"
                )
            log.info('Successfully fetched empty index.db from %s', empty_artifact_ref)
        except IIBError as e:
            # Fallback: Create empty index.db from from_index by removing all operators
            log.warning(
                f"Failed to fetch empty index.db with tag '{empty_tag}': {e}. "
                f"Falling back to creating empty index.db from {from_index}"
            )
            index_db_path = _create_empty_index_db_from_source(
                request_id=request_id,
                from_index=from_index,
                temp_dir=temp_dir,
            )

        # Create empty FBC catalog directory
        # The index.db is already empty, so we want an empty catalog as well
        set_request_state(request_id, 'in_progress', 'Creating empty FBC catalog directory')

        localized_catalog_path = Path(localized_git_catalog_path)
        if localized_catalog_path.is_dir():
            log.info('Removing all contents from catalog directory to create empty catalog')
            shutil.rmtree(localized_catalog_path)

        localized_catalog_path.mkdir(parents=True, exist_ok=True)
        log.info('Created empty catalog directory at %s', localized_catalog_path)

        # Create a placeholder file so Git tracks the empty directory
        gitkeep_file = localized_catalog_path / '.gitkeep'
        with open(gitkeep_file, 'w') as f:
            f.write('')
        log.info('Created .gitkeep file in empty catalog directory')

        # Create empty catalog directory structure for validation
        fbc_dir_path = Path(temp_dir) / 'catalog'
        if fbc_dir_path.is_dir():
            shutil.rmtree(fbc_dir_path)
        # Copy cleaned catalog to correct location expected in Dockerfile
        shutil.copytree(localized_catalog_path, fbc_dir_path)

        # Validate empty catalog
        set_request_state(request_id, 'in_progress', 'Validating empty catalog')
        opm_validate(str(fbc_dir_path))

        # Write build metadata to a file to be added with the commit
        set_request_state(request_id, 'in_progress', 'Writing build metadata')

        # Write standard build metadata
        write_build_metadata(
            local_git_repo_path,
            opm_version,
            ocp_version,
            distribution_scope,
            binary_image_resolved,
            request_id,
            arches,
        )

        # Add custom labels to metadata file if provided
        # The write_build_metadata function writes standard labels,
        # but we need to update it to include custom labels
        if labels:
            metadata_path = Path(local_git_repo_path) / '.iib-build-metadata.json'
            if not metadata_path.is_file():
                raise IIBError(
                    f"Build metadata file not found at {metadata_path}. "
                    "write_build_metadata should have created it."
                )
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            metadata['labels'].update(labels)
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

        try:
            # Commit changes and create MR or push directly
            # For create_empty_index, overwrite_from_index is always False (throw-away request)
            mr_details, last_commit_sha = git_commit_and_create_mr_or_push(
                request_id=request_id,
                local_git_repo_path=local_git_repo_path,
                index_git_repo=index_git_repo,
                branch=branch,
                commit_message=(
                    f"IIB: Create empty index for request {request_id}\n\n"
                    f"Creating empty index image from {from_index}"
                ),
                overwrite_from_index=False,  # Always False for create_empty_index
            )

            # Wait for Konflux pipeline and extract built image URL
            image_url = monitor_pipeline_and_extract_image(
                request_id=request_id,
                last_commit_sha=last_commit_sha,
            )

            # Copy built index to all output pull specs
            output_pull_specs = replicate_image_to_tagged_destinations(
                request_id=request_id,
                image_url=image_url,
            )

            # Use the first output_pull_spec as the primary one for request updates
            output_pull_spec = output_pull_specs[0]
            # Update request with final output
            if not output_pull_spec:
                raise IIBError(
                    "output_pull_spec was not set. "
                    "This should not happen if the pipeline completed successfully."
                )

            # Update index image pull spec
            _update_index_image_pull_spec(
                output_pull_spec=output_pull_spec,
                request_id=request_id,
                arches=arches,
                from_index=from_index,
                overwrite_from_index=False,  # Always False for create_empty_index
                overwrite_from_index_token=None,
                resolved_prebuild_from_index=from_index_resolved,
                # Passing an empty index_repo_map is intentional. In IIB 1.0, if
                # overwrite_from_index token is given, we push to git by default at the
                # end of a request. In IIB 2.Oh!, the commit is pushed earlier to trigger
                # a Konflux pipelinerun. So the old workflow isn't needed.
                index_repo_map={},
            )

            # Push the empty index.db with request ID tag
            # Since overwrite_from_index is False, this will only push with request_id tag
            # and will not overwrite the v4.x tag
            original_index_db_digest = push_index_db_artifact(
                request_id=request_id,
                from_index=from_index,
                index_db_path=str(index_db_path),
                operators=[],  # Empty list since we're creating an empty index
                overwrite_from_index=False,  # Always False for create_empty_index
                request_type='create_empty_index',
            )

            # Close MR if it was opened
            cleanup_merge_request_if_exists(mr_details, index_git_repo)

            set_request_state(
                request_id,
                'complete',
                'The empty index image was successfully created',
            )
        except Exception as e:
            cleanup_on_failure(
                mr_details=mr_details,
                last_commit_sha=last_commit_sha,
                index_git_repo=index_git_repo,
                overwrite_from_index=False,  # Always False for create_empty_index
                request_id=request_id,
                from_index=from_index,
                index_repo_map=index_to_gitlab_push_map or {},
                original_index_db_digest=original_index_db_digest,
                reason=f"error: {e}",
            )
            raise IIBError(f"Failed to create empty index: {e}")

    # Reset Docker config for the next request. This is a fail safe.
    reset_docker_config()
