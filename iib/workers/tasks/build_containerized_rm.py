# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import shutil
import tempfile
from typing import Dict, List, Optional, Set

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
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
from iib.workers.tasks.fbc_utils import merge_catalogs_dirs
from iib.workers.tasks.opm_operations import (
    Opm,
    opm_registry_rm_fbc,
    opm_validate,
    remove_operator_deprecations,
    verify_operators_exists,
)
from iib.workers.tasks.utils import (
    prepare_request_for_build,
    reset_docker_config,
    request_logger,
    RequestConfigAddRm,
)

__all__ = ['handle_containerized_rm_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.build.handle_containerized_rm_request",
    attributes=get_binary_versions(),
)
def handle_containerized_rm_request(
    operators: List[str],
    request_id: int,
    from_index: str,
    binary_image: Optional[str] = None,
    add_arches: Optional[Set[str]] = None,
    overwrite_from_index: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    distribution_scope: Optional[str] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    build_tags: Optional[List[str]] = None,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
    binary_image_less_arches_allowed_versions: Optional[List[str]] = None,
) -> None:
    """
    Coordinate the work needed to remove the input operators using containerized workflow.

    This function uses Git-based workflows and Konflux pipelines instead of local builds.

    :param list operators: a list of strings representing the name of the operators to
        remove from the index image.
    :param int request_id: the ID of the IIB build request
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param set add_arches: the set of arches to build in addition to the arches ``from_index`` is
        currently built for.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :param list build_tags: List of tags which will be applied to intermediate index images.
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to remove their catalogs from GitLab.
    :param list binary_image_less_arches_allowed_versions: list of versions of the binary image
        that are allowed to build for less arches. Defaults to ``None``.
    :raises IIBError: if the index image build fails.
    """
    reset_docker_config()
    set_request_state(request_id, 'in_progress', 'Preparing request for build')

    # Prepare request
    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigAddRm(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
            binary_image_less_arches_allowed_versions=binary_image_less_arches_allowed_versions,
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

    _update_index_image_build_state(request_id, prebuild_info)

    mr_details: Optional[Dict[str, str]] = None
    local_git_repo_path: Optional[str] = None
    index_git_repo: Optional[str] = None
    operators_in_db: Set[str] = set()
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

        # Pull index.db artifact (uses ImageStream cache if configured, otherwise pulls directly)
        index_db_path = fetch_and_verify_index_db_artifact(
            from_index=from_index,
            temp_dir=temp_dir,
        )

        # Remove operators from /configs
        set_request_state(request_id, 'in_progress', 'Removing operators from catalog')
        for operator in operators:
            operator_path = os.path.join(localized_git_catalog_path, operator)
            if os.path.exists(operator_path):
                log.debug('Removing operator from catalog: %s', operator_path)
                shutil.rmtree(operator_path)

        # Remove operator deprecations
        remove_operator_deprecations(
            from_index_configs_dir=localized_git_catalog_path, operators=operators
        )

        # Check if operators exist in index.db and remove if present
        set_request_state(request_id, 'in_progress', 'Checking and removing from index database')
        operators_in_db_list, index_db_path_verified = verify_operators_exists(
            from_index=None,
            base_dir=temp_dir,
            operator_packages=operators,
            overwrite_from_index_token=overwrite_from_index_token,
            index_db_path=index_db_path,
        )
        operators_in_db = set(operators_in_db_list)

        # Use verified path or fall back to original
        if index_db_path_verified:
            index_db_path = index_db_path_verified

        if operators_in_db:
            log.info('Removing operators %s from index.db', operators_in_db)
            # Remove from index.db and migrate to FBC
            fbc_dir, _ = opm_registry_rm_fbc(
                base_dir=temp_dir,
                from_index=from_index_resolved,
                operators=list[str](operators_in_db),
                index_db_path=index_db_path,
            )

            # rename `catalog` directory because we need to use this name for
            # final destination of catalog (defined in Dockerfile)
            catalog_from_db = os.path.join(temp_dir, 'from_db')
            os.rename(fbc_dir, catalog_from_db)

            # Merge migrated FBC with existing FBC in Git repo
            # overwrite data in `catalog_from_index` by data from `catalog_from_db`
            # this adds changes on not opted in operators to final FBC
            log.info('Merging migrated catalog with Git catalog')
            merge_catalogs_dirs(catalog_from_db, localized_git_catalog_path)

        fbc_dir_path = os.path.join(temp_dir, 'catalog')
        # We need to regenerate file-based catalog because we merged changes
        if os.path.exists(fbc_dir_path):
            shutil.rmtree(fbc_dir_path)
        # Copy catalog to correct location expected in Dockerfile
        # Use copytree instead of move to preserve the configs directory in Git repo
        shutil.copytree(localized_git_catalog_path, fbc_dir_path)

        # Validate merged catalog
        set_request_state(request_id, 'in_progress', 'Validating catalog')
        opm_validate(fbc_dir_path)

        # Write build metadata to a file to be added with the commit
        set_request_state(request_id, 'in_progress', 'Writing build metadata')
        write_build_metadata(
            local_git_repo_path,
            opm_version,
            ocp_version,
            distribution_scope,
            binary_image_resolved,
            request_id,
            arches,
        )

        try:
            # Commit changes and create MR or push directly
            operators_str = ', '.join(operators)
            mr_details, last_commit_sha = git_commit_and_create_mr_or_push(
                request_id=request_id,
                local_git_repo_path=local_git_repo_path,
                index_git_repo=index_git_repo,
                branch=branch,
                commit_message=(
                    f"IIB: Remove operators for request {request_id}\n\n"
                    f"Operators: {operators_str}"
                ),
                overwrite_from_index=overwrite_from_index,
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
                build_tags=build_tags,
            )

            # Use the first output_pull_spec as the primary one for request updates
            output_pull_spec = output_pull_specs[0]
            # Update request with final output
            if not output_pull_spec:
                raise IIBError(
                    "output_pull_spec was not set. "
                    "This should not happen if the pipeline completed successfully."
                )

            # Send an empty index_repo_map because the Git repository is already
            # updated with the changes
            _update_index_image_pull_spec(
                output_pull_spec=output_pull_spec,
                request_id=request_id,
                arches=arches,
                from_index=from_index,
                overwrite_from_index=overwrite_from_index,
                overwrite_from_index_token=overwrite_from_index_token,
                resolved_prebuild_from_index=from_index_resolved,
                add_or_rm=True,
                is_image_fbc=True,
                # Passing an empty index_repo_map is intentional. In IIB 1.0, if
                # overwrite_from_index token is given, we push to git by default at the
                # end of a request. In IIB 2.0, the commit is pushed earlier to trigger
                # a Konflux pipelinerun. So the old workflow isn't needed.
                index_repo_map={},
                rm_operators=operators,
            )

            # Push updated index.db if overwrite_from_index is True
            # We can push it directly from temp_dir since we're still inside the
            # context manager. Do it as the last step to avoid rolling back the
            # index.db file if the pipeline fails.
            original_index_db_digest = push_index_db_artifact(
                request_id=request_id,
                from_index=from_index,
                index_db_path=index_db_path,
                operators=operators,
                overwrite_from_index=overwrite_from_index,
                request_type='rm',
            )

            # Close MR if it was opened
            cleanup_merge_request_if_exists(mr_details, index_git_repo)

            operators_str = ', '.join(operators)
            set_request_state(
                request_id,
                'complete',
                f"The operator(s) {operators_str} were successfully removed "
                "from the index image",
            )
        except Exception as e:
            cleanup_on_failure(
                mr_details=mr_details,
                last_commit_sha=last_commit_sha,
                index_git_repo=index_git_repo,
                overwrite_from_index=overwrite_from_index,
                request_id=request_id,
                from_index=from_index,
                index_repo_map=index_to_gitlab_push_map or {},
                original_index_db_digest=original_index_db_digest,
                reason=f"error: {e}",
            )
            raise IIBError(f"Failed to remove operators: {e}")

    # Reset Docker config for the next request. This is a fail safe.
    reset_docker_config()
