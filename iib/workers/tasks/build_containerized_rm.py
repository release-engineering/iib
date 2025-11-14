# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import os
import shutil
import tempfile
from typing import Dict, List, Optional, Set

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.build import (
    _skopeo_copy,
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    cleanup_on_failure,
    pull_index_db_artifact,
    write_build_metadata,
)
from iib.workers.tasks.fbc_utils import merge_catalogs_dirs
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    close_mr,
    commit_and_push,
    create_mr,
    get_git_token,
    get_last_commit_sha,
    revert_last_commit,
)
from iib.workers.tasks.konflux_utils import (
    find_pipelinerun,
    get_pipelinerun_image_url,
    wait_for_pipeline_completion
)
from iib.workers.tasks.opm_operations import (
    Opm,
    opm_registry_rm_fbc,
    opm_validate,
    remove_operator_deprecations,
    verify_operators_exists,
)
from iib.workers.tasks.oras_utils import (
    _get_artifact_combined_tag,
    get_indexdb_artifact_pullspec,
    _get_name_and_tag_from_pullspec,
    push_oras_artifact,
)
from iib.workers.tasks.utils import (
    get_image_digest,
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
        # Get Git repository information
        # Strip tag and digest from from_index to get base image name for lookup
        base_image_name = from_index.split('@')[0].rsplit(':', 1)[0]

        if not index_to_gitlab_push_map or base_image_name not in index_to_gitlab_push_map:
            raise IIBError(
                f"Git repository mapping not found for from_index: {from_index} "
                f"(base image: {base_image_name}). "
                "index_to_gitlab_push_map is required."
            )

        index_git_repo = index_to_gitlab_push_map[base_image_name]
        token_name, git_token = get_git_token(index_git_repo)
        branch = ocp_version

        # Clone Git repository
        set_request_state(request_id, 'in_progress', 'Cloning Git repository')
        local_git_repo_path = os.path.join(temp_dir, 'git', branch)
        os.makedirs(local_git_repo_path, exist_ok=True)

        clone_git_repo(index_git_repo, branch, token_name, git_token, local_git_repo_path)

        localized_git_catalog_path = os.path.join(local_git_repo_path, 'configs')
        if not os.path.exists(localized_git_catalog_path):
            raise IIBError(f"Catalogs directory not found in {local_git_repo_path}")

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

        # Pull index.db artifact (uses ImageStream cache if configured, otherwise pulls directly)
        artifact_dir = pull_index_db_artifact(from_index, temp_dir)

        # Find the index.db file in the artifact
        index_db_path = os.path.join(artifact_dir, "index.db")
        if not os.path.exists(index_db_path):
            raise IIBError(f"Index.db file not found at {index_db_path}")

        # Check if operators exist in index.db and remove if present
        set_request_state(request_id, 'in_progress', 'Checking and removing from index database')
        operators_in_db, index_db_path_verified = verify_operators_exists(
            from_index=None,
            base_dir=temp_dir,
            operator_packages=operators,
            overwrite_from_index_token=overwrite_from_index_token,
            index_db_path=index_db_path,
        )

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
        )

        try:
            # Commit changes and create PR or push directly
            set_request_state(request_id, 'in_progress', 'Committing changes to Git repository')
            log.info("Committing changes to Git repository. Triggering KONFLUX pipeline.")

            # Determine if this is a throw-away request (no overwrite_from_index_token)
            if not overwrite_from_index_token:
                # Create MR for throw-away requests
                mr_details = create_mr(
                    request_id=request_id,
                    local_repo_path=local_git_repo_path,
                    repo_url=index_git_repo,
                    branch=branch,
                    commit_message=f"IIB: Remove operators {', '.join(operators)} for request {request_id}",
                )
                log.info("Created merge request: %s", mr_details.get('mr_url'))
            else:
                # Push directly to branch
                commit_and_push(
                    request_id=request_id,
                    local_repo_path=local_git_repo_path,
                    repo_url=index_git_repo,
                    branch=branch,
                    commit_message=f"IIB: Remove operators {', '.join(operators)} for request {request_id}",
                )

            # Get commit SHA before waiting for pipeline (while temp directory still exists)
            last_commit_sha = get_last_commit_sha(local_repo_path=local_git_repo_path)

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

            # Extract IMAGE_URL from pipelinerun results
            image_url = get_pipelinerun_image_url(pipelinerun_name, run)

            # Build list of output pull specs to copy to
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

            # Copy built index from Konflux to all output pull specs
            set_request_state(request_id, 'in_progress', 'Copying built index to IIB registry')
            for spec in output_pull_specs:
                _skopeo_copy(
                    source=f'docker://{image_url}',
                    destination=f'docker://{spec}',
                    copy_all=True,
                    exc_msg=f'Failed to copy built index from Konflux to {spec}',
                )
                log.info("Successfully copied image to %s", spec)

            # Use the first output_pull_spec as the primary one for request updates
            output_pull_spec = output_pull_specs[0]
            # Update request with final output
            if not output_pull_spec:
                raise IIBError(
                    "output_pull_spec was not set. "
                    "This should not happen if the pipeline completed successfully."
                )

            # Send an empty index_repo_map because the Git repository is already updated with the changes
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
                # Passing an empty index_repo_map is intentional. In IIB 1.0, if overwrite_from_index
                # token is given, we push to git by default at the end of a request. In IIB 2.Oh!,
                # the commit is pushed earlier to trigger a Konflux pipelinerun. So the old
                # workflow isn't needed.
                index_repo_map={},
                rm_operators=operators,
            )

            # Push updated index.db if overwrite_from_index_token is provided
            # We can push it directly from temp_dir since we're still inside the context manager
            # Do it as the last step to avoid rolling back the index.db file if the pipeline fails.
            if operators_in_db and index_db_path and os.path.exists(index_db_path):
                # Get directory and filename separately to push only the filename
                # This ensures ORAS extracts the file as just "index.db" without directory structure
                index_db_dir = os.path.dirname(index_db_path)
                index_db_filename = os.path.basename(index_db_path)
                log.info('Pushing from directory: %s, filename: %s', index_db_dir, index_db_filename)

                # Push with request_id tag irrespective of the overwrite_from_index_token
                set_request_state(request_id, 'in_progress', 'Pushing updated index database')
                image_name, tag = _get_name_and_tag_from_pullspec(from_index)
                request_artifact_ref = conf['iib_index_db_artifact_template'].format(
                    registry=conf['iib_index_db_artifact_registry'],
                    tag=f"{_get_artifact_combined_tag(image_name, tag)}-{request_id}",
                )
                artifact_refs = [request_artifact_ref]
                if overwrite_from_index_token:
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
                            'operation': 'remove_operators',
                            'operators': ','.join(operators),
                        },
                    )
                    log.info('Pushed %s to registry', artifact_ref)

            # Close MR if it was opened
            if mr_details and index_git_repo:
                try:
                    close_mr(mr_details, index_git_repo)
                    log.info("Closed merge request: %s", mr_details.get('mr_url'))
                except IIBError as e:
                    log.warning("Failed to close merge request: %s", e)

            set_request_state(
                request_id,
                'complete',
                f"The operator(s) {', '.join(operators)} were successfully removed from the index image",
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
