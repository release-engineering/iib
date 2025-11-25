# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import tempfile
from typing import Dict, List, Optional, Set

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.build import (
    _update_index_image_build_state,
    _update_index_image_pull_spec,
    _skopeo_copy,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    pull_index_db_artifact,
    write_build_metadata,
    get_list_of_output_pullspec,
    cleanup_on_failure,
    push_index_db_artifact,
)
from iib.workers.tasks.git_utils import (
    create_mr,
    clone_git_repo,
    get_git_token,
    get_last_commit_sha,
    resolve_git_url,
    commit_and_push,
    close_mr,
)
from iib.workers.tasks.konflux_utils import (
    wait_for_pipeline_completion,
    find_pipelinerun,
    get_pipelinerun_image_url,
)
from iib.workers.tasks.opm_operations import (
    Opm,
    opm_registry_add_fbc_fragment_containerized,
)
from iib.workers.tasks.utils import (
    get_resolved_image,
    prepare_request_for_build,
    request_logger,
    set_registry_token,
    RequestConfigFBCOperation,
    reset_docker_config,
)

__all__ = ['handle_containerized_fbc_operation_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.build.handle_containerized_fbc_operation_request",
    attributes=get_binary_versions(),
)
def handle_containerized_fbc_operation_request(
    request_id: int,
    fbc_fragments: List[str],
    from_index: str,
    binary_image: Optional[str] = None,
    distribution_scope: str = '',
    overwrite_from_index: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    build_tags: Optional[List[str]] = None,
    add_arches: Optional[Set[str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
    used_fbc_fragment: bool = False,
) -> None:
    """
    Add fbc fragments to an fbc index image.

    :param list fbc_fragments: list of fbc fragments that need to be added to final FBC index image
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param set add_arches: the set of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to push their catalogs into GitLab.
    :param bool used_fbc_fragment: flag indicating if the original request used fbc_fragment
        (single) instead of fbc_fragments (array). Used for backward compatibility.
    """
    reset_docker_config()
    set_request_state(request_id, 'in_progress', 'Resolving the fbc fragments')

    # Resolve all fbc fragments
    resolved_fbc_fragments = []
    for fbc_fragment in fbc_fragments:
        with set_registry_token(overwrite_from_index_token, fbc_fragment, append=True):
            resolved_fbc_fragment = get_resolved_image(fbc_fragment)
            resolved_fbc_fragments.append(resolved_fbc_fragment)

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            fbc_fragments=fbc_fragments,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )

    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']
    arches = prebuild_info['arches']

    index_to_gitlab_push_map = index_to_gitlab_push_map or {}
    # Variables mr_details, last_commit_sha and original_index_db_digest
    # needs to be assigned; otherwise cleanup_on_failure() fails when an exception is raised.
    mr_details: Optional[Dict[str, str]] = None
    last_commit_sha: Optional[str] = None
    original_index_db_digest: Optional[str] = None

    Opm.set_opm_version(from_index_resolved)

    # Store all resolved fragments
    prebuild_info['fbc_fragments_resolved'] = resolved_fbc_fragments

    # For backward compatibility, only populate old fields if original request used fbc_fragment
    # This flag should be passed from the API layer
    if used_fbc_fragment and resolved_fbc_fragments:
        prebuild_info['fbc_fragment_resolved'] = resolved_fbc_fragments[0]

    _update_index_image_build_state(request_id, prebuild_info)

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        # Get Git repository information
        index_git_repo = resolve_git_url(
            from_index=from_index, index_repo_map=index_to_gitlab_push_map
        )
        if not index_git_repo:
            raise IIBError(f"Cannot resolve the git repository for {from_index}")
        log.info(
            "Git repo for %s: %s",
            from_index,
            index_git_repo,
        )

        token_name, git_token = get_git_token(index_git_repo)
        branch = prebuild_info['ocp_version']

        # Clone Git repository
        set_request_state(request_id, 'in_progress', 'Cloning Git repository')
        local_git_repo_path = os.path.join(temp_dir, 'git', branch)
        os.makedirs(local_git_repo_path, exist_ok=True)

        clone_git_repo(index_git_repo, branch, token_name, git_token, local_git_repo_path)

        localized_git_catalog_path = os.path.join(local_git_repo_path, 'configs')
        if not os.path.exists(localized_git_catalog_path):
            raise IIBError(f"Catalogs directory not found in {local_git_repo_path}")

        # Pull index.db artifact (uses ImageStream cache if configured, otherwise pulls directly)
        artifact_dir = pull_index_db_artifact(
            from_index,
            temp_dir,
        )
        artifact_index_db_file = os.path.join(artifact_dir, "index.db")

        log.debug("Artifact DB path %s", artifact_index_db_file)
        if not os.path.exists(artifact_index_db_file):
            log.error("Artifact DB file not found at %s", artifact_index_db_file)
            raise IIBError(f"Artifact DB file not found at {artifact_index_db_file}")

        set_request_state(request_id, 'in_progress', 'Adding fbc fragment')
        (
            updated_catalog_path,
            index_db_path,
            operators_in_db,
        ) = opm_registry_add_fbc_fragment_containerized(
            request_id=request_id,
            temp_dir=temp_dir,
            from_index_configs_dir=localized_git_catalog_path,
            fbc_fragments=resolved_fbc_fragments,
            overwrite_from_index_token=overwrite_from_index_token,
            index_db_path=artifact_index_db_file,
        )

        # Write build metadata to a file to be added with the commit
        set_request_state(request_id, 'in_progress', 'Writing build metadata')
        write_build_metadata(
            local_git_repo_path,
            Opm.opm_version,
            prebuild_info['ocp_version'],
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
                    commit_message=(
                        f"IIB: Add data from FBC fragments for request {request_id}\n\n"
                        f"FBC fragments: {', '.join(fbc_fragments)}"
                    ),
                )
                log.info("Created merge request: %s", mr_details.get('mr_url'))
            else:
                # Push directly to the branch
                commit_and_push(
                    request_id=request_id,
                    local_repo_path=local_git_repo_path,
                    repo_url=index_git_repo,
                    branch=branch,
                    commit_message=(
                        f"IIB: Add data from FBC fragments for request {request_id}\n\n"
                        f"FBC fragments: {', '.join(fbc_fragments)}"
                    ),
                )

            # Get commit SHA before waiting for the pipeline (while the temp directory still exists)
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

            set_request_state(request_id, 'in_progress', 'Copying built index to IIB registry')
            # Extract IMAGE_URL from pipelinerun results
            image_url = get_pipelinerun_image_url(pipelinerun_name, run)
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

            # Use the first output_pull_spec as the primary one for request updates
            output_pull_spec = output_pull_specs[0]
            # Update request with final output
            if not output_pull_spec:
                raise IIBError(
                    "output_pull_spec was not set. "
                    "This should not happen if the pipeline completed successfully."
                )

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
                # the overwrite_from_index token is given, we push to git by default
                # at the end of a request. In IIB 2.0, the commit is pushed earlier to trigger
                # a Konflux pipelinerun. So the old workflow isn't needed.
                index_repo_map={},
            )

            # Push updated index.db if overwrite_from_index_token is provided
            # We can push it directly from temp_dir since we're still inside the
            # context manager. Do it as the last step to avoid rolling back the
            # index.db file if the pipeline fails.
            original_index_db_digest = push_index_db_artifact(
                request_id=request_id,
                from_index=from_index,
                index_db_path=index_db_path,
                operators=operators_in_db,
                operators_in_db=set(operators_in_db),
                overwrite_from_index=overwrite_from_index,
                request_type='rm',
            )

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
                f"The operator(s) {operators_in_db} were successfully removed "
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
            raise IIBError(f"Failed to add FBC fragment: {e}")
