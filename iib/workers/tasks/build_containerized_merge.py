# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import tempfile
import shutil
from typing import Dict, List, Optional


from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.build import (
    _update_index_image_build_state,
    _get_present_bundles,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    write_build_metadata,
    cleanup_on_failure,
    cleanup_merge_request_if_exists,
    push_index_db_artifact,
    validate_bundles_in_parallel,
    fetch_and_verify_index_db_artifact,
    prepare_git_repository_for_build,
    git_commit_and_create_mr_or_push,
    monitor_pipeline_and_extract_image,
    replicate_image_to_tagged_destinations,
)
from iib.workers.tasks.build_merge_index_image import get_missing_bundles_from_target_to_source
from iib.workers.tasks.build_merge_index_image import get_bundles_latest_version
from iib.workers.tasks.opm_operations import (
    Opm,
    _opm_registry_add,
    deprecate_bundles_db,
    opm_migrate,
    opm_validate,
    get_list_bundles,
)
from iib.workers.tasks.utils import (
    prepare_request_for_build,
    request_logger,
    reset_docker_config,
    RequestConfigMerge,
    set_registry_token,
    get_bundles_from_deprecation_list,
)
from iib.workers.tasks.fbc_utils import merge_catalogs_dirs
from iib.workers.tasks.iib_static_types import BundleImage


__all__ = ['handle_containerized_merge_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.build.handle_containerized_merge_request",
    attributes=get_binary_versions(),
)
def handle_containerized_merge_request(
    source_from_index: str,
    deprecation_list: List[str],
    request_id: int,
    binary_image: Optional[str] = None,
    target_index: Optional[str] = None,
    overwrite_target_index: bool = False,
    overwrite_target_index_token: Optional[str] = None,
    distribution_scope: Optional[str] = None,
    binary_image_config: Optional[str] = None,
    build_tags: Optional[List[str]] = None,
    graph_update_mode: Optional[str] = None,
    ignore_bundle_ocp_version: Optional[bool] = False,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
    parallel_threads: int = 5,
) -> None:
    """
    Coordinate the work needed to merge old (N) index image with new (N+1) index image.

    :param str source_from_index: pull specification to be used as the base for building the new
        index image.
    :param str target_index: pull specification of content stage index image for the
        corresponding target index image.
    :param list deprecation_list: list of deprecated bundles for the target index image.
    :param int request_id: the ID of the IIB build request.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param bool overwrite_target_index: if True, overwrite the input ``target_index`` with
        the built index image.
    :param str overwrite_target_index_token: the token used for overwriting the input
        ``target_index`` image. This is required to use ``overwrite_target_index``.
        The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param build_tags: list of extra tag to use for intermediate index image
    :param str graph_update_mode: Graph update mode that defines how channel graphs are updated
        in the index.
    :param bool ignore_bundle_ocp_version: When set to `true` and image set as target_index is
        listed in `iib_no_ocp_label_allow_list` config then bundles without
        "com.redhat.openshift.versions" label set will be added in the result `index_image`.
    :raises IIBError: if the index image merge fails.
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to push their catalogs into GitLab.
    :param int parallel_threads: the number of parallel threads to use for validating the bundles
    :raises IIBError: if the index image merge fails.
    """
    reset_docker_config()
    set_request_state(request_id, 'in_progress', 'Preparing request for merge')

    # Prepare request
    with set_registry_token(overwrite_target_index_token, target_index, append=True):
        prebuild_info = prepare_request_for_build(
            request_id,
            RequestConfigMerge(
                _binary_image=binary_image,
                overwrite_target_index_token=overwrite_target_index_token,
                source_from_index=source_from_index,
                target_index=target_index,
                distribution_scope=distribution_scope,
                binary_image_config=binary_image_config,
            ),
        )

    source_from_index_resolved = prebuild_info['source_from_index_resolved']
    target_index_resolved = prebuild_info['target_index_resolved']

    # Set OPM version
    Opm.set_opm_version(target_index_resolved)
    opm_version = Opm.opm_version

    _update_index_image_build_state(request_id, prebuild_info)

    mr_details: Optional[Dict[str, str]] = None
    local_git_repo_path: Optional[str] = None
    index_git_repo: Optional[str] = None
    last_commit_sha: Optional[str] = None
    output_pull_spec: Optional[str] = None
    original_index_db_digest: Optional[str] = None

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        # Setup and clone Git repository
        branch = prebuild_info['ocp_version']
        (
            index_git_repo,
            local_git_repo_path,
            localized_git_catalog_path,
        ) = prepare_git_repository_for_build(
            request_id=request_id,
            from_index=source_from_index,
            temp_dir=temp_dir,
            branch=branch,
            index_to_gitlab_push_map=index_to_gitlab_push_map or {},
        )

        # Pull both source and target index.db artifacts and read present bundle
        target_index_db_path = None
        source_index_db_path = fetch_and_verify_index_db_artifact(source_from_index, temp_dir)
        if target_index:
            target_index_db_path = fetch_and_verify_index_db_artifact(target_index, temp_dir)

        # Get the bundles from the index.db file
        with set_registry_token(overwrite_target_index_token, target_index, append=True):
            target_index_bundles: List[BundleImage] = []
            target_index_bundles_pull_spec: List[str] = []

            source_index_bundles, source_index_bundles_pull_spec = _get_present_bundles(
                source_index_db_path, temp_dir
            )
            log.debug("Source index bundles %s", source_index_bundles)
            log.debug("Source index bundles pull spec %s", source_index_bundles_pull_spec)

            if target_index_db_path:
                target_index_bundles, target_index_bundles_pull_spec = _get_present_bundles(
                    target_index_db_path, temp_dir
                )
                log.debug("Target index bundles %s", target_index_bundles)
                log.debug("Target index bundles pull spec %s", target_index_bundles_pull_spec)

        # Validate the bundles from source and target have their pullspecs present in the registry
        set_request_state(
            request_id,
            'in_progress',
            'Validating whether the bundles have their pullspecs present in the registry',
        )
        unique_bundles = set(source_index_bundles_pull_spec + target_index_bundles_pull_spec)
        validate_bundles_in_parallel(
            bundles=list(unique_bundles),
            threads=parallel_threads,
            wait=True,
        )

        set_request_state(request_id, 'in_progress', 'Adding bundles missing in source index image')
        log.info('Adding bundles from target index image which are missing from source index image')

        missing_bundles, invalid_bundles = get_missing_bundles_from_target_to_source(
            source_index_bundles=source_index_bundles,
            target_index_bundles=target_index_bundles,
            source_from_index=source_from_index_resolved,
            ocp_version=prebuild_info['target_ocp_version'],
            target_index=target_index_resolved,
            ignore_bundle_ocp_version=ignore_bundle_ocp_version,
        )
        missing_bundle_paths = [bundle['bundlePath'] for bundle in missing_bundles]

        # Add the missing bundles to the index.db file
        set_request_state(
            request_id, 'in_progress', 'Adding the missing bundles to the source index.db file'
        )

        if target_index_db_path:
            _opm_registry_add(temp_dir, source_index_db_path, missing_bundle_paths)

        # Process the deprecation list
        set_request_state(request_id, 'in_progress', 'Processing the deprecation list')
        intermediate_bundles = missing_bundle_paths + source_index_bundles_pull_spec
        deprecation_bundles = get_bundles_from_deprecation_list(
            intermediate_bundles, deprecation_list
        )
        deprecation_bundles = deprecation_bundles + [
            bundle['bundlePath'] for bundle in invalid_bundles
        ]

        # process the deprecation list into the intermediary index.db file
        if deprecation_bundles:
            # We need to get the latest pullpecs from bundles in order to avoid failures
            # on "opm deprecatetruncate" due to versions already removed before.
            # Once we give the latest versions all lower ones get automatically deprecated by OPM.
            all_bundles = source_index_bundles + target_index_bundles
            deprecation_bundles = get_bundles_latest_version(deprecation_bundles, all_bundles)

            deprecate_bundles_db(
                base_dir=temp_dir, index_db_file=source_index_db_path, bundles=deprecation_bundles
            )

        # Retrieve the operators from the intermediary index.db file
        # This will be required for pushing the updated index.db file to the IIB registry
        bundles_in_db = get_list_bundles(source_index_db_path, temp_dir)
        operators_in_db = [bundle['packageName'] for bundle in bundles_in_db]

        # Migrate the intermediary index.db file to FBC and generate the Dockerfile
        set_request_state(
            request_id,
            'in_progress',
            'Migrating the intermediary index.db file to FBC and generating the Dockerfile',
        )
        fbc_dir, _ = opm_migrate(source_index_db_path, temp_dir)

        # rename `catalog` directory because we need to use this name for
        # final destination of catalog (defined in Dockerfile)
        catalog_from_db = os.path.join(temp_dir, 'from_db')
        os.rename(fbc_dir, catalog_from_db)

        # Merge migrated FBC with existing FBC in Git repo
        # overwrite data in `catalog_from_index` by data from `catalog_from_db`
        # this adds changes on not opted in operators to final FBC
        log.info('Merging migrated catalog with Git catalog')
        merge_catalogs_dirs(catalog_from_db, localized_git_catalog_path)

        # We need to regenerate file-based catalog because we merged changes
        fbc_dir_path = os.path.join(temp_dir, 'catalog')
        if os.path.exists(fbc_dir_path):
            shutil.rmtree(fbc_dir_path)
        # Copy catalog to correct location expected in Dockerfile
        # Use copytree instead of move to preserve the configs directory in Git repo
        shutil.copytree(localized_git_catalog_path, fbc_dir_path)

        # Validate the FBC config
        set_request_state(request_id, 'in_progress', 'Validating the FBC config')
        opm_validate(fbc_dir_path)

        # Write build metadata to a file to be added with the commit
        set_request_state(request_id, 'in_progress', 'Writing build metadata')
        arches = set(prebuild_info['arches'])
        write_build_metadata(
            local_git_repo_path,
            opm_version,
            prebuild_info['target_ocp_version'],
            prebuild_info['distribution_scope'],
            prebuild_info['binary_image_resolved'],
            request_id,
            arches,
        )

        try:
            # Commit changes and create PR or push directly
            mr_details, last_commit_sha = git_commit_and_create_mr_or_push(
                request_id=request_id,
                local_git_repo_path=local_git_repo_path,
                index_git_repo=index_git_repo,
                branch=branch,
                commit_message=(
                    f"IIB: Merge operators for request {request_id}\n\n"
                    f"Missing bundles: {', '.join(missing_bundle_paths)}"
                ),
                overwrite_from_index=overwrite_target_index,
            )

            # Wait for Konflux pipeline and extract built image UR
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

            _update_index_image_pull_spec(
                output_pull_spec=output_pull_spec,
                request_id=request_id,
                arches=prebuild_info['arches'],
                from_index=source_from_index,
                overwrite_from_index=overwrite_target_index,
                overwrite_from_index_token=overwrite_target_index_token,
                resolved_prebuild_from_index=source_from_index_resolved,
                add_or_rm=True,
                is_image_fbc=True,
                # Passing an empty index_repo_map is intentional. In IIB 1.0, if
                # the overwrite_from_index token is given, we push to git by default
                # at the end of a request. In IIB 2.0, the commit is pushed earlier to trigger
                # a Konflux pipelinerun. So the old workflow isn't needed.
                index_repo_map={},
            )

            # Push updated index.db if overwrite_target_index_token is provided
            # We can push it directly from temp_dir since we're still inside the
            # context manager. Do it as the last step to avoid rolling back the
            # index.db file if the pipeline fails.
            original_index_db_digest = push_index_db_artifact(
                request_id=request_id,
                from_index=source_from_index,
                index_db_path=source_index_db_path,
                operators=operators_in_db,
                overwrite_from_index=overwrite_target_index,
                request_type='merge',
            )

            # Close MR if it was opened
            cleanup_merge_request_if_exists(mr_details, index_git_repo)

            # Update request with final output
            set_request_state(
                request_id,
                'complete',
                f"The operator(s) {operators_in_db} were successfully merged "
                "from the target index image into the source index image",
            )
        except Exception as e:
            cleanup_on_failure(
                mr_details=mr_details,
                last_commit_sha=last_commit_sha,
                index_git_repo=index_git_repo,
                overwrite_from_index=overwrite_target_index,
                request_id=request_id,
                from_index=source_from_index,
                index_repo_map={},
                original_index_db_digest=original_index_db_digest,
                reason=f"error: {e}",
            )
            # Reset Docker config for the next request. This is a fail safe.
            reset_docker_config()
            raise IIBError(f"Failed to merge operators: {e}")
