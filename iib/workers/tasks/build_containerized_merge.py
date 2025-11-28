# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import threading
import queue
import os
import tempfile
import shutil
from typing import Dict, List, Optional


from iib.workers.config import get_worker_config
from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.build import (
    _update_index_image_build_state,
    _get_present_bundles,
    _update_index_image_pull_spec,
    _skopeo_copy,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    write_build_metadata,
    get_list_of_output_pullspec,
    cleanup_on_failure,
    push_index_db_artifact,
)
from iib.workers.tasks.iib_static_types import BundleImage
from iib.workers.tasks.git_utils import (
    create_mr,
    clone_git_repo,
    get_git_token,
    get_last_commit_sha,
    resolve_git_url,
    commit_and_push,
    close_mr,
)
from iib.workers.tasks.build_merge_index_image import get_missing_bundles_from_target_to_source
from iib.workers.tasks.build_merge_index_image import get_bundles_latest_version
from iib.workers.tasks.konflux_utils import (
    wait_for_pipeline_completion,
    find_pipelinerun,
    get_pipelinerun_image_url,
)
from iib.workers.tasks.opm_operations import (
    Opm,
    _opm_registry_add,
    _get_or_create_temp_index_db_file,
    opm_registry_deprecatetruncate,
    opm_migrate,
    opm_validate,
    get_list_bundles,
)
from iib.workers.tasks.utils import (
    prepare_request_for_build,
    request_logger,
    add_max_ocp_version_property,
    reset_docker_config,
    RequestConfigMerge,
    skopeo_inspect,
    get_bundles_from_deprecation_list,
)
from iib.workers.tasks.oras_utils import get_indexdb_artifact_pullspec, get_oras_artifact
from iib.workers.tasks.fbc_utils import merge_catalogs_dirs


__all__ = ['handle_containerized_merge_request']

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

    def run(self) -> None:
        """Execute the validation of the bundle pullspecs."""
        bundle = None
        try:
            while not self.bundles_queue.empty():
                bundle = self.bundles_queue.get()
                skopeo_inspect(f'docker://{bundle}', '--raw', return_json=False)
        except IIBError as e:
            log.error(f"Error validating bundle {bundle}: {e}")
            self.exception = e
        finally:
            self.bundles_queue.task_done()


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
    :param build_tags: list of extra tag to use for intermetdiate index image
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
        # Get Git repository information
        index_git_repo = resolve_git_url(
            from_index=source_from_index, index_repo_map=index_to_gitlab_push_map or {}
        )
        if not index_git_repo:
            raise IIBError(f"Cannot resolve the git repository for {source_from_index}")
        log.info(
            "Git repo for %s: %s",
            source_from_index,
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

        # Pull both source and target index.db artifacts and read present bundles
        set_request_state(
            request_id,
            'in_progress',
            'Retrieving the bundles on index.db from source and target index images',
        )

        # Get the index.db from ORAS registry for source and target index images
        source_index_db_ref = get_indexdb_artifact_pullspec(source_from_index_resolved)
        target_index_db_ref = get_indexdb_artifact_pullspec(target_index_resolved)
        source_index_db_path = get_oras_artifact(source_index_db_ref, temp_dir)
        target_index_db_path = get_oras_artifact(target_index_db_ref, temp_dir)

        # Get the bundles from the index.db file
        source_index_bundles, source_index_bundles_pull_spec = _get_present_bundles(
            source_index_db_path, temp_dir
        )
        log.debug("Source index bundles %s", source_index_bundles)
        log.debug("Source index bundles pull spec %s", source_index_bundles_pull_spec)

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

        bundles_from_source_queue: queue.Queue[BundleImage] = queue.Queue()
        bundles_from_target_queue: queue.Queue[BundleImage] = queue.Queue()
        for bundle in source_index_bundles:
            bundles_from_source_queue.put(bundle)
        for bundle in target_index_bundles:
            bundles_from_target_queue.put(bundle)

        # Validate the bundles from source and target have their pullspecs present in the registry
        validate_source_threads = []
        validate_target_threads = []
        for _ in range(parallel_threads):
            validate_source_thread = ValidateBundlesThread(bundles_from_source_queue)
            validate_source_threads.append(validate_source_thread)
            validate_source_thread.start()

            validate_target_thread = ValidateBundlesThread(bundles_from_target_queue)
            validate_target_threads.append(validate_target_thread)
            validate_target_thread.start()

        # Wait for all threads to complete and raise if any thread fails on validation
        for t in validate_source_threads + validate_target_threads:
            t.join()
            if t.exception:
                raise t.exception

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
        if missing_bundle_paths:
            add_max_ocp_version_property(missing_bundle_paths, temp_dir)

        # Add the missing bundles to the index.db file
        set_request_state(
            request_id, 'in_progress', 'Adding the missing bundles to the index.db file'
        )
        intermediary_db = _get_or_create_temp_index_db_file(temp_dir, source_from_index_resolved)
        _opm_registry_add(temp_dir, intermediary_db, missing_bundle_paths)

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
            conf = get_worker_config()
            # We need to get the latest pullpecs from bundles in order to avoid failures
            # on "opm deprecatetruncate" due to versions already removed before.
            # Once we give the latest versions all lower ones get automatically deprecated by OPM.
            all_bundles = source_index_bundles + target_index_bundles
            deprecation_bundles = get_bundles_latest_version(deprecation_bundles, all_bundles)

            for i in range(0, len(deprecation_bundles), conf.iib_deprecate_bundles_limit):
                opm_registry_deprecatetruncate(
                    base_dir=temp_dir,
                    index_db=intermediary_db,
                    bundles=deprecation_bundles[i : i + conf.iib_deprecate_bundles_limit],  # noqa: E203,E501
                )

        # Retrieve the operators from the intermediary index.db file
        # This will be required for pushing the updated index.db file to the IIB registry
        bundles_in_db = get_list_bundles(intermediary_db, temp_dir)
        operators_in_db = [bundle['packageName'] for bundle in bundles_in_db]

        # Migrate the intermediary index.db file to FBC and generate the Dockerfile
        set_request_state(
            request_id,
            'in_progress',
            'Migrating the intermediary index.db file to FBC and generating the Dockerfile',
        )
        fbc_dir, _ = opm_migrate(intermediary_db, temp_dir)

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
        shutil.move(catalog_from_db, fbc_dir_path)

        # Validate the FBC config
        set_request_state(request_id, 'in_progress', 'Validating the FBC config')
        opm_validate(fbc_dir_path)

        # Generate the Dockerfile
        set_request_state(request_id, 'in_progress', 'Generating the Dockerfile')

        # Write build metadata to a file to be added with the commit
        set_request_state(request_id, 'in_progress', 'Writing build metadata')
        write_build_metadata(
            local_git_repo_path,
            opm_version,
            prebuild_info['target_ocp_version'],
            prebuild_info['distribution_scope'],
            prebuild_info['binary_image_resolved'],
            request_id,
        )

        try:
            # Commit changes and create PR or push directly
            set_request_state(request_id, 'in_progress', 'Committing changes to Git repository')
            log.info("Committing changes to Git repository. Triggering KONFLUX pipeline.")

            # Determine if this is a throw-away request (no overwrite_target_index_token)
            if not overwrite_target_index_token:
                # Create MR for throw-away requests
                mr_details = create_mr(
                    request_id=request_id,
                    local_repo_path=local_git_repo_path,
                    repo_url=index_git_repo,
                    branch=branch,
                    commit_message=(
                        f"IIB: Merge operators for request {request_id}\n\n"
                        f"Missing bundles: {', '.join(missing_bundle_paths)}"
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
                        f"Missing bundles: {', '.join(missing_bundle_paths)}"
                    ),
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

            set_request_state(request_id, 'in_progress', 'Copying built index to IIB registry')

            # Extract IMAGE_URL from pipelinerun results
            image_url = get_pipelinerun_image_url(pipelinerun_name, run)
            output_pull_specs = get_list_of_output_pullspec(request_id, build_tags)

            # Copy built index from Konflux to all output pull specs
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
                index_db_path=intermediary_db,
                operators=operators_in_db,
                operators_in_db=set(operators_in_db),
                overwrite_from_index=overwrite_target_index,
                request_type='merge',
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
            raise IIBError(f"Failed to merge operators: {e}")
