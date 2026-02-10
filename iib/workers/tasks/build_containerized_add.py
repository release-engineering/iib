# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import shutil
import stat
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.build import (
    inspect_related_images,
    _update_index_image_pull_spec,
    _update_index_image_build_state,
    _get_present_bundles,
    _get_missing_bundles,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    prepare_git_repository_for_build,
    fetch_and_verify_index_db_artifact,
    write_build_metadata,
    git_commit_and_create_mr_or_push,
    monitor_pipeline_and_extract_image,
    replicate_image_to_tagged_destinations,
    push_index_db_artifact,
    cleanup_merge_request_if_exists,
    cleanup_on_failure,
)
from iib.workers.tasks.fbc_utils import merge_catalogs_dirs
from iib.workers.tasks.iib_static_types import (
    BundleImage,
)
from iib.workers.tasks.opm_operations import (
    opm_migrate,
    Opm,
    _opm_registry_add,
    deprecate_bundles_db,
)
from iib.workers.tasks.utils import (
    chmod_recursively,
    get_bundles_from_deprecation_list,
    get_resolved_bundles,
    request_logger,
    reset_docker_config,
    set_registry_token,
    RequestConfigAddRm,
    get_image_label,
    verify_labels,
    prepare_request_for_build,
)

__all__ = ['handle_containerized_add_request']

log = logging.getLogger(__name__)
worker_config = get_worker_config()


@app.task
@request_logger
@instrument_tracing(span_name="workers.tasks.handle_add_request", attributes=get_binary_versions())
def handle_containerized_add_request(
    bundles: List[str],
    request_id: int,
    binary_image: Optional[str] = None,
    from_index: Optional[str] = None,
    add_arches: Optional[Set[str]] = None,
    overwrite_from_index: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    distribution_scope: Optional[str] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    deprecation_list: Optional[List[str]] = None,
    build_tags: Optional[List[str]] = None,
    graph_update_mode: Optional[str] = None,
    check_related_images: bool = False,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
    binary_image_less_arches_allowed_versions: Optional[List[str]] = None,
    username: Optional[str] = None,
) -> None:
    """
    Coordinate the work needed to build the index image with the input bundles.

    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param set add_arches: the set of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :param list deprecation_list: list of deprecated bundles for the target index image. Defaults
        to ``None``.
    :param list build_tags: List of tags which will be applied to intermediate index images.
    :param str graph_update_mode: Graph update mode that defines how channel graphs are updated
        in the index.
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to push their catalogs into GitLab.
    :param list binary_image_less_arches_allowed_versions: list of versions of the binary image
        that are allowed to build for less arches. Defaults to ``None``.
    :raises IIBError: if the index image build fails.
    """
    reset_docker_config()
    # Resolve bundles to their digests
    set_request_state(request_id, 'in_progress', 'Resolving the bundles')

    with set_registry_token(overwrite_from_index_token, from_index, append=True):
        resolved_bundles = get_resolved_bundles(bundles)
        verify_labels(resolved_bundles)
        if check_related_images:
            inspect_related_images(
                resolved_bundles,
                request_id,
                worker_config.iib_related_image_registry_replacement.get(username),
            )

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigAddRm(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            bundles=bundles,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
            binary_image_less_arches_allowed_versions=binary_image_less_arches_allowed_versions,
        ),
    )
    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']
    arches = prebuild_info['arches']
    operators = list(prebuild_info['bundle_mapping'].keys())
    distribution_scope = prebuild_info['distribution_scope']

    index_to_gitlab_push_map = index_to_gitlab_push_map or {}
    # Variables mr_details, last_commit_sha and original_index_db_digest
    # needs to be assigned; otherwise cleanup_on_failure() fails when an exception is raised.
    mr_details: Optional[Dict[str, str]] = None
    last_commit_sha: Optional[str] = None
    original_index_db_digest: Optional[str] = None

    Opm.set_opm_version(from_index_resolved)

    _update_index_image_build_state(request_id, prebuild_info)
    present_bundles: List[BundleImage] = []
    present_bundles_pull_spec: List[str] = []
    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        branch = prebuild_info['ocp_version']

        # Set up and clone Git repository
        (
            index_git_repo,
            local_git_repo_path,
            localized_git_catalog_path,
        ) = prepare_git_repository_for_build(
            request_id=request_id,
            from_index=str(from_index),
            temp_dir=temp_dir,
            branch=branch,
            index_to_gitlab_push_map=index_to_gitlab_push_map,
        )

        # Pull index.db artifact (uses ImageStream cache if configured, otherwise pulls directly)
        artifact_index_db_file = fetch_and_verify_index_db_artifact(
            from_index=str(from_index),
            temp_dir=temp_dir,
        )

        msg = 'Checking if bundles are already present in index image'
        log.info(msg)
        set_request_state(request_id, 'in_progress', msg)

        # Extract packages from FBC directory to speed up opm render
        extracted_packages = Path(temp_dir) / "extracted_packages"
        extracted_packages.mkdir(parents=True, exist_ok=True)

        package_names = prebuild_info['bundle_mapping'].keys()
        log.debug("Extracting packages from FBC directory: %s", package_names)
        for package in package_names:
            package_dir = Path(localized_git_catalog_path) / package
            if not package_dir.is_dir():
                log.debug("Package %s not found in FBC directory", package)
                continue
            shutil.copytree(package_dir, extracted_packages / package)

        with set_registry_token(overwrite_from_index_token, from_index_resolved, append=True):
            present_bundles, present_bundles_pull_spec = _get_present_bundles(
                str(extracted_packages), temp_dir
            )

        filtered_bundles = _get_missing_bundles(present_bundles, resolved_bundles)
        excluded_bundles = [bundle for bundle in resolved_bundles if bundle not in filtered_bundles]
        resolved_bundles = filtered_bundles

        if excluded_bundles:
            log.info(
                'Following bundles are already present in the index image: %s',
                ' '.join(excluded_bundles),
            )

        # This is a replacement for opm_registry_add_fbc for a containerized version of IIB.
        # Note: only index.db is modified (FBC directory is unchanged)
        _opm_registry_add(
            base_dir=temp_dir,
            index_db=artifact_index_db_file,
            bundles=resolved_bundles,
            overwrite_csv=(prebuild_info['distribution_scope'] in ['dev', 'stage']),
            graph_update_mode=graph_update_mode,
        )

        deprecation_bundles = get_bundles_from_deprecation_list(
            present_bundles_pull_spec + resolved_bundles, deprecation_list or []
        )

        if deprecation_bundles:
            deprecate_bundles_db(
                bundles=deprecation_bundles,
                base_dir=temp_dir,
                index_db_file=artifact_index_db_file,
            )

        from_db_dir = Path(temp_dir) / "from_db"
        from_db_dir.mkdir(parents=True, exist_ok=True)
        # get catalog from SQLite index.db (hidden db) - not opted in operators
        catalog_from_db, _ = opm_migrate(
            index_db=artifact_index_db_file,
            base_dir=str(from_db_dir),
            generate_cache=False,
        )

        # we have to remove all `deprecation_bundles` from `localized_git_catalog_path`
        # before merging catalogs otherwise if catalog was deprecated and
        # removed from `index.db` it stays on FBC (from_index)
        # Therefore we have to remove the directory before merging
        for deprecate_bundle_pull_spec in deprecation_bundles:
            # remove deprecated operators from FBC stored in index image
            deprecate_bundle_package = get_image_label(
                deprecate_bundle_pull_spec, 'operators.operatorframework.io.bundle.package.v1'
            )
            bundle_from_index = Path(localized_git_catalog_path) / deprecate_bundle_package
            if bundle_from_index.is_dir():
                log.debug(
                    "Removing deprecated bundle from catalog before merging: %s",
                    deprecate_bundle_package,
                )
                shutil.rmtree(bundle_from_index)
        # overwrite data in `localized_git_catalog_path` by data from `catalog_from_db`
        # this adds changes on not opted in operators to final
        merge_catalogs_dirs(catalog_from_db, localized_git_catalog_path)

        # If the container-tool podman is used in the opm commands above, opm will create temporary
        # files and directories without the write permission. This will cause the context manager
        # to fail to delete these files. Adjust the file modes to avoid this error.
        chmod_recursively(
            temp_dir,
            dir_mode=(stat.S_IRWXU | stat.S_IRWXG),
            file_mode=(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP),
        )

        # Write build metadata to a file to be added with the commit
        set_request_state(request_id, 'in_progress', 'Writing build metadata')
        write_build_metadata(
            local_git_repo_path,
            Opm.opm_version,
            prebuild_info['ocp_version'],
            str(distribution_scope),
            binary_image_resolved,
            request_id,
            arches,
        )

        try:
            # Commit changes and create MR or push directly
            mr_details, last_commit_sha = git_commit_and_create_mr_or_push(
                request_id=request_id,
                local_git_repo_path=local_git_repo_path,
                index_git_repo=index_git_repo,
                branch=branch,
                commit_message=(
                    f"IIB: Add bundles for request {request_id}\n\n"
                    f"Bundles: {', '.join(bundles)}"
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
                from_index=str(from_index),
                index_db_path=artifact_index_db_file,
                operators=operators,
                overwrite_from_index=overwrite_from_index,
                request_type='add',
            )

            # Close MR if it was opened
            cleanup_merge_request_if_exists(mr_details, index_git_repo)

            set_request_state(
                request_id,
                'complete',
                'The operator bundle(s) were successfully added to the index image',
            )
        except Exception as e:
            cleanup_on_failure(
                mr_details=mr_details,
                last_commit_sha=last_commit_sha,
                index_git_repo=index_git_repo,
                overwrite_from_index=overwrite_from_index,
                request_id=request_id,
                from_index=str(from_index),
                index_repo_map=index_to_gitlab_push_map or {},
                original_index_db_digest=original_index_db_digest,
                reason=f"error: {e}",
            )
            raise IIBError(f"Failed to add bundles: {e}")
