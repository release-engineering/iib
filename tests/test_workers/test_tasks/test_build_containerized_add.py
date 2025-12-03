# SPDX-License-Identifier: GPL-3.0-or-later
import os
from pathlib import Path
from unittest import mock
import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_containerized_add


@pytest.mark.parametrize('check_related_images', (True, False))
@pytest.mark.parametrize('with_deprecations', (True, False))
@pytest.mark.parametrize(
    'present_bundles_return',
    (
        ([], []),
        (
            [
                {
                    'bundlePath': 'some-operator/some-bundle/1.0.0',
                    'packageName': 'some-operator',
                    'version': '1.0.0',
                }
            ],
            ['registry.example.com/some-operator@sha256:present'],
        ),
    ),
    ids=('present_empty', 'present_non_empty'),
)
@mock.patch('iib.workers.tasks.build_containerized_add.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_add.Path.mkdir')
@mock.patch('iib.workers.tasks.build_containerized_add.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_add.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_add.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_add.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_add._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_add.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_add.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_add.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_add.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_add.chmod_recursively')
@mock.patch('iib.workers.tasks.build_containerized_add.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_add.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_add.Path.is_dir')
@mock.patch('iib.workers.tasks.build_containerized_add.get_image_label')
@mock.patch('iib.workers.tasks.build_containerized_add.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_add.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_add.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_add._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_add._get_missing_bundles')
@mock.patch('iib.workers.tasks.build_containerized_add._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_add.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_add.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_add.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_add._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_add.Opm')
@mock.patch('iib.workers.tasks.build_containerized_add.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_add.inspect_related_images')
@mock.patch('iib.workers.tasks.build_containerized_add.verify_labels')
@mock.patch('iib.workers.tasks.build_containerized_add.get_resolved_bundles')
@mock.patch('iib.workers.tasks.build_containerized_add.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_add.reset_docker_config')
def test_handle_containerized_add_request(
    mock_reset_docker,
    mock_set_token,
    mock_get_resolved,
    mock_verify_labels,
    mock_inspect,
    mock_prepare_req,
    mock_opm,
    mock_update_build_state,
    mock_td,
    mock_prepare_git,
    mock_fetch_index_db,
    mock_get_present,
    mock_get_missing,
    mock_opm_add,
    mock_get_deprecations,
    mock_deprecate,
    mock_opm_migrate,
    mock_get_image_label,
    mock_path_isdir,
    mock_rmtree,
    mock_merge,
    mock_chmod,
    mock_write_meta,
    mock_git_commit,
    mock_monitor,
    mock_replicate,
    mock_update_pull_spec,
    mock_push_index_db,
    mock_cleanup_mr,
    mock_set_state,
    mock_cleanup_failure,
    mock_makedirs,
    mock_copytree,
    with_deprecations,
    check_related_images,
    present_bundles_return,
    tmpdir,
):
    # Mock input data
    bundles = ['some-bundle:latest']
    request_id = 123
    binary_image = 'binary-image:latest'
    resolved_bundles = ['some-bundle@sha256:123456']
    index_db_path = '/tmp/index.db'
    temp_dir_path = '/tmp/iib-123-temp'
    from_index = 'index:latest'

    mock_get_resolved.return_value = resolved_bundles
    mock_td.return_value.__enter__.return_value = temp_dir_path

    # Mock prebuild info
    prebuild_info = {
        'from_index_resolved': 'from-index@sha256:abcdef',
        'binary_image_resolved': 'binary-image@sha256:fedcba',
        'arches': {'amd64'},
        'bundle_mapping': {'some-operator': resolved_bundles},
        'ocp_version': 'v4.12',
        'distribution_scope': 'prod',
        'binary_image': binary_image,
    }
    mock_prepare_req.return_value = prebuild_info

    # Mock git preparation
    index_git_repo = mock.Mock()
    local_git_repo_path = Path(tmpdir) / 'git_repo'
    localized_git_catalog_path = Path(local_git_repo_path) / "configs"
    local_git_repo_path.mkdir(parents=True)
    mock_prepare_git.return_value = (
        index_git_repo,
        local_git_repo_path,
        localized_git_catalog_path,
    )

    mock_fetch_index_db.return_value = index_db_path

    # Ensure path checks pass for the copytree loop
    mock_path_isdir.return_value = True

    # Set return value from parameter
    mock_get_present.return_value = present_bundles_return
    _, present_bundles_pull_specs = present_bundles_return

    mock_get_missing.return_value = resolved_bundles

    # Mock deprecation handling
    deprecation_list = ['deprecated-bundle:1.0'] if with_deprecations else None
    if with_deprecations:
        mock_get_deprecations.return_value = ['deprecated-bundle@sha256:old']
        mock_get_image_label.return_value = 'deprecated-operator-package'
        package = Path(localized_git_catalog_path) / 'deprecated-operator-package'
        package.mkdir(parents=True)
    else:
        mock_get_deprecations.return_value = []

    # Mock OPM migration
    catalog_from_db = '/tmp/from_db'
    mock_opm_migrate.return_value = (catalog_from_db, None)

    # Mock commit and push
    mock_git_commit.return_value = ({'mr_id': 1}, 'commit_sha_123')

    # Mock pipeline monitoring
    image_url = 'registry.example.com/output-image:tag'
    mock_monitor.return_value = image_url

    # Mock replication
    output_pull_specs = ['registry.example.com/final-image:123']
    mock_replicate.return_value = output_pull_specs

    # Mock final artifact push
    mock_push_index_db.return_value = 'sha256:index_db_digest'

    # Call the function
    if with_deprecations:
        with mock.patch('pathlib.Path.is_dir', return_value=True):
            build_containerized_add.handle_containerized_add_request(
                bundles=bundles,
                request_id=request_id,
                binary_image=binary_image,
                from_index=from_index,
                check_related_images=check_related_images,
                deprecation_list=deprecation_list,
                overwrite_from_index_token="user:pass",
            )
    else:
        build_containerized_add.handle_containerized_add_request(
            bundles=bundles,
            request_id=request_id,
            binary_image=binary_image,
            from_index=from_index,
            check_related_images=check_related_images,
            deprecation_list=deprecation_list,
            overwrite_from_index_token="user:pass",
        )

    # Verifications
    mock_reset_docker.assert_called_once()
    mock_set_state.assert_called()
    mock_get_resolved.assert_called_once_with(bundles)
    mock_verify_labels.assert_called_once_with(resolved_bundles)

    if check_related_images:
        mock_inspect.assert_called_once()
    else:
        mock_inspect.assert_not_called()

    mock_prepare_req.assert_called_once()

    # Verify git preparation
    mock_prepare_git.assert_called_once_with(
        request_id=request_id,
        from_index=str(from_index),
        temp_dir=temp_dir_path,
        branch='v4.12',
        index_to_gitlab_push_map={},
    )

    # Verify bundle checks
    mock_get_present.assert_called_once()
    mock_get_missing.assert_called_once()

    # Verify that present bundles pull specs are correctly used for deprecations
    mock_get_deprecations.assert_called_once_with(
        present_bundles_pull_specs + resolved_bundles,
        deprecation_list or [],
    )

    # Verify copytree call for extraction
    expected_src = Path(localized_git_catalog_path) / 'some-operator'
    expected_dst = Path(temp_dir_path) / 'extracted_packages' / 'some-operator'
    mock_copytree.assert_any_call(expected_src, expected_dst)

    # Verify OPM operations
    mock_opm_add.assert_called_once_with(
        base_dir=temp_dir_path,
        index_db=index_db_path,
        bundles=resolved_bundles,
        overwrite_csv=False,
        graph_update_mode=None,
    )

    # Verify deprecation handling
    if with_deprecations:
        mock_get_deprecations.assert_called_once()
        mock_deprecate.assert_called_once()
        mock_rmtree.assert_called()
        expected_path = Path(localized_git_catalog_path) / 'deprecated-operator-package'
        mock_rmtree.assert_any_call(expected_path)
    else:
        mock_deprecate.assert_not_called()
        mock_rmtree.assert_not_called()

    # Verify makedirs and migrate
    assert mock_makedirs.call_count >= 2
    mock_opm_migrate.assert_called_once_with(
        index_db=index_db_path,
        base_dir=os.path.join(temp_dir_path, 'from_db'),
        generate_cache=False,
    )

    mock_merge.assert_called_once_with(catalog_from_db, localized_git_catalog_path)
    mock_chmod.assert_called_once()
    mock_write_meta.assert_called_once()
    mock_git_commit.assert_called_once()
    mock_monitor.assert_called_once_with(request_id=request_id, last_commit_sha='commit_sha_123')
    mock_replicate.assert_called_once()

    mock_update_pull_spec.assert_called_once_with(
        output_pull_spec=output_pull_specs[0],
        request_id=request_id,
        arches={'amd64'},
        from_index=from_index,
        overwrite_from_index=False,
        overwrite_from_index_token="user:pass",
        resolved_prebuild_from_index='from-index@sha256:abcdef',
        add_or_rm=True,
        is_image_fbc=True,
        index_repo_map={},
    )

    mock_push_index_db.assert_called_once()
    mock_cleanup_mr.assert_called_once()
    mock_cleanup_failure.assert_not_called()


@mock.patch('iib.workers.tasks.build_containerized_add.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_add.Path.mkdir')
@mock.patch('iib.workers.tasks.build_containerized_add.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_add.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_add.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_add.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_add._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_add.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_add.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_add.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_add.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_add.chmod_recursively')
@mock.patch('iib.workers.tasks.build_containerized_add.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_add.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_add.Path.is_dir')
@mock.patch('iib.workers.tasks.build_containerized_add.get_image_label')
@mock.patch('iib.workers.tasks.build_containerized_add.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_add.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_add.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_add._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_add._get_missing_bundles')
@mock.patch('iib.workers.tasks.build_containerized_add._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_add.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_add.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_add.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_add._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_add.Opm')
@mock.patch('iib.workers.tasks.build_containerized_add.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_add.inspect_related_images')
@mock.patch('iib.workers.tasks.build_containerized_add.verify_labels')
@mock.patch('iib.workers.tasks.build_containerized_add.get_resolved_bundles')
@mock.patch('iib.workers.tasks.build_containerized_add.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_add.reset_docker_config')
def test_handle_containerized_add_request_failure(
    mock_reset_docker,
    mock_set_token,
    mock_get_resolved,
    mock_verify_labels,
    mock_inspect,
    mock_prepare_req,
    mock_opm,
    mock_update_build_state,
    mock_td,
    mock_prepare_git,
    mock_fetch_index_db,
    mock_get_present,
    mock_get_missing,
    mock_opm_add,
    mock_get_deprecations,
    mock_deprecate,
    mock_opm_migrate,
    mock_get_image_label,
    mock_path_isdir,
    mock_rmtree,
    mock_merge,
    mock_chmod,
    mock_write_meta,
    mock_git_commit,
    mock_monitor,
    mock_replicate,
    mock_update_pull_spec,
    mock_push_index_db,
    mock_cleanup_mr,
    mock_set_state,
    mock_cleanup_failure,
    mock_makedirs,
    mock_copytree,
):
    # Mock input
    bundles = ['some-bundle:latest']
    request_id = 123
    resolved_bundles = ['some-bundle@sha256:123456']
    binary_image = 'binary-image:latest'

    # Mock successful pre-build steps
    mock_get_resolved.return_value = resolved_bundles
    prebuild_info = {
        'from_index_resolved': 'from-index@sha256:abcdef',
        'binary_image_resolved': 'binary-image@sha256:fedcba',
        'arches': {'amd64'},
        'bundle_mapping': {'some-operator': resolved_bundles},
        'ocp_version': 'v4.12',
        'distribution_scope': 'prod',
        'binary_image': binary_image,
    }
    mock_prepare_req.return_value = prebuild_info

    # Mock git repo preparation
    mock_prepare_git.return_value = (mock.Mock(), '/tmp/repo', '/tmp/repo/catalog')

    # Mock TD
    mock_td.return_value.__enter__.return_value = '/tmp/iib-test'

    # Mock path existence for the copytree loop (prevents real FS access)
    mock_path_isdir.return_value = True

    # Mock present bundles check
    mock_get_present.return_value = ([], [])
    mock_get_missing.return_value = resolved_bundles

    # Mock OPM migrate to return valid paths
    mock_opm_migrate.return_value = ('/tmp/from_db', None)

    # Setup a failure deeper in the process
    mock_git_commit.side_effect = Exception("Git error")

    with pytest.raises(IIBError, match="Failed to add bundles: Git error"):
        build_containerized_add.handle_containerized_add_request(
            bundles=bundles, request_id=request_id, from_index="index:latest"
        )

    # Verify cleanup was called
    mock_cleanup_failure.assert_called_once()
    args, kwargs = mock_cleanup_failure.call_args
    assert kwargs['request_id'] == request_id
    assert "Git error" in kwargs['reason']

    # Verify successful path wasn't completed
    mock_push_index_db.assert_not_called()
