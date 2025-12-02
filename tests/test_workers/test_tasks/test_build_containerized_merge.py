# SPDX-License-Identifier: GPL-3.0-or-later
import os
import pytest
from unittest import mock

from iib.exceptions import IIBError
from iib.workers.tasks import build_containerized_merge
from iib.workers.tasks.utils import RequestConfigMerge


# Store original set before mocking
_original_set = set


def _mock_set_for_bundles(iterable):
    """Define a helper to handle set() calls on lists of dictionaries (bundles)."""
    if not iterable:
        return _original_set()
    # Convert to list if needed
    if not isinstance(iterable, (list, tuple)):
        iterable = list(iterable)
    if len(iterable) > 0 and isinstance(iterable[0], dict):
        # For bundles (dicts), deduplicate based on bundlePath
        seen_paths = []
        result = []
        for item in iterable:
            bundle_path = item.get('bundlePath', str(item))
            if bundle_path not in seen_paths:
                seen_paths.append(bundle_path)
                result.append(item)

        # Return a set-like object that can be converted to list
        class SetLike:
            def __init__(self, items):
                self.items = items

            def __iter__(self):
                return iter(self.items)

            def __len__(self):
                return len(self.items)

        return SetLike(result)
    # For other types, use the real set
    return _original_set(iterable)


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_success(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test successful merge request with all operations."""
    # Setup
    request_id = 1
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'
    deprecation_list = ['bundle1:1.0', 'bundle2:2.0']
    binary_image = 'registry.io/binary:latest'
    overwrite_target_index_token = 'user:token'

    # Mock temp directory
    temp_dir = '/tmp/iib-1-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    # Mock prepare_request_for_build
    prebuild_info = {
        'arches': {'amd64', 's390x'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc123',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def456',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi789',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    # Mock OPM
    mock_opm.opm_version = 'v1.28.0'

    # Mock git repository setup
    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    # Mock index.db artifacts
    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    # Mock bundles
    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'},
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111', 'bundle2@sha256:222']
    target_bundles = [
        {'bundlePath': 'bundle3@sha256:333', 'csvName': 'bundle3-3.0', 'packageName': 'bundle3'},
        {'bundlePath': 'bundle4@sha256:444', 'csvName': 'bundle4-4.0', 'packageName': 'bundle4'},
    ]
    target_bundles_pull_spec = ['bundle3@sha256:333', 'bundle4@sha256:444']
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    # Mock missing bundles
    missing_bundles = [
        {'bundlePath': 'bundle3@sha256:333', 'csvName': 'bundle3-3.0', 'packageName': 'bundle3'},
    ]
    invalid_bundles = []
    mock_gmbfts.return_value = (missing_bundles, invalid_bundles)

    # Mock deprecation bundles
    mock_gbfdl.return_value = ['bundle1:1.0']
    mock_gblv.return_value = ['bundle1:1.0']

    # Mock FBC migration
    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    # Mock bundles in DB
    bundles_in_db = [
        {'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle3@sha256:333', 'packageName': 'bundle3'},
    ]
    mock_glb.return_value = bundles_in_db

    # Mock file system operations
    mock_exists.return_value = True

    # Mock git commit
    mr_details = None
    last_commit_sha = 'abc123commit'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    # Mock Konflux pipeline
    image_url = 'quay.io/konflux/built-image@sha256:xyz789'
    mock_mpaei.return_value = image_url

    # Mock image replication
    output_pull_specs = ['quay.io/iib/iib-build:1']
    mock_ritd.return_value = output_pull_specs

    # Mock index.db push
    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=deprecation_list,
        request_id=request_id,
        binary_image=binary_image,
        target_index=target_index,
        overwrite_target_index=True,
        overwrite_target_index_token=overwrite_target_index_token,
        distribution_scope='prod',
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify prepare_request_for_build was called
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigMerge(
            _binary_image=binary_image,
            overwrite_target_index_token=overwrite_target_index_token,
            source_from_index=source_from_index,
            target_index=target_index,
            distribution_scope='prod',
            binary_image_config=None,
        ),
    )

    # Verify OPM version was set
    mock_opm.set_opm_version.assert_called_once_with(prebuild_info['target_index_resolved'])

    # Verify git repository was prepared
    mock_pgrfb.assert_called_once()

    # Verify index.db artifacts were fetched
    assert mock_favida.call_count == 2

    # Verify bundles were retrieved
    assert mock_gpb.call_count == 2

    # Verify bundles were validated
    mock_vbip.assert_called_once()
    # Verify it was called with List[str] format (pullspec strings)
    call_args = mock_vbip.call_args
    bundles_arg = call_args[0][0] if call_args[0] else call_args[1]['bundles']
    assert isinstance(bundles_arg, list)
    # All items should be strings (pullspecs), not BundleImage dicts
    assert all(isinstance(b, str) for b in bundles_arg)
    # Verify expected bundles are in the list
    expected_bundles = set(source_bundles_pull_spec + target_bundles_pull_spec)
    assert set(bundles_arg) == expected_bundles

    # Verify missing bundles were identified
    mock_gmbfts.assert_called_once()

    # Verify missing bundles were added
    mock_ora.assert_called_once()

    # Verify deprecation was processed
    mock_dbd.assert_called_once()

    # Verify FBC migration
    mock_om.assert_called_once()

    # Verify catalog merge
    mock_mcd.assert_called_once()

    # Verify FBC validation
    mock_ov.assert_called_once()

    # Verify build metadata was written
    mock_wbm.assert_called_once()

    # Verify git commit/push
    mock_gccmop.assert_called_once()

    # Verify pipeline monitoring
    mock_mpaei.assert_called_once()

    # Verify image replication
    mock_ritd.assert_called_once()

    # Verify index.db push
    mock_pida.assert_called_once()

    # Verify final state
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][0] == request_id
    assert final_call[0][1] == 'complete'
    assert 'successfully merged' in final_call[0][2]

    # Verify reset_docker_config was called
    assert mock_rdc.call_count >= 1


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_success_with_deprecations(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test successful merge request with deprecations executed correctly."""
    # Setup
    request_id = 9
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'
    deprecation_list = ['bundle1:1.0', 'bundle2:2.0']
    binary_image = 'registry.io/binary:latest'
    overwrite_target_index_token = 'user:token'

    # Mock temp directory
    temp_dir = '/tmp/iib-9-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    # Mock prepare_request_for_build
    prebuild_info = {
        'arches': {'amd64', 's390x'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc123',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def456',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi789',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    # Mock OPM
    mock_opm.opm_version = 'v1.28.0'

    # Mock git repository setup
    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    # Mock index.db artifacts
    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    # Mock bundles
    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'},
        {'bundlePath': 'bundle3@sha256:333', 'csvName': 'bundle3-3.0', 'packageName': 'bundle3'},
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111', 'bundle2@sha256:222', 'bundle3@sha256:333']
    target_bundles = [
        {'bundlePath': 'bundle4@sha256:444', 'csvName': 'bundle4-4.0', 'packageName': 'bundle4'},
    ]
    target_bundles_pull_spec = ['bundle4@sha256:444']
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    # Mock missing bundles
    missing_bundles = [
        {'bundlePath': 'bundle4@sha256:444', 'csvName': 'bundle4-4.0', 'packageName': 'bundle4'},
    ]
    invalid_bundles = []
    mock_gmbfts.return_value = (missing_bundles, invalid_bundles)

    # Mock deprecation bundles - these should be found from the deprecation_list
    deprecation_bundles_from_list = ['bundle1@sha256:111', 'bundle2@sha256:222']
    deprecation_bundles_latest = ['bundle1@sha256:111', 'bundle2@sha256:222']
    mock_gbfdl.return_value = deprecation_bundles_from_list
    mock_gblv.return_value = deprecation_bundles_latest

    # Mock FBC migration
    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    # Mock bundles in DB
    bundles_in_db = [
        {'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'packageName': 'bundle2'},
        {'bundlePath': 'bundle3@sha256:333', 'packageName': 'bundle3'},
        {'bundlePath': 'bundle4@sha256:444', 'packageName': 'bundle4'},
    ]
    mock_glb.return_value = bundles_in_db

    # Mock file system operations
    mock_exists.return_value = True

    # Mock git commit
    mr_details = None
    last_commit_sha = 'abc123commit'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    # Mock Konflux pipeline
    image_url = 'quay.io/konflux/built-image@sha256:xyz789'
    mock_mpaei.return_value = image_url

    # Mock image replication
    output_pull_specs = ['quay.io/iib/iib-build:9']
    mock_ritd.return_value = output_pull_specs

    # Mock index.db push
    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=deprecation_list,
        request_id=request_id,
        binary_image=binary_image,
        target_index=target_index,
        overwrite_target_index=True,
        overwrite_target_index_token=overwrite_target_index_token,
        distribution_scope='prod',
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify prepare_request_for_build was called
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigMerge(
            _binary_image=binary_image,
            overwrite_target_index_token=overwrite_target_index_token,
            source_from_index=source_from_index,
            target_index=target_index,
            distribution_scope='prod',
            binary_image_config=None,
        ),
    )

    # Verify OPM version was set
    mock_opm.set_opm_version.assert_called_once_with(prebuild_info['target_index_resolved'])

    # Verify git repository was prepared
    mock_pgrfb.assert_called_once()

    # Verify index.db artifacts were fetched
    assert mock_favida.call_count == 2

    # Verify bundles were retrieved
    assert mock_gpb.call_count == 2

    # Verify bundles were validated
    mock_vbip.assert_called_once()
    # Verify it was called with List[str] format (pullspec strings)
    call_args = mock_vbip.call_args
    bundles_arg = call_args[0][0] if call_args[0] else call_args[1]['bundles']
    assert isinstance(bundles_arg, list)
    # All items should be strings (pullspecs), not BundleImage dicts
    assert all(isinstance(b, str) for b in bundles_arg)
    # Verify expected bundles are in the list
    expected_bundles = set(source_bundles_pull_spec + target_bundles_pull_spec)
    assert set(bundles_arg) == expected_bundles

    # Verify missing bundles were identified
    mock_gmbfts.assert_called_once()

    # Verify missing bundles were added
    mock_ora.assert_called_once()

    # Verify deprecation processing was executed
    # 1. get_bundles_from_deprecation_list should be called with
    # intermediate_bundles and deprecation_list
    mock_gbfdl.assert_called_once()
    gbfdl_call_args = mock_gbfdl.call_args
    assert deprecation_list == gbfdl_call_args[0][1]
    # Verify intermediate_bundles includes missing bundles + source bundles
    intermediate_bundles = gbfdl_call_args[0][0]
    assert 'bundle4@sha256:444' in intermediate_bundles  # missing bundle
    assert 'bundle1@sha256:111' in intermediate_bundles  # source bundle

    # 2. get_bundles_latest_version should be called with deprecation bundles and all bundles
    mock_gblv.assert_called_once()
    gblv_call_args = mock_gblv.call_args
    assert deprecation_bundles_from_list == gblv_call_args[0][0]
    all_bundles = gblv_call_args[0][1]
    # Verify all_bundles includes both source and target bundles
    assert len(all_bundles) == len(source_bundles) + len(target_bundles)

    # 3. deprecate_bundles_db should be called with the latest deprecation bundles
    mock_dbd.assert_called_once()
    dbd_call_args = mock_dbd.call_args
    assert dbd_call_args[1]['base_dir'] == temp_dir
    assert dbd_call_args[1]['index_db_file'] == source_index_db_path
    assert dbd_call_args[1]['bundles'] == deprecation_bundles_latest
    # Verify the deprecation bundles match what was expected
    assert 'bundle1@sha256:111' in dbd_call_args[1]['bundles']
    assert 'bundle2@sha256:222' in dbd_call_args[1]['bundles']

    # Verify FBC migration
    mock_om.assert_called_once()

    # Verify catalog merge
    mock_mcd.assert_called_once()

    # Verify FBC validation
    mock_ov.assert_called_once()

    # Verify build metadata was written
    mock_wbm.assert_called_once()

    # Verify git commit/push
    mock_gccmop.assert_called_once()

    # Verify pipeline monitoring
    mock_mpaei.assert_called_once()

    # Verify image replication
    mock_ritd.assert_called_once()

    # Verify index.db push
    mock_pida.assert_called_once()

    # Verify final state - operation completed successfully
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][0] == request_id
    assert final_call[0][1] == 'complete'
    assert 'successfully merged' in final_call[0][2]

    # Verify reset_docker_config was called
    assert mock_rdc.call_count >= 1


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_with_mr(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test merge request that creates and closes MR."""
    request_id = 2
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'

    temp_dir = '/tmp/iib-2-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111']
    target_bundles = [
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'}
    ]
    target_bundles_pull_spec = ['bundle2@sha256:222']
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    missing_bundles = [
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'}
    ]
    mock_gmbfts.return_value = (missing_bundles, [])

    mock_gbfdl.return_value = []
    bundles_in_db = [{'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'}]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    # Mock MR creation
    mr_details = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/1', 'mr_id': 1}
    last_commit_sha = 'commit_sha_123'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    output_pull_specs = ['quay.io/iib/iib-build:2']
    mock_ritd.return_value = output_pull_specs

    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test without overwrite_target_index_token (creates MR)
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=[],
        request_id=request_id,
        target_index=target_index,
        overwrite_target_index=False,
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify MR was created
    commit_msg = mock_gccmop.call_args[1]['commit_message']
    assert f'IIB: Merge operators for request {request_id}' in commit_msg

    # Verify completion
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][1] == 'complete'


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_no_missing_bundles(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test merge request when no bundles are missing."""
    request_id = 3
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'

    temp_dir = '/tmp/iib-3-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111']
    target_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    target_bundles_pull_spec = ['bundle1@sha256:111']
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    # No missing bundles
    mock_gmbfts.return_value = ([], [])

    mock_gbfdl.return_value = []
    bundles_in_db = [{'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'}]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    output_pull_specs = ['quay.io/iib/iib-build:3']
    mock_ritd.return_value = output_pull_specs

    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=[],
        request_id=request_id,
        target_index=target_index,
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify _opm_registry_add was called with empty list
    mock_ora.assert_called_once()
    assert mock_ora.call_args[0][2] == []


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_with_deprecation(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test merge request with deprecation list."""
    request_id = 4
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'
    deprecation_list = ['bundle1:1.0', 'bundle2:2.0']

    temp_dir = '/tmp/iib-4-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'},
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111', 'bundle2@sha256:222']
    target_bundles = []
    target_bundles_pull_spec = []
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    mock_gmbfts.return_value = ([], [])

    # Mock deprecation bundles
    mock_gbfdl.return_value = ['bundle1@sha256:111', 'bundle2@sha256:222']
    mock_gblv.return_value = ['bundle1@sha256:111', 'bundle2@sha256:222']

    bundles_in_db = [
        {'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'packageName': 'bundle2'},
    ]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    output_pull_specs = ['quay.io/iib/iib-build:4']
    mock_ritd.return_value = output_pull_specs

    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=deprecation_list,
        request_id=request_id,
        target_index=target_index,
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify deprecation was processed
    mock_gbfdl.assert_called_once()
    mock_gblv.assert_called_once()
    mock_dbd.assert_called_once()


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_pipeline_failure(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_cof,
    mock_rdc,
):
    """Test that pipeline failure triggers cleanup."""
    request_id = 5
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'

    temp_dir = '/tmp/iib-5-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111']
    target_bundles = []
    target_bundles_pull_spec = []
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    mock_gmbfts.return_value = ([], [])

    mock_gbfdl.return_value = []
    bundles_in_db = [{'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'}]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    # Mock pipeline to raise error
    mock_mpaei.side_effect = IIBError('Pipeline not found')

    # Test
    with pytest.raises(IIBError, match='Failed to merge operators'):
        build_containerized_merge.handle_containerized_merge_request(
            source_from_index=source_from_index,
            deprecation_list=[],
            request_id=request_id,
            target_index=target_index,
            index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()
    cleanup_call = mock_cof.call_args
    assert cleanup_call[1]['request_id'] == request_id
    assert 'Pipeline not found' in cleanup_call[1]['reason']


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_missing_output_pull_spec(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_cof,
    mock_rdc,
):
    """Test error when output_pull_spec is not set."""
    request_id = 6
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'

    temp_dir = '/tmp/iib-6-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111']
    target_bundles = []
    target_bundles_pull_spec = []
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    mock_gmbfts.return_value = ([], [])

    mock_gbfdl.return_value = []
    bundles_in_db = [{'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'}]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    # Mock replicate_image_to_tagged_destinations to return empty list
    mock_ritd.return_value = []

    # Test
    with pytest.raises(IIBError, match='list index out of range'):
        build_containerized_merge.handle_containerized_merge_request(
            source_from_index=source_from_index,
            deprecation_list=[],
            request_id=request_id,
            target_index=target_index,
            index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()


@pytest.mark.parametrize(
    'build_tags, expected_tag_count',
    [
        (None, 1),  # Only request_id
        (['latest'], 2),  # request_id + latest
        (['latest', 'v4.14'], 3),  # request_id + latest + v4.14
    ],
)
@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_with_build_tags(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
    build_tags,
    expected_tag_count,
):
    """Test that build_tags parameter results in correct number of image replications."""
    request_id = 7
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'

    temp_dir = '/tmp/iib-7-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111']
    target_bundles = []
    target_bundles_pull_spec = []
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    mock_gmbfts.return_value = ([], [])

    mock_gbfdl.return_value = []
    bundles_in_db = [{'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'}]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    # Mock replicate_image_to_tagged_destinations to return list with expected count
    output_pull_specs = ['quay.io/iib/iib-build:7']
    mock_ritd.return_value = output_pull_specs

    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=[],
        request_id=request_id,
        target_index=target_index,
        build_tags=build_tags,
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify replicate_image_to_tagged_destinations was called with build_tags
    mock_ritd.assert_called_once()
    assert mock_ritd.call_args[1]['build_tags'] == build_tags


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_with_invalid_bundles(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test merge request with invalid bundles (OCP version mismatch)."""
    request_id = 8
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = 'quay.io/namespace/target-index:v4.15'

    temp_dir = '/tmp/iib-8-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': 'quay.io/namespace/target-index@sha256:ghi',
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.15',
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    target_index_db_path = os.path.join(temp_dir, 'target_index.db')
    mock_favida.side_effect = [source_index_db_path, target_index_db_path]

    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'}
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111']
    target_bundles = [
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'},
    ]
    target_bundles_pull_spec = ['bundle2@sha256:222']
    mock_gpb.side_effect = [
        (source_bundles, source_bundles_pull_spec),
        (target_bundles, target_bundles_pull_spec),
    ]

    # Mock invalid bundles (OCP version mismatch)
    invalid_bundles = [
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'},
    ]
    mock_gmbfts.return_value = ([], invalid_bundles)

    # Invalid bundles should be added to deprecation list
    mock_gbfdl.return_value = ['bundle2@sha256:222']
    mock_gblv.return_value = ['bundle2@sha256:222']

    bundles_in_db = [
        {'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'packageName': 'bundle2'},
    ]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    output_pull_specs = ['quay.io/iib/iib-build:8']
    mock_ritd.return_value = output_pull_specs

    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=[],
        request_id=request_id,
        target_index=target_index,
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify invalid bundles were added to deprecation list
    mock_gbfdl.assert_called_once()
    # Verify deprecation was called with invalid bundles
    mock_dbd.assert_called_once()
    deprecation_bundles = mock_dbd.call_args[1]['bundles']
    assert 'bundle2@sha256:222' in deprecation_bundles


@mock.patch('iib.workers.tasks.build_containerized_merge.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_merge.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_merge.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_merge.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_merge.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_merge.git_commit_and_create_mr_or_push')
@mock.patch('iib.workers.tasks.build_containerized_merge.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_merge.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_merge.opm_migrate')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_list_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.deprecate_bundles_db')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_latest_version')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_containerized_merge._opm_registry_add')
@mock.patch('iib.workers.tasks.build_containerized_merge.get_missing_bundles_from_target_to_source')
@mock.patch('iib.workers.tasks.build_containerized_merge.validate_bundles_in_parallel')
@mock.patch('iib.workers.tasks.build_containerized_merge._get_present_bundles')
@mock.patch('iib.workers.tasks.build_containerized_merge.fetch_and_verify_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_git_repository_for_build')
@mock.patch('iib.workers.tasks.build_containerized_merge._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.Opm')
@mock.patch('iib.workers.tasks.build_containerized_merge.prepare_request_for_build')
@mock.patch('iib.workers.api_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_merge.set_registry_token')
@mock.patch('iib.workers.tasks.build_containerized_merge.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_merge.shutil.move')
@mock.patch('iib.workers.tasks.build_containerized_merge.os.path.exists')
@mock.patch('builtins.set', side_effect=_mock_set_for_bundles)
def test_handle_containerized_merge_request_without_target_index(
    mock_set,
    mock_exists,
    mock_move,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_set_registry_token,
    mock_srs,
    mock_srs_api,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_pgrfb,
    mock_favida,
    mock_gpb,
    mock_vbip,
    mock_gmbfts,
    mock_ora,
    mock_gbfdl,
    mock_gblv,
    mock_dbd,
    mock_glb,
    mock_om,
    mock_mcd,
    mock_copytree,
    mock_ov,
    mock_wbm,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_uiips,
    mock_pida,
    mock_cmrif,
    mock_cof,
    mock_rdc,
):
    """Test merge request when target_index is None."""
    request_id = 10
    source_from_index = 'quay.io/namespace/source-index:v4.14'
    target_index = None  # No target index provided

    temp_dir = '/tmp/iib-10-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    prebuild_info = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'source_from_index_resolved': 'quay.io/namespace/source-index@sha256:def',
        'target_index_resolved': None,  # Should be None when target_index is None
        'ocp_version': 'v4.14',
        'target_ocp_version': 'v4.14',  # Should default to source version
        'distribution_scope': 'prod',
    }
    mock_prfb.return_value = prebuild_info

    mock_opm.opm_version = 'v1.28.0'

    index_git_repo = 'https://gitlab.com/repo'
    local_git_repo_path = os.path.join(temp_dir, 'git_repo')
    localized_git_catalog_path = os.path.join(local_git_repo_path, 'catalogs')
    mock_pgrfb.return_value = (index_git_repo, local_git_repo_path, localized_git_catalog_path)

    # Only source index.db should be fetched when target_index is None
    source_index_db_path = os.path.join(temp_dir, 'source_index.db')
    mock_favida.return_value = source_index_db_path

    # Only source bundles should be retrieved
    source_bundles = [
        {'bundlePath': 'bundle1@sha256:111', 'csvName': 'bundle1-1.0', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'csvName': 'bundle2-2.0', 'packageName': 'bundle2'},
    ]
    source_bundles_pull_spec = ['bundle1@sha256:111', 'bundle2@sha256:222']
    mock_gpb.return_value = (source_bundles, source_bundles_pull_spec)

    # No missing bundles since there's no target index
    mock_gmbfts.return_value = ([], [])

    # Mock deprecation bundles
    mock_gbfdl.return_value = ['bundle1@sha256:111']
    mock_gblv.return_value = ['bundle1@sha256:111']

    bundles_in_db = [
        {'bundlePath': 'bundle1@sha256:111', 'packageName': 'bundle1'},
        {'bundlePath': 'bundle2@sha256:222', 'packageName': 'bundle2'},
    ]
    mock_glb.return_value = bundles_in_db

    fbc_dir = os.path.join(temp_dir, 'fbc_catalog')
    mock_om.return_value = (fbc_dir, None)

    mock_exists.return_value = True

    mr_details = None
    last_commit_sha = 'commit_sha'
    mock_gccmop.return_value = (mr_details, last_commit_sha)

    image_url = 'quay.io/konflux/image@sha256:built'
    mock_mpaei.return_value = image_url

    output_pull_specs = ['quay.io/iib/iib-build:10']
    mock_ritd.return_value = output_pull_specs

    original_index_db_digest = 'sha256:original123'
    mock_pida.return_value = original_index_db_digest

    # Test with target_index=None
    build_containerized_merge.handle_containerized_merge_request(
        source_from_index=source_from_index,
        deprecation_list=['bundle1:1.0'],
        request_id=request_id,
        target_index=target_index,  # None
        index_to_gitlab_push_map={'quay.io/namespace/source-index': index_git_repo},
    )

    # Verify only source index.db was fetched (not target)
    assert mock_favida.call_count == 1
    mock_favida.assert_called_once_with(source_from_index, temp_dir)

    # Verify only source bundles were retrieved (not target)
    assert mock_gpb.call_count == 1
    mock_gpb.assert_called_once_with(source_index_db_path, temp_dir)

    # Verify bundles were validated (only source bundles)
    mock_vbip.assert_called_once()
    call_args = mock_vbip.call_args
    bundles_arg = call_args[0][0] if call_args[0] else call_args[1]['bundles']
    assert isinstance(bundles_arg, list)
    assert all(isinstance(b, str) for b in bundles_arg)
    # Should only contain source bundles
    assert set(bundles_arg) == set(source_bundles_pull_spec)

    # Verify get_missing_bundles_from_target_to_source was called with empty target bundles
    mock_gmbfts.assert_called_once()
    gmbfts_call_args = mock_gmbfts.call_args
    assert gmbfts_call_args[1]['target_index_bundles'] == []

    # Verify _opm_registry_add was called with empty list (no missing bundles)
    mock_ora.assert_not_called()

    # Verify deprecation was processed
    mock_gbfdl.assert_called_once()
    mock_gblv.assert_called_once()
    mock_dbd.assert_called_once()

    # Verify final state
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][0] == request_id
    assert final_call[0][1] == 'complete'
    assert 'successfully merged' in final_call[0][2]
