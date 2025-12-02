# SPDX-License-Identifier: GPL-3.0-or-later
import os
import pytest
from unittest import mock

from iib.exceptions import IIBError
from iib.workers.tasks import build_containerized_rm
from iib.workers.tasks.utils import RequestConfigAddRm


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_image_digest')
@mock.patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils._get_artifact_combined_tag')
@mock.patch('iib.workers.tasks.containerized_utils._get_name_and_tag_from_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
def test_handle_containerized_rm_request_success_with_overwrite(
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gntfp,
    mock_gact,
    mock_giap,
    mock_gid,
    mock_poa,
    mock_gwc,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
):
    """Test successful operator removal with overwrite_from_index."""
    # Setup
    request_id = 1
    operators = ['operator1', 'operator2']
    from_index = 'quay.io/namespace/index-image:v4.14'
    binary_image = 'registry.io/binary:latest'
    overwrite_from_index_token = 'user:token'

    # Mock temp directory
    temp_dir = '/tmp/iib-1-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    # Mock prepare_request_for_build
    mock_prfb.return_value = {
        'arches': {'amd64', 's390x'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc123',
        'from_index_resolved': 'quay.io/namespace/index-image@sha256:def456',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    # Mock OPM
    mock_opm.opm_version = 'v1.28.0'

    # Mock git operations
    mock_ggt.return_value = ('token_name', 'git_token')

    # Mock file system operations
    mock_exists.return_value = True

    # Mock pull_index_db_artifact
    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir

    # Mock verify_operators_exists
    mock_voe.return_value = ({'operator1', 'operator2'}, os.path.join(artifact_dir, 'index.db'))

    # Mock opm_registry_rm_fbc
    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    # Mock git commit
    mock_glcs.return_value = 'abc123commit'

    # Mock Konflux pipeline
    mock_fpr.return_value = [{'metadata': {'name': 'pipelinerun-123'}}]
    mock_wfpc.return_value = {'status': 'success'}
    mock_gpiu.return_value = 'quay.io/konflux/built-image@sha256:xyz789'

    # Mock ORAS push related functions
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'
    mock_gid.return_value = 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abc'

    # Mock worker config
    mock_gwc.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Test
    build_containerized_rm.handle_containerized_rm_request(
        operators=operators,
        request_id=request_id,
        from_index=from_index,
        binary_image=binary_image,
        overwrite_from_index=True,
        overwrite_from_index_token=overwrite_from_index_token,
        index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
    )

    # Verify prepare_request_for_build was called
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigAddRm(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=None,
            distribution_scope=None,
            binary_image_config=None,
        ),
    )

    # Verify OPM version was set
    mock_opm.set_opm_version.assert_called_once()

    # Verify git operations
    mock_cgr.assert_called_once()

    # Verify operators were removed from catalog and index.db
    assert mock_rmtree.call_count >= 1  # At least for operator removal
    mock_orrf.assert_called_once()

    # Verify catalog merge and validation
    mock_mcd.assert_called_once()
    mock_ov.assert_called_once()

    # Verify commit was pushed (not MR since overwrite_from_index_token is provided)
    mock_cap.assert_called_once()
    commit_msg = mock_cap.call_args[1]['commit_message']
    assert f'IIB: Remove operators for request {request_id}' in commit_msg
    assert 'Operators: operator1, operator2' in commit_msg

    # Verify Konflux pipeline was triggered and waited on
    mock_fpr.assert_called_once_with('abc123commit')
    mock_wfpc.assert_called_once_with('pipelinerun-123')

    # Verify image was copied
    assert mock_sc.call_count >= 1

    # Verify index.db was pushed (2 times: request_id tag + v4.x tag)
    assert mock_poa.call_count == 2

    # Verify final state
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][0] == request_id
    assert final_call[0][1] == 'complete'
    assert 'successfully removed' in final_call[0][2]

    # Verify reset_docker_config was called
    assert mock_rdc.call_count >= 1


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils._get_artifact_combined_tag')
@mock.patch('iib.workers.tasks.containerized_utils._get_name_and_tag_from_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
def test_handle_containerized_rm_request_with_mr(
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cmr,
    mock_close_mr,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gntfp,
    mock_gact,
    mock_giap,
    mock_poa,
    mock_gwc,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
):
    """Test operator removal without overwrite creates and closes MR."""
    # Setup
    request_id = 2
    operators = ['test-operator']
    from_index = 'quay.io/namespace/index-image:v4.14'

    # Mock temp directory
    temp_dir = '/tmp/iib-2-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    # Mock prepare_request_for_build
    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc123',
        'from_index_resolved': 'quay.io/namespace/index-image@sha256:def456',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    # Mock OPM
    mock_opm.opm_version = 'v1.28.0'

    # Mock git operations
    mock_ggt.return_value = ('token_name', 'git_token')

    # Mock file system
    mock_exists.return_value = True

    # Mock artifact pull
    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir

    # Mock operators exist
    mock_voe.return_value = ({'test-operator'}, os.path.join(artifact_dir, 'index.db'))

    # Mock opm operation
    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    # Mock MR creation
    mock_cmr.return_value = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/1', 'mr_id': 1}
    mock_glcs.return_value = 'commit_sha_123'

    # Mock Konflux
    mock_fpr.return_value = [{'metadata': {'name': 'pr-456'}}]
    mock_wfpc.return_value = {'status': 'success'}
    mock_gpiu.return_value = 'quay.io/konflux/image@sha256:built'

    # Mock ORAS push related functions (only request_id tag, no overwrite)
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'

    # Mock config
    mock_gwc.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Test - without overwrite_from_index_token
    build_containerized_rm.handle_containerized_rm_request(
        operators=operators,
        request_id=request_id,
        from_index=from_index,
        index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
    )

    # Verify MR was created
    mock_cmr.assert_called_once()
    commit_msg = mock_cmr.call_args[1]['commit_message']
    assert f'IIB: Remove operators for request {request_id}' in commit_msg
    assert 'Operators: test-operator' in commit_msg

    # Verify MR was closed
    mock_close_mr.assert_called_once()

    # Verify completion
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][1] == 'complete'


@pytest.mark.parametrize(
    'operators_in_db, should_call_opm_rm',
    [
        (set(), False),  # No operators in DB
        ({'operator1'}, True),  # Operators in DB
        ({'op1', 'op2'}, True),  # Multiple operators in DB
    ],
)
@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_image_digest')
@mock.patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils._get_artifact_combined_tag')
@mock.patch('iib.workers.tasks.containerized_utils._get_name_and_tag_from_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
def test_handle_containerized_rm_conditional_opm_rm(
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gntfp,
    mock_gact,
    mock_giap,
    mock_gid,
    mock_poa,
    mock_gwc,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
    operators_in_db,
    should_call_opm_rm,
):
    """Test that opm_registry_rm_fbc is only called when operators exist in DB."""
    # Setup
    request_id = 3
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    # Mock temp directory
    temp_dir = '/tmp/iib-3-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    # Mock prepare_request_for_build
    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'token_value')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir

    # Mock verify_operators_exists to return the parameterized operators_in_db
    mock_voe.return_value = (operators_in_db, os.path.join(artifact_dir, 'index.db'))

    # Mock opm_registry_rm_fbc
    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    # Mock pipeline
    mock_glcs.return_value = 'commit'
    mock_fpr.return_value = [{'metadata': {'name': 'pr'}}]
    mock_wfpc.return_value = {}
    mock_gpiu.return_value = 'image@sha'

    # Mock ORAS push related functions (conditionally used based on operators_in_db)
    mock_gntfp.return_value = ('index', 'v4.14')
    mock_gact.return_value = 'index-v4.14'
    mock_giap.return_value = 'reg/index-db:v4.14'
    mock_gid.return_value = 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abc'

    mock_gwc.return_value = {
        'iib_registry': 'reg',
        'iib_image_push_template': '{registry}/iib:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Test
    build_containerized_rm.handle_containerized_rm_request(
        operators=operators,
        request_id=request_id,
        from_index=from_index,
        overwrite_from_index=True,
        overwrite_from_index_token='user:token',
        index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
    )

    # Verify opm_registry_rm_fbc called only when operators exist in DB
    if should_call_opm_rm:
        mock_orrf.assert_called_once()
        mock_mcd.assert_called_once()
        mock_rename.assert_called_once()
    else:
        mock_orrf.assert_not_called()
        mock_mcd.assert_not_called()
        mock_rename.assert_not_called()


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
def test_handle_containerized_rm_missing_git_mapping(
    mock_prfb,
    mock_uiibs,
    mock_opm,
    mock_tempdir,
    mock_srs,
    mock_rdc,
):
    """Test that missing git mapping raises error."""
    request_id = 4
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-4-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    # Mock OPM to avoid version check
    mock_opm.opm_version = 'v1.28.0'

    # Test with empty git mapping
    with pytest.raises(IIBError, match='Git repository mapping not found'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            index_to_gitlab_push_map={},  # Empty mapping
        )

    # Test with None git mapping
    with pytest.raises(IIBError, match='Git repository mapping not found'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            index_to_gitlab_push_map=None,  # None mapping
        )


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_rm_missing_configs_dir(
    mock_srs_utils,
    mock_makedirs,
    mock_exists,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_cof,
    mock_rdc,
):
    """Test that missing configs directory raises error."""
    request_id = 5
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-5-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')

    # Mock exists to return False for configs directory specifically
    def exists_side_effect(path):
        return 'configs' not in path

    mock_exists.side_effect = exists_side_effect

    # Test
    with pytest.raises(IIBError, match='Catalogs directory not found'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
        )

    # Verify cleanup was NOT called (error happens before try block)
    mock_cof.assert_not_called()


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_rm_missing_index_db(
    mock_srs_utils,
    mock_makedirs,
    mock_exists,
    mock_rmtree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_cof,
    mock_rdc,
):
    """Test that missing index.db file raises error."""
    request_id = 6
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-6-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')

    # Mock file system - configs exists but index.db doesn't
    def exists_side_effect(path):
        return 'index.db' not in path

    mock_exists.side_effect = exists_side_effect

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir

    # Test
    with pytest.raises(IIBError, match='Index.db file not found'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
        )

    # Verify cleanup was NOT called (error happens before try block)
    mock_cof.assert_not_called()


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_rm_pipeline_failure(
    mock_srs_utils,
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_cof,
    mock_rdc,
):
    """Test that pipeline failure triggers cleanup."""
    request_id = 7
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-7-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir
    mock_voe.return_value = ({'operator1'}, os.path.join(artifact_dir, 'index.db'))

    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    mock_glcs.return_value = 'commit_sha'

    # Mock pipeline to raise error
    mock_fpr.side_effect = IIBError('Pipeline not found')

    # Test
    with pytest.raises(IIBError, match='Failed to remove operators'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            overwrite_from_index=True,
            overwrite_from_index_token='user:token',
            index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()
    cleanup_call = mock_cof.call_args
    assert cleanup_call[1]['request_id'] == request_id
    assert 'Pipeline not found' in cleanup_call[1]['reason']


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_image_digest')
@mock.patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils._get_artifact_combined_tag')
@mock.patch('iib.workers.tasks.containerized_utils._get_name_and_tag_from_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
def test_handle_containerized_rm_with_index_db_push(
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gntfp,
    mock_gact,
    mock_giap,
    mock_gid,
    mock_poa,
    mock_gwc,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
):
    """Test that index.db is pushed when operators exist in DB and overwrite token is provided."""
    request_id = 8
    operators = ['operator1', 'operator2']
    from_index = 'quay.io/namespace/index-image:v4.14'
    overwrite_token = 'user:token'

    temp_dir = '/tmp/iib-8-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index-image@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    index_db_path = os.path.join(artifact_dir, 'index.db')
    mock_pida.return_value = artifact_dir

    # Operators exist in DB
    mock_voe.return_value = ({'operator1', 'operator2'}, index_db_path)

    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    mock_glcs.return_value = 'commit'
    mock_fpr.return_value = [{'metadata': {'name': 'pr'}}]
    mock_wfpc.return_value = {}
    mock_gpiu.return_value = 'image@sha'

    # Mock ORAS push related functions
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'
    mock_gid.return_value = 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab'

    mock_gwc.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Test
    build_containerized_rm.handle_containerized_rm_request(
        operators=operators,
        request_id=request_id,
        from_index=from_index,
        overwrite_from_index=True,
        overwrite_from_index_token=overwrite_token,
        index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
    )

    # Verify index.db was pushed (2 times: request_id tag + v4.x tag)
    assert mock_poa.call_count == 2

    # Verify original digest was captured
    mock_gid.assert_called_once()

    # Verify annotations were added
    first_push_call = mock_poa.call_args_list[0]
    assert 'annotations' in first_push_call[1]
    assert first_push_call[1]['annotations']['request_id'] == str(request_id)
    assert first_push_call[1]['annotations']['request_type'] == 'rm'


@pytest.mark.parametrize(
    'build_tags, expected_tag_count',
    [
        (None, 1),  # Only request_id
        (['latest'], 2),  # request_id + latest
        (['latest', 'v4.14'], 3),  # request_id + latest + v4.14
    ],
)
@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_rm_with_build_tags(
    mock_srs_utils,
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gwc,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
    build_tags,
    expected_tag_count,
):
    """Test that build_tags parameter results in correct number of skopeo copies."""
    request_id = 9
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-9-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir
    mock_voe.return_value = (set(), os.path.join(artifact_dir, 'index.db'))

    mock_glcs.return_value = 'commit'
    mock_fpr.return_value = [{'metadata': {'name': 'pr'}}]
    mock_wfpc.return_value = {}
    mock_gpiu.return_value = 'image@sha'

    mock_gwc.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Test
    build_containerized_rm.handle_containerized_rm_request(
        operators=operators,
        request_id=request_id,
        from_index=from_index,
        overwrite_from_index=True,
        overwrite_from_index_token='user:token',
        build_tags=build_tags,
        index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
    )

    # Verify skopeo_copy was called correct number of times
    assert mock_sc.call_count == expected_tag_count


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils._get_artifact_combined_tag')
@mock.patch('iib.workers.tasks.containerized_utils._get_name_and_tag_from_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
def test_handle_containerized_rm_close_mr_failure_logged(
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cmr,
    mock_close_mr,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gntfp,
    mock_gact,
    mock_giap,
    mock_poa,
    mock_gwc,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
):
    """Test that MR close failure is logged but doesn't fail the request."""
    request_id = 10
    operators = ['test-operator']
    from_index = 'quay.io/namespace/index-image:v4.14'

    temp_dir = '/tmp/iib-10-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc123',
        'from_index_resolved': 'quay.io/namespace/index-image@sha256:def456',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token_name', 'git_token')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir
    mock_voe.return_value = ({'test-operator'}, os.path.join(artifact_dir, 'index.db'))

    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    mock_cmr.return_value = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/1', 'mr_id': 1}
    mock_glcs.return_value = 'commit_sha_123'

    mock_fpr.return_value = [{'metadata': {'name': 'pr-456'}}]
    mock_wfpc.return_value = {'status': 'success'}
    mock_gpiu.return_value = 'quay.io/konflux/image@sha256:built'

    # Mock ORAS push related functions
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'

    mock_gwc.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Mock close_mr to raise error
    mock_close_mr.side_effect = IIBError('Failed to close MR')

    # Test - should complete successfully despite MR close failure
    build_containerized_rm.handle_containerized_rm_request(
        operators=operators,
        request_id=request_id,
        from_index=from_index,
        index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
    )

    # Verify MR was attempted to be closed
    mock_close_mr.assert_called_once()

    # Verify request still completed successfully
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][1] == 'complete'


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.rename')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_rm_pipelinerun_missing_name(
    mock_srs_utils,
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_rename,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_cof,
    mock_rdc,
):
    """Test error when pipelinerun metadata doesn't contain name."""
    request_id = 11
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-11-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir
    mock_voe.return_value = ({'operator1'}, os.path.join(artifact_dir, 'index.db'))

    fbc_dir = os.path.join(temp_dir, 'fbc')
    mock_orrf.return_value = (fbc_dir, None)

    mock_glcs.return_value = 'commit'

    # Mock pipelinerun without 'name' in metadata
    mock_fpr.return_value = [{'metadata': {}}]  # Missing 'name' key

    # Test
    with pytest.raises(IIBError, match='Pipelinerun name not found'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            overwrite_from_index=True,
            overwrite_from_index_token='user:token',
            index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()


@mock.patch('iib.workers.tasks.build_containerized_rm.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_rm.cleanup_on_failure')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.build_containerized_rm.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_rm.merge_catalogs_dirs')
@mock.patch('iib.workers.tasks.build_containerized_rm.opm_registry_rm_fbc')
@mock.patch('iib.workers.tasks.build_containerized_rm.verify_operators_exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_rm.remove_operator_deprecations')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_rm._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.Opm')
@mock.patch('iib.workers.tasks.build_containerized_rm.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_rm.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_rm.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_rm.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.path.exists')
@mock.patch('iib.workers.tasks.build_containerized_rm.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_rm_missing_output_pull_spec(
    mock_srs_utils,
    mock_makedirs,
    mock_exists,
    mock_copytree,
    mock_rmtree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_rod,
    mock_pida,
    mock_voe,
    mock_orrf,
    mock_mcd,
    mock_ov,
    mock_wbm,
    mock_cap,
    mock_glcs,
    mock_fpr,
    mock_wfpc,
    mock_gpiu,
    mock_gwc_utils,
    mock_gwc,
    mock_sc,
    mock_cof,
    mock_rdc,
):
    """Test error when output_pull_spec is not set (defensive check)."""
    request_id = 12
    operators = ['operator1']
    from_index = 'quay.io/namespace/index:v4.14'

    temp_dir = '/tmp/iib-12-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    mock_opm.opm_version = 'v1.28.0'
    mock_ggt.return_value = ('token', 'value')
    mock_exists.return_value = True

    artifact_dir = os.path.join(temp_dir, 'artifact')
    mock_pida.return_value = artifact_dir
    mock_voe.return_value = (set(), os.path.join(artifact_dir, 'index.db'))

    mock_glcs.return_value = 'commit'
    mock_fpr.return_value = [{'metadata': {'name': 'pr'}}]
    mock_wfpc.return_value = {}
    mock_gpiu.return_value = 'image@sha'

    # Mock worker config to return empty string for template (defensive edge case)
    # Need to mock both: one for containerized_utils and one for build_containerized_rm
    config_with_empty_template = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '',  # Empty template results in empty output_pull_spec
    }
    mock_gwc_utils.return_value = config_with_empty_template
    mock_gwc.return_value = config_with_empty_template

    # Test
    with pytest.raises(IIBError, match='output_pull_spec was not set'):
        build_containerized_rm.handle_containerized_rm_request(
            operators=operators,
            request_id=request_id,
            from_index=from_index,
            overwrite_from_index=True,
            overwrite_from_index_token='user:token',
            index_to_gitlab_push_map={'quay.io/namespace/index': 'https://gitlab.com/repo'},
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()
