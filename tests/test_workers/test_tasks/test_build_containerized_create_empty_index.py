# SPDX-License-Identifier: GPL-3.0-or-later
import os
import pytest
from unittest import mock

from iib.exceptions import IIBError
from iib.workers.tasks import build_containerized_create_empty_index
from iib.workers.tasks.utils import RequestConfigCreateIndexImage


@mock.patch('builtins.open', new_callable=mock.mock_open)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.cleanup_on_failure')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_pull_spec'
)
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
@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_oras_artifact')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.os.path.exists')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_build_state'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Opm')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Path')
def test_handle_containerized_create_empty_index_primary_path(
    mock_path_class,
    mock_rmtree,
    mock_copytree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_exists,
    mock_makedirs,
    mock_gwc_local,
    mock_goa,
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
    mock_gid,
    mock_poa,
    mock_gwc_utils,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
    mock_open_file,
):
    """Test successful empty index creation using pre-existing empty index.db artifact."""
    # Setup
    request_id = 1
    from_index = 'quay.io/namespace/index-image:v4.14'

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
    mock_exists.return_value = True

    # Mock Path operations
    mock_path_instance = mock.MagicMock()
    mock_path_instance.is_file.return_value = True
    mock_path_instance.is_dir.return_value = True
    mock_path_instance.__truediv__ = lambda self, other: mock_path_instance
    mock_path_instance.__str__ = lambda self: '/tmp/iib-1-test/index.db'
    mock_path_class.return_value = mock_path_instance

    # Mock get_worker_config for empty tag
    mock_gwc_local.return_value = {
        'iib_empty_index_db_tag': 'empty',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
    }

    # Mock ORAS artifact fetch (primary path - empty artifact exists)
    artifact_dir = os.path.join(temp_dir, 'oras_artifact')
    mock_goa.return_value = artifact_dir

    # Mock MR creation
    mock_cmr.return_value = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/1', 'mr_id': 1}
    mock_glcs.return_value = 'commit_sha_123'

    # Mock Konflux pipeline
    mock_fpr.return_value = [{'metadata': {'name': 'pr-456'}}]
    mock_wfpc.return_value = {'status': 'success'}
    mock_gpiu.return_value = 'quay.io/konflux/image@sha256:built'

    # Mock ORAS push related functions
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'
    mock_gid.return_value = 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abc'

    # Mock worker config for utils
    mock_gwc_utils.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Mock metadata file read/write for labels
    import json as json_module

    mock_metadata_content = json_module.dumps({"labels": {"existing_label": "value"}})
    # Configure mock_open to handle both read and write
    read_data = mock.MagicMock()
    read_data.read.return_value = mock_metadata_content
    mock_open_file.return_value.__enter__.return_value = read_data

    # Test with custom labels
    custom_labels = {'custom_label': 'custom_value', 'another_label': 'another_value'}
    build_containerized_create_empty_index.handle_containerized_create_empty_index_request(
        from_index=from_index,
        request_id=request_id,
        labels=custom_labels,
        index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
    )

    # Verify prepare_request_for_build was called
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigCreateIndexImage(
            _binary_image=None,
            from_index=from_index,
            binary_image_config=None,
        ),
    )

    # Verify OPM version was set
    mock_opm.set_opm_version.assert_called_once()

    # Verify git operations
    mock_cgr.assert_called_once()

    # Verify empty artifact was fetched (primary path)
    mock_goa.assert_called_once()

    # Verify .gitkeep file was created by checking open was called
    # (indirectly verified by successful execution)

    # Verify catalog validation
    mock_ov.assert_called_once()

    # Verify MR was created (overwrite_from_index=False)
    mock_cmr.assert_called_once()

    # Verify MR was closed
    mock_close_mr.assert_called_once()

    # Verify index.db was pushed with empty operators list
    assert mock_poa.call_count == 1  # Only request_id tag since overwrite_from_index=False

    # Verify completion
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][1] == 'complete'
    assert 'successfully created' in final_call[0][2]

    # Verify reset_docker_config was called
    assert mock_rdc.call_count >= 1


@pytest.mark.parametrize(
    'operators_in_db, opm_rm_side_effect, expected_opm_rm_calls, verify_permissive',
    [
        # Normal fallback: fetch from_index and remove operators
        (['operator1', 'operator2'], None, 1, False),
        # Permissive mode: first call fails, second succeeds with permissive=True
        (
            ['operator1'],
            [IIBError('Error deleting packages from database'), None],
            2,
            True,
        ),
        # Index already empty: no operators found, _opm_registry_rm is not called
        ([], None, 0, False),
    ],
)
@mock.patch('builtins.open', new_callable=mock.mock_open)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.cleanup_on_failure')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_pull_spec'
)
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
@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index._opm_registry_rm')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_operator_package_list')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index.fetch_and_verify_index_db_artifact'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_oras_artifact')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.os.path.exists')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_build_state'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Opm')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Path')
def test_handle_containerized_create_empty_index_fallback(
    mock_path_class,
    mock_rmtree,
    mock_copytree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_exists,
    mock_makedirs,
    mock_gwc_local,
    mock_goa,
    mock_favida,
    mock_gopl,
    mock_orm,
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
    mock_gid,
    mock_poa,
    mock_gwc_utils,
    mock_srs_utils,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
    mock_open_file,
    operators_in_db,
    opm_rm_side_effect,
    expected_opm_rm_calls,
    verify_permissive,
):
    """Test empty index creation using fallback path (fetch from_index and remove operators).

    Covers:
    1. Normal fallback: operators removed successfully on first try
    2. Permissive mode: first removal fails, second succeeds with permissive=True
    3. Already empty: no operators in DB, opm_registry_rm not called
    """
    # Setup
    request_id = 2
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
    mock_exists.return_value = True

    # Mock Path operations
    mock_path_instance = mock.MagicMock()
    mock_path_instance.is_file.return_value = True
    mock_path_instance.is_dir.return_value = True
    mock_path_instance.__truediv__ = lambda self, other: mock_path_instance
    mock_path_instance.__str__ = lambda self: '/tmp/iib-2-test/index.db'
    mock_path_class.return_value = mock_path_instance

    # Mock get_worker_config for empty tag
    mock_gwc_local.return_value = {
        'iib_empty_index_db_tag': 'empty',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
    }

    # Mock ORAS artifact fetch to fail (trigger fallback)
    mock_goa.side_effect = IIBError('Empty artifact not found')

    # Mock fallback path: fetch from from_index
    index_db_path = os.path.join(temp_dir, 'artifact', 'index.db')
    mock_favida.return_value = index_db_path

    # Mock operators in DB
    mock_gopl.return_value = operators_in_db

    # Mock opm_registry_rm with potential permissive mode
    if opm_rm_side_effect:
        mock_orm.side_effect = opm_rm_side_effect

    # Mock MR creation
    mock_cmr.return_value = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/2', 'mr_id': 2}
    mock_glcs.return_value = 'commit_sha_456'

    # Mock Konflux pipeline
    mock_fpr.return_value = [{'metadata': {'name': 'pr-789'}}]
    mock_wfpc.return_value = {'status': 'success'}
    mock_gpiu.return_value = 'quay.io/konflux/image@sha256:fallback'

    # Mock ORAS push related functions
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'
    mock_gid.return_value = 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abc'

    # Mock worker config for utils
    mock_gwc_utils.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Test
    build_containerized_create_empty_index.handle_containerized_create_empty_index_request(
        from_index=from_index,
        request_id=request_id,
        index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
    )

    # Verify fallback path was taken
    mock_goa.assert_called_once()  # Primary path attempted
    mock_favida.assert_called_once()  # Fallback triggered

    # Verify operators were fetched from index.db
    mock_gopl.assert_called_once()

    # Verify opm_registry_rm was called correct number of times
    assert mock_orm.call_count == expected_opm_rm_calls

    # For permissive mode, verify second call has permissive=True
    if verify_permissive:
        second_call = mock_orm.call_args_list[1]
        assert second_call[1]['permissive'] is True

    # Verify catalog validation
    mock_ov.assert_called_once()

    # Verify MR was created and closed
    mock_cmr.assert_called_once()
    mock_close_mr.assert_called_once()

    # Verify index.db was pushed
    assert mock_poa.call_count == 1

    # Verify completion
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][1] == 'complete'


@mock.patch('builtins.open', new_callable=mock.mock_open)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.cleanup_on_failure')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_oras_artifact')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.os.path.exists')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_build_state'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Opm')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Path')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_create_empty_index_pipeline_failure(
    mock_srs_utils,
    mock_path_class,
    mock_rmtree,
    mock_copytree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_exists,
    mock_makedirs,
    mock_gwc_local,
    mock_goa,
    mock_ov,
    mock_wbm,
    mock_cmr,
    mock_glcs,
    mock_fpr,
    mock_cof,
    mock_rdc,
    mock_open_file,
):
    """Test that pipeline failure triggers cleanup."""
    request_id = 3
    from_index = 'quay.io/namespace/index-image:v4.14'

    temp_dir = '/tmp/iib-3-test'
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

    # Mock Path operations
    mock_path_instance = mock.MagicMock()
    mock_path_instance.is_file.return_value = True
    mock_path_instance.is_dir.return_value = True
    mock_path_instance.__truediv__ = lambda self, other: mock_path_instance
    mock_path_instance.__str__ = lambda self: '/tmp/iib-3-test/index.db'
    mock_path_class.return_value = mock_path_instance

    mock_gwc_local.return_value = {
        'iib_empty_index_db_tag': 'empty',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
    }

    # Mock successful artifact fetch
    artifact_dir = os.path.join(temp_dir, 'oras_artifact')
    mock_goa.return_value = artifact_dir

    # Mock MR creation
    mock_cmr.return_value = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/3', 'mr_id': 3}
    mock_glcs.return_value = 'commit_sha'

    # Mock pipeline to raise error
    mock_fpr.side_effect = IIBError('Pipeline not found')

    # Test
    with pytest.raises(IIBError, match='Failed to create empty index'):
        build_containerized_create_empty_index.handle_containerized_create_empty_index_request(
            from_index=from_index,
            request_id=request_id,
            index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()
    cleanup_call = mock_cof.call_args
    assert cleanup_call[1]['request_id'] == request_id
    assert 'Pipeline not found' in cleanup_call[1]['reason']


@pytest.mark.parametrize('index_to_gitlab_push_map', [None, {}])
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Opm')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_build_state'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.prepare_request_for_build')
def test_handle_containerized_create_empty_index_missing_git_mapping(
    mock_prfb,
    mock_uiibs,
    mock_opm,
    mock_tempdir,
    mock_srs,
    mock_rdc,
    index_to_gitlab_push_map,
):
    """Test that missing git mapping raises error."""
    request_id = 4
    from_index = 'quay.io/namespace/index-image:v4.14'

    temp_dir = '/tmp/iib-4-test'
    mock_tempdir.return_value.__enter__.return_value = temp_dir

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/namespace/index-image@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    # Mock OPM to avoid version check
    mock_opm.opm_version = 'v1.28.0'

    # Test that the missing/empty mapping raises error
    with pytest.raises(IIBError, match='Git repository mapping not found'):
        build_containerized_create_empty_index.handle_containerized_create_empty_index_request(
            from_index=from_index,
            request_id=request_id,
            index_to_gitlab_push_map=index_to_gitlab_push_map,
        )


@mock.patch('builtins.open', new_callable=mock.mock_open)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.reset_docker_config')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.cleanup_on_failure')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_pull_spec'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index.replicate_image_to_tagged_destinations'  # noqa: E501
)
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
@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.write_build_metadata')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.opm_validate')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index._opm_registry_rm')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_operator_package_list')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index.fetch_and_verify_index_db_artifact'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_oras_artifact')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.os.makedirs')
@mock.patch('iib.workers.tasks.containerized_utils.os.path.exists')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch(
    'iib.workers.tasks.build_containerized_create_empty_index._update_index_image_build_state'
)
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Opm')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.copytree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.shutil.rmtree')
@mock.patch('iib.workers.tasks.build_containerized_create_empty_index.Path')
def test_handle_containerized_create_empty_index_unexpected_opm_error(
    mock_path_class,
    mock_rmtree,
    mock_copytree,
    mock_tempdir,
    mock_srs,
    mock_prfb,
    mock_opm,
    mock_uiibs,
    mock_ggt,
    mock_cgr,
    mock_exists,
    mock_makedirs,
    mock_gwc_local,
    mock_goa,
    mock_favida,
    mock_gopl,
    mock_orm,
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
    mock_gid,
    mock_poa,
    mock_gwc_utils,
    mock_srs_utils,
    mock_ritd,
    mock_sc,
    mock_uiips,
    mock_cof,
    mock_rdc,
    mock_open_file,
):
    """Test unexpected IIBError during operator removal in fallback path (line 106)."""
    request_id = 5
    from_index = 'quay.io/namespace/index-image:v4.14'
    temp_dir = '/tmp/iib-5-test'

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

    # Mock Path operations
    mock_path_instance = mock.MagicMock()
    mock_path_instance.is_file.return_value = True
    mock_path_instance.is_dir.return_value = True
    mock_path_instance.__truediv__ = lambda self, other: mock_path_instance
    mock_path_instance.__str__ = lambda self: '/tmp/iib-5-test/index.db'
    mock_path_class.return_value = mock_path_instance

    mock_gwc_local.return_value = {
        'iib_empty_index_db_tag': 'empty',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
    }

    # Trigger fallback path
    mock_goa.side_effect = IIBError('Empty artifact not found')
    mock_favida.return_value = os.path.join(temp_dir, 'artifact', 'index.db')
    mock_gopl.return_value = ['operator1']
    # Set up the unexpected OPM error
    mock_orm.side_effect = IIBError('Unexpected OPM error')

    # Pipeline flow setup
    mock_cmr.return_value = {'mr_url': 'https://gitlab.com/repo/-/merge_requests/5', 'mr_id': 5}
    mock_glcs.return_value = 'commit_sha'
    mock_fpr.return_value = [{'metadata': {'name': 'pr-123'}}]
    mock_wfpc.return_value = {'status': 'success'}
    mock_gpiu.return_value = 'quay.io/konflux/image@sha256:built'

    # Mock ORAS push related functions
    mock_gntfp.return_value = ('index-image', 'v4.14')
    mock_gact.return_value = 'index-image-v4.14'
    mock_giap.return_value = 'registry.io/index-db:v4.14'
    mock_gid.return_value = 'sha256:abc'
    mock_ritd.return_value = ['registry.io/iib-build:5']

    mock_gwc_utils.return_value = {
        'iib_registry': 'registry.io',
        'iib_image_push_template': '{registry}/iib-build:{request_id}',
        'iib_index_db_artifact_registry': 'artifact-registry.io',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }

    # Execute and verify error is raised
    with pytest.raises(IIBError, match='Unexpected OPM error'):
        build_containerized_create_empty_index.handle_containerized_create_empty_index_request(
            from_index=from_index,
            request_id=request_id,
            index_to_gitlab_push_map={'quay.io/namespace/index-image': 'https://gitlab.com/repo'},
        )
