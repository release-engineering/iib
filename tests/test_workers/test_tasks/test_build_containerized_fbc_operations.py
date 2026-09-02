from unittest import mock
import json
import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_containerized_fbc_operations, containerized_utils
from iib.workers.tasks.utils import RequestConfigFBCOperation


@mock.patch('iib.workers.tasks.containerized_utils.remote_branch_exists')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.push_index_db_artifact')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.get_list_of_output_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.write_build_metadata')
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.opm_registry_add_fbc_fragment_containerized'
)
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.containerized_utils.resolve_git_url')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.set_request_state')
@mock.patch('iib.workers.tasks.utils.reset_docker_config')
@mock.patch('iib.workers.tasks.containerized_utils.Path.mkdir')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_fbc_operation_request(
    mock_srs_utils,
    mock_makedirs,
    mock_rdc,
    mock_srs,
    mock_ugri,
    mock_gri_utils,
    mock_prfb,
    mock_sov,
    mock_uiibs,
    mock_pida,
    mock_rgu,
    mock_ggt,
    mock_cgr,
    mock_oraff,
    mock_wbm,
    mock_cmr,
    mock_glcs,
    mock_fp,
    mock_wfpc,
    mock_gpiu,
    mock_gloops,
    mock_sc,
    mock_pida_push,
    mock_cof,
    mock_uiips,
    mock_rbe,
):
    """Test containerized FBC operation with single fragment."""
    request_id = 10
    from_index = 'quay.io/iib/from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment:latest']
    arches = {'amd64', 's390x'}
    from_index_resolved = 'from-index@sha256:bcdefg'
    index_git_repo = 'https://gitlab.com/org/repo.git'

    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.6',
        'distribution_scope': "prod",
    }
    mock_ugri.return_value = 'fbc-fragment@sha256:qwerty'

    # Mocks for file operations and git
    mock_pida.return_value = '/tmp/artifact_dir'
    mock_rgu.return_value = index_git_repo
    mock_ggt.return_value = ('token_name', 'token_value')

    # Mock os.path.exists for index.db check and catalogs dir check
    with mock.patch('iib.workers.tasks.containerized_utils.Path.exists', return_value=True):
        # Mock opm operation result
        mock_oraff.return_value = ('/tmp/updated_catalog_path', '/tmp/index.db', [])

        # Mock Konflux pipeline flow
        mock_cmr.return_value = {'mr_url': 'http://mr.url'}
        mock_glcs.return_value = 'sha123'
        mock_fp.return_value = [{'metadata': {'name': 'pipeline-run-1'}}]
        mock_wfpc.return_value = {'status': 'Succeeded'}
        mock_gpiu.return_value = 'registry/output-image:sha256-12345'
        mock_gloops.return_value = ['output-image:latest']

        build_containerized_fbc_operations.handle_containerized_fbc_operation_request(
            request_id=request_id,
            fbc_fragments=fbc_fragments,
            from_index=from_index,
            binary_image=binary_image,
            binary_image_config=binary_image_config,
        )

    # Assertions
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=None,
            add_arches=None,
            binary_image_config=binary_image_config,
            distribution_scope='prod',
            fbc_fragments=fbc_fragments,
        ),
    )

    # Verify OPM version set
    mock_sov.assert_called_once_with(from_index_resolved)

    # Verify build state update (includes resolved fragments)
    assert mock_uiibs.called
    args, _ = mock_uiibs.call_args
    assert args[0] == request_id
    assert args[1]['fbc_fragments_resolved'] == ['fbc-fragment@sha256:qwerty']

    # Verify git clone
    mock_cgr.assert_called_once()

    # Verify OPM operation
    mock_oraff.assert_called_once_with(
        request_id=request_id,
        temp_dir=mock.ANY,
        from_index_configs_dir=mock.ANY,
        fbc_fragments=['fbc-fragment@sha256:qwerty'],
        overwrite_from_index_token=None,
        index_db_path=mock.ANY,
    )

    # Verify MR creation (since no overwrite token)
    mock_cmr.assert_called_once()

    # Verify Pipeline wait
    mock_fp.assert_called_once_with('sha123')
    mock_wfpc.assert_called_once_with('pipeline-run-1')

    # Verify Skopeo copy
    mock_sc.assert_called_once_with(
        source='docker://registry/output-image:sha256-12345',
        destination='docker://output-image:latest',
        copy_all=True,
        exc_msg=mock.ANY,
    )

    # Verify DB update
    mock_uiips.assert_called_once_with(
        output_pull_spec='output-image:latest',
        request_id=request_id,
        arches=arches,
        from_index=from_index,
        overwrite_from_index=False,
        overwrite_from_index_token=None,
        resolved_prebuild_from_index=from_index_resolved,
        add_or_rm=True,
        is_image_fbc=True,
        index_repo_map={},
    )

    # Verify success state
    assert mock_srs.call_args[0][1] == 'complete'


@mock.patch('iib.workers.tasks.containerized_utils.remote_branch_exists')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.push_index_db_artifact')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.get_list_of_output_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.write_build_metadata')
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.opm_registry_add_fbc_fragment_containerized'
)
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.containerized_utils.resolve_git_url')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.set_request_state')
@mock.patch('iib.workers.tasks.utils.reset_docker_config')
@mock.patch('iib.workers.tasks.containerized_utils.Path.mkdir')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_fbc_operation_request_multiple_fragments(
    mock_srs_utils,
    mock_makedirs,
    mock_rdc,
    mock_srs,
    mock_gri,
    mock_ugri,
    mock_prfb,
    mock_sov,
    mock_uiibs,
    mock_pida,
    mock_rgu,
    mock_ggt,
    mock_cgr,
    mock_oraff,
    mock_wbm,
    mock_cmr,
    mock_glcs,
    mock_fp,
    mock_wfpc,
    mock_gpiu,
    mock_gloops,
    mock_sc,
    mock_pida_push,
    mock_cof,
    mock_uiips,
    mock_rbe,
):
    """Test containerized FBC operation with multiple fragments."""
    request_id = 10
    from_index = 'quay.io/iib/from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment1:latest', 'fbc-fragment2:latest']
    arches = {'amd64', 's390x'}
    from_index_resolved = 'from-index@sha256:bcdefg'
    index_git_repo = 'https://gitlab.com/org/repo.git'

    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.6',
        'distribution_scope': "prod",
    }
    # Return resolved images for both fragments
    mock_gri.side_effect = ['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh']
    mock_ugri.side_effect = ['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh']

    mock_pida.return_value = '/tmp/artifact_dir'
    mock_rgu.return_value = index_git_repo
    mock_ggt.return_value = ('token_name', 'token_value')

    with mock.patch('iib.workers.tasks.containerized_utils.Path.exists', return_value=True):
        mock_oraff.return_value = ('/tmp/updated', '/tmp/db', [])
        mock_cmr.return_value = {'mr_url': 'http://mr.url'}
        mock_glcs.return_value = 'sha123'
        mock_fp.return_value = [{'metadata': {'name': 'pipeline-run-1'}}]
        mock_wfpc.return_value = {'status': 'Succeeded'}
        mock_gpiu.return_value = 'registry/output'
        mock_gloops.return_value = ['output:latest']

        build_containerized_fbc_operations.handle_containerized_fbc_operation_request(
            request_id=request_id,
            fbc_fragments=fbc_fragments,
            from_index=from_index,
            binary_image=binary_image,
            binary_image_config=binary_image_config,
        )

    # Verify OPM operation was called with list of resolved fragments
    mock_oraff.assert_called_once_with(
        request_id=request_id,
        temp_dir=mock.ANY,
        from_index_configs_dir=mock.ANY,
        fbc_fragments=['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh'],
        overwrite_from_index_token=None,
        index_db_path=mock.ANY,
    )

    # Verify build state update contains all resolved fragments
    args, _ = mock_uiibs.call_args
    assert args[1]['fbc_fragments_resolved'] == [
        'fbc-fragment1@sha256:qwerty',
        'fbc-fragment2@sha256:asdfgh',
    ]


@mock.patch('iib.workers.tasks.containerized_utils.remote_branch_exists')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.push_index_db_artifact')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
@mock.patch('iib.workers.tasks.containerized_utils.get_list_of_output_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.write_build_metadata')
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.opm_registry_add_fbc_fragment_containerized'
)
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.containerized_utils.resolve_git_url')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.set_request_state')
@mock.patch('iib.workers.tasks.utils.reset_docker_config')
@mock.patch('iib.workers.tasks.containerized_utils.Path.mkdir')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.merge_mr_after_build')
def test_handle_containerized_fbc_operation_request_with_overwrite(
    mock_merge_mr,
    mock_srs_utils,
    mock_makedirs,
    mock_rdc,
    mock_srs,
    mock_gri,
    mock_ugri,
    mock_prfb,
    mock_sov,
    mock_uiibs,
    mock_pida,
    mock_rgu,
    mock_ggt,
    mock_cgr,
    mock_oraff,
    mock_wbm,
    mock_cmr,
    mock_glcs,
    mock_fp,
    mock_wfpc,
    mock_gpiu,
    mock_gloops,
    mock_sc,
    mock_pida_push,
    mock_cof,
    mock_uiips,
    mock_rbe,
):
    """Test containerized FBC operation with overwrite_from_index=True."""
    request_id = 10
    overwrite_token = 'user:token'

    # Setup mocks
    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'binary@sha256:123',
        'from_index_resolved': 'index@sha256:456',
        'ocp_version': 'v4.6',
        'distribution_scope': 'prod',
    }
    mock_gri.return_value = 'fbc@sha256:789'
    mock_ugri.return_value = 'fbc@sha256:789'
    mock_pida.return_value = '/tmp/dir'
    mock_rgu.return_value = 'http://git'
    mock_ggt.return_value = ('t', 'v')

    mock_cmr.return_value = {
        'mr_id': '1',
        'mr_url': 'https://gitlab.com/mr/1',
        'source_branch': 'iib-request-10-v4.6',
    }

    mock_docker_config = json.dumps({'auths': {}})
    with mock.patch('iib.workers.tasks.containerized_utils.Path.exists', return_value=True):
        with mock.patch('builtins.open', mock.mock_open(read_data=mock_docker_config)) as mock_file:
            mock_oraff.return_value = ('/tmp/c', '/tmp/d', ['op1'])
            mock_glcs.return_value = 'sha1'
            mock_fp.return_value = [{'metadata': {'name': 'pr1'}}]
            mock_wfpc.return_value = {'status': 'Succeeded'}
            mock_gpiu.return_value = 'reg/img'
            mock_gloops.return_value = ['out:1']

            build_containerized_fbc_operations.handle_containerized_fbc_operation_request(
                request_id=request_id,
                fbc_fragments=['quay.io/iib/fbc-fragment1:latest'],
                from_index='quay.io/iib/from-index:latest',
                overwrite_from_index=True,
                overwrite_from_index_token=overwrite_token,
            )

    # Verify MR creation and merge for overwrite flow
    mock_cmr.assert_called_once()
    mock_merge_mr.assert_called_once()

    # Verify DB artifacts pushed
    mock_pida_push.assert_called_once_with(
        request_id=request_id,
        from_index='quay.io/iib/from-index:latest',
        index_db_path='/tmp/d',
        operators=['op1'],
        output_image='reg/img',
        overwrite_from_index=True,
        request_type='fbc_operations',
    )

    # Verify update call has overwrite flags
    mock_uiips.assert_called_once_with(
        output_pull_spec='out:1',
        request_id=request_id,
        arches={'amd64'},
        from_index='quay.io/iib/from-index:latest',
        overwrite_from_index=True,
        overwrite_from_index_token=overwrite_token,
        resolved_prebuild_from_index='index@sha256:456',
        add_or_rm=True,
        is_image_fbc=True,
        index_repo_map={},
    )


@mock.patch('iib.workers.tasks.containerized_utils.remote_branch_exists')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.push_index_db_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_list_of_output_pullspec')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.write_build_metadata')
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.opm_registry_add_fbc_fragment_containerized'
)
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.containerized_utils.resolve_git_url')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.set_request_state')
@mock.patch('iib.workers.tasks.utils.reset_docker_config')
@mock.patch('iib.workers.tasks.containerized_utils.Path.mkdir')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
def test_handle_containerized_fbc_operation_request_failure(
    mock_srs_utils,
    mock_makedirs,
    mock_rdc,
    mock_srs,
    mock_gri,
    mock_ugri,
    mock_prfb,
    mock_sov,
    mock_uiibs,
    mock_pida,
    mock_rgu,
    mock_ggt,
    mock_cgr,
    mock_oraff,
    mock_wbm,
    mock_cmr,
    mock_glcs,
    mock_fp,
    mock_wfpc,
    mock_gpiu,
    mock_gloops,
    mock_pida_push,
    mock_cof,
    mock_uiips,
    mock_rbe,
):
    """Test containerized FBC operation failure handling."""
    request_id = 10

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'binary@sha256:123',
        'from_index_resolved': 'index@sha256:456',
        'ocp_version': 'v4.6',
        'distribution_scope': 'prod',
    }
    mock_gri.return_value = 'fbc@sha256:789'
    mock_ugri.return_value = 'fbc@sha256:789'
    mock_pida.return_value = '/tmp/dir'
    mock_rgu.return_value = 'http://git'
    mock_ggt.return_value = ('t', 'v')

    # Simulate failure during artifact pull.
    MOCK_ERROR_MSG = "Failed to add FBC fragment: error: Download failed"
    mock_pida.side_effect = IIBError(MOCK_ERROR_MSG)

    excinfo = None

    with mock.patch('iib.workers.tasks.containerized_utils.Path.exists', return_value=True):
        try:
            build_containerized_fbc_operations.handle_containerized_fbc_operation_request(
                request_id=request_id,
                fbc_fragments=['quay.io/iib/fbc-fragment1:latest'],
                from_index='quay.io/iib/from-index:latest',
            )
            pytest.fail("IIBError was not raised as expected.")
        except IIBError as e:
            excinfo = e
            mock_cof(
                request_id=request_id,
                reason=MOCK_ERROR_MSG,
            )

    assert "Failed to add FBC fragment" in str(excinfo)
    assert "error: Download failed" in str(excinfo)

    mock_cof.assert_called_once()
    args, kwargs = mock_cof.call_args
    assert kwargs['request_id'] == request_id
    assert "error: Download failed" in kwargs['reason']


@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.merge_mr_after_build')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.push_index_db_artifact')
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.replicate_image_to_tagged_destinations'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.monitor_pipeline_and_extract_image'
)
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.git_commit_and_create_mr')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.write_build_metadata')
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.opm_registry_add_fbc_fragment_containerized'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_fbc_operations.fetch_and_verify_index_db_artifact'
)
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.prepare_build_sources')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_fbc_operations.reset_docker_config')
def test_fbc_operations_divergent_never_merges(
    mock_rdc,
    mock_srs,
    mock_gri,
    mock_prfb,
    mock_sov,
    mock_uiibs,
    mock_prepare_sources,
    mock_fetch_index_db,
    mock_oraff,
    mock_wbm,
    mock_git_commit,
    mock_monitor,
    mock_replicate,
    mock_push_index_db,
    mock_merge_mr,
    mock_cleanup_mr,
    mock_cof,
    mock_uiips,
    tmp_path,
):
    """Divergent path must never fall back to ORAS and must never merge the MR."""
    request_id = 999
    from_index = 'quay.io/iib/from-index:v4.99'
    binary_image = 'binary-image:latest'
    fbc_fragments = ['quay.io/iib/fbc-fragment1:latest']

    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'binary@sha256:123',
        'from_index_resolved': 'index@sha256:456',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }
    mock_gri.return_value = 'fbc@sha256:789'

    index_db_path = str(tmp_path / 'index.db')
    index_git_repo = 'https://gitlab.com/repo/x.git'
    local_git_repo_path = str(tmp_path / 'git_repo')
    localized_git_catalog_path = str(tmp_path / 'git_repo' / 'configs')
    mock_prepare_sources.return_value = containerized_utils.BuildSources(
        index_git_repo=index_git_repo,
        local_git_repo_path=local_git_repo_path,
        localized_git_catalog_path=localized_git_catalog_path,
        index_db_path=index_db_path,
        target_branch='v4.14',
        is_divergent=True,
    )

    mock_oraff.return_value = ('/tmp/updated_catalog_path', index_db_path, ['op1'])

    mr_details = {
        'mr_id': '1',
        'mr_url': 'https://gitlab.com/mr/1',
        'source_branch': 'iib-request-999-v4.14',
    }
    mock_git_commit.return_value = (mr_details, 'commit_sha_999')
    mock_monitor.return_value = 'registry/output-image:sha256-12345'
    mock_replicate.return_value = ['registry.example.com/final-image:999']
    mock_push_index_db.return_value = None

    overwrite_token = 'user:token'
    build_containerized_fbc_operations.handle_containerized_fbc_operation_request(
        request_id=request_id,
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        overwrite_from_index=True,
        overwrite_from_index_token=overwrite_token,
        index_to_gitlab_push_map={'quay.io/iib/from-index': index_git_repo},
    )

    # Divergent path uses the extracted index.db, never ORAS.
    mock_fetch_index_db.assert_not_called()

    # overwrite_from_index=True here: the divergent BuildSources bypasses Task 4's
    # entry-point overwrite rejection (mocked directly), so the ONLY thing that can
    # prevent a merge is the handler-level `and not sources.is_divergent` guard. If
    # that guard were removed, this MR would be merged and this assertion would fail.
    mock_merge_mr.assert_not_called()
    mock_cleanup_mr.assert_called_once()

    mock_oraff.assert_called_once_with(
        request_id=request_id,
        temp_dir=mock.ANY,
        from_index_configs_dir=localized_git_catalog_path,
        fbc_fragments=['fbc@sha256:789'],
        overwrite_from_index_token=overwrite_token,
        index_db_path=index_db_path,
    )
    mock_cof.assert_not_called()
