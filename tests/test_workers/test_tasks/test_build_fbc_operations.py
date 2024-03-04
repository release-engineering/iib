from unittest import mock


from iib.workers.tasks import build_fbc_operations
from iib.workers.tasks.utils import RequestConfigFBCOperation


@mock.patch('iib.workers.tasks.build_fbc_operations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_fbc_operations._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_fbc_operations._push_image')
@mock.patch('iib.workers.tasks.build_fbc_operations._build_image')
@mock.patch('iib.workers.tasks.build_fbc_operations._add_label_to_index')
@mock.patch('iib.workers.tasks.build_fbc_operations.opm_registry_add_fbc_fragment')
@mock.patch('iib.workers.tasks.build_fbc_operations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_fbc_operations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.build_fbc_operations.get_resolved_image')
@mock.patch('iib.workers.tasks.build_fbc_operations.set_request_state')
@mock.patch('iib.workers.tasks.build_fbc_operations._cleanup')
@mock.patch('iib.workers.tasks.opm_operations.Opm.set_opm_version')
def test_handle_fbc_operation_request(
    mock_sov,
    mock_cleanup,
    mock_srs,
    mock_gri,
    mock_ugri,
    mock_prfb,
    mock_uiibs,
    mock_oraff,
    mock_alti,
    mock_bi,
    mock_pi,
    mock_cpml,
    mock_uiips,
):
    request_id = 10
    from_index = 'from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragment = 'fbc-fragment:latest'
    arches = {'amd64', 's390x'}
    from_index_resolved = 'from-index@sha256:bcdefg'

    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.6',
        'distribution_scope': "prod",
    }
    mock_gri.return_value = 'fbc-fragment@sha256:qwerty'

    build_fbc_operations.handle_fbc_operation_request(
        request_id=request_id,
        fbc_fragment=fbc_fragment,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
    )
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=None,
            add_arches=None,
            binary_image_config=binary_image_config,
            distribution_scope='prod',
            fbc_fragment='fbc-fragment@sha256:qwerty',
        ),
    )
    mock_sov.assert_called_once_with(from_index_resolved)
    mock_oraff.assert_called_once()
    mock_cpml.assert_called_once_with(request_id, {'s390x', 'amd64'}, None)
    assert mock_srs.call_count == 3
    assert mock_alti.call_count == 2
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    assert mock_srs.call_args[0][1] == 'complete'
