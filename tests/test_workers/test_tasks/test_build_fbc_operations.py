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
    fbc_fragments = ['fbc-fragment:latest']
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
        fbc_fragments=fbc_fragments,
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
            fbc_fragments=fbc_fragments,
        ),
    )
    mock_sov.assert_called_once_with(from_index_resolved)
    mock_oraff.assert_called_once_with(
        request_id,
        mock.ANY,  # temp_dir
        from_index_resolved,
        'binary-image@sha256:abcdef',
        ['fbc-fragment@sha256:qwerty'],  # Now a list
        None,  # overwrite_from_index_token
    )
    mock_cpml.assert_called_once_with(request_id, {'s390x', 'amd64'}, None)
    assert mock_srs.call_count == 3  # 3 original calls (no internal calls due to mocking)
    assert mock_alti.call_count == 2
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    assert mock_srs.call_args[0][1] == 'complete'


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
def test_handle_fbc_operation_request_multiple_fragments(
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
    fbc_fragments = ['fbc-fragment1:latest', 'fbc-fragment2:latest']
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
    mock_gri.side_effect = ['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh']

    build_fbc_operations.handle_fbc_operation_request(
        request_id=request_id,
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
    )

    # Verify that opm_registry_add_fbc_fragment was called once with all fragments
    assert mock_oraff.call_count == 1

    # Verify the call was made with the correct resolved fragments list
    mock_oraff.assert_called_once_with(
        request_id,
        mock.ANY,  # temp_dir
        from_index_resolved,
        'binary-image@sha256:abcdef',
        ['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh'],  # List of fragments
        None,  # overwrite_from_index_token
    )

    # Verify final completion message mentions both fragments
    completion_call = mock_srs.call_args_list[-1]
    assert '2 FBC fragment(s) were successfully added' in completion_call[0][2]

    mock_sov.assert_called_once_with(from_index_resolved)
    mock_cpml.assert_called_once_with(request_id, {'s390x', 'amd64'}, None)
    assert mock_srs.call_count == 3  # 3 original calls (no internal calls due to mocking)
    assert mock_alti.call_count == 2
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    assert mock_srs.call_args[0][1] == 'complete'


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
def test_handle_fbc_operation_request_empty_fragments(
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
    fbc_fragments = []
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

    build_fbc_operations.handle_fbc_operation_request(
        request_id=request_id,
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
    )

    # Verify that opm_registry_add_fbc_fragment was called with empty list
    mock_oraff.assert_called_once_with(
        request_id,
        mock.ANY,  # temp_dir
        from_index_resolved,
        'binary-image@sha256:abcdef',
        [],  # Empty list of fragments
        None,  # overwrite_from_index_token
    )

    # Verify completion message mentions 0 fragments
    completion_call = mock_srs.call_args_list[-1]
    assert '0 FBC fragment(s) were successfully added' in completion_call[0][2]

    mock_sov.assert_called_once_with(from_index_resolved)
    mock_cpml.assert_called_once_with(request_id, {'s390x', 'amd64'}, None)
    assert mock_srs.call_count == 3
    assert mock_alti.call_count == 2
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    assert mock_srs.call_args[0][1] == 'complete'


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
def test_handle_fbc_operation_request_with_overwrite_token(
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
    """Test FBC operation with overwrite_from_index_token."""
    request_id = 10
    from_index = 'from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment1:latest', 'fbc-fragment2:latest']
    overwrite_from_index_token = 'user:password'
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
    mock_gri.side_effect = ['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh']

    build_fbc_operations.handle_fbc_operation_request(
        request_id=request_id,
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
        overwrite_from_index_token=overwrite_from_index_token,
    )

    # Verify that opm_registry_add_fbc_fragment was called once with all fragments and token
    assert mock_oraff.call_count == 1

    # Verify the call was made with the correct resolved fragments list and token
    mock_oraff.assert_called_once_with(
        request_id,
        mock.ANY,  # temp_dir
        from_index_resolved,
        'binary-image@sha256:abcdef',
        ['fbc-fragment1@sha256:qwerty', 'fbc-fragment2@sha256:asdfgh'],  # List of fragments
        overwrite_from_index_token,
    )

    # Verify config was called with token
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=None,
            binary_image_config=binary_image_config,
            distribution_scope='prod',
            fbc_fragments=fbc_fragments,
        ),
    )


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
def test_handle_fbc_operation_request_with_build_tags(
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
    """Test FBC operation with build tags."""
    request_id = 10
    from_index = 'from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment:latest']
    build_tags = {'tag1', 'tag2'}
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
    mock_cpml.return_value = 'output-image:latest'

    build_fbc_operations.handle_fbc_operation_request(
        request_id=request_id,
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
        build_tags=build_tags,
    )

    # Verify create_and_push_manifest_list was called with build tags
    mock_cpml.assert_called_once_with(request_id, {'s390x', 'amd64'}, build_tags)


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
def test_handle_fbc_operation_request_with_add_arches(
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
    """Test FBC operation with add_arches."""
    request_id = 10
    from_index = 'from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment:latest']
    add_arches = {'ppc64le', 'arm64'}
    arches = {'amd64', 's390x', 'ppc64le', 'arm64'}
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
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
        add_arches=add_arches,
    )

    # Verify config was called with add_arches
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=None,
            add_arches=add_arches,
            binary_image_config=binary_image_config,
            distribution_scope='prod',
            fbc_fragments=fbc_fragments,
        ),
    )


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
def test_handle_fbc_operation_request_with_distribution_scope(
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
    """Test FBC operation with distribution_scope."""
    request_id = 10
    from_index = 'from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment:latest']
    distribution_scope = 'stage'
    arches = {'amd64', 's390x'}
    from_index_resolved = 'from-index@sha256:bcdefg'

    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.6',
        'distribution_scope': distribution_scope,
    }
    mock_gri.return_value = 'fbc-fragment@sha256:qwerty'

    build_fbc_operations.handle_fbc_operation_request(
        request_id=request_id,
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
        distribution_scope=distribution_scope,
    )

    # Verify config was called with distribution_scope
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=None,
            add_arches=None,
            binary_image_config=binary_image_config,
            distribution_scope=distribution_scope,
            fbc_fragments=fbc_fragments,
        ),
    )


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
def test_handle_fbc_operation_request_with_overwrite_from_index(
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
    """Test FBC operation with overwrite_from_index flag."""
    request_id = 10
    from_index = 'from-index:latest'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    fbc_fragments = ['fbc-fragment:latest']
    overwrite_from_index = True
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
        fbc_fragments=fbc_fragments,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
        overwrite_from_index=overwrite_from_index,
    )

    # Verify _update_index_image_pull_spec was called with overwrite_from_index
    mock_uiips.assert_called_once_with(
        output_pull_spec=mock.ANY,
        request_id=request_id,
        arches=arches,
        from_index=from_index,
        overwrite_from_index=overwrite_from_index,
        overwrite_from_index_token=None,
        resolved_prebuild_from_index=from_index_resolved,
        add_or_rm=True,
    )
