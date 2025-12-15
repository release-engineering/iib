# SPDX-License-Identifier: GPL-3.0-or-later
import json
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_containerized_regenerate_bundle


@pytest.mark.parametrize(
    'pinned_by_iib_label, pinned_by_iib_bool',
    (
        ('true', True),
        ('True', True),
        (None, False),
        ('false', False),
        ('False', False),
    ),
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.cleanup_merge_request_if_exists'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.replicate_image_to_tagged_destinations'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.monitor_pipeline_and_extract_image'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.git_commit_and_create_mr_or_push'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.'
    'extract_files_from_image_non_privileged'
)
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.clone_git_repo')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_labels')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_arches')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.update_request')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_worker_config')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.tempfile.TemporaryDirectory')
def test_handle_containerized_regenerate_bundle_request(
    mock_temp_dir,
    mock_gwc,
    mock_ur,
    mock_srs,
    mock_gri,
    mock_gia,
    mock_gil,
    mock_aob,
    mock_gpa,
    mock_ggt,
    mock_cgr,
    mock_effinp,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_cmrie,
    pinned_by_iib_label,
    pinned_by_iib_bool,
    tmpdir,
):
    """Test successful containerized regenerate bundle request."""
    # Setup
    arches = ['amd64', 's390x']
    from_bundle_image = 'bundle-image:latest'
    from_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    bundle_image = 'quay.io/iib:99'
    organization = 'acme'
    request_id = 99
    bundle_git_repo = 'https://gitlab.com/bundle/repo'

    # Use tmpdir with the mock
    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)

    # Mock worker config
    mock_gwc.return_value = {
        'iib_index_image_output_registry': None,
        'iib_registry': 'quay.io',
    }

    # Mock image resolution and metadata
    mock_gri.return_value = from_bundle_image_resolved
    mock_gia.return_value = list(arches)
    mock_gil.return_value = {'com.redhat.iib.pinned': pinned_by_iib_label}

    # Mock Git token
    mock_ggt.return_value = ('GITLAB_TOKEN', 'test-token')

    # Mock bundle adjustments
    mock_aob.return_value = {'operators.operatorframework.io.bundle.package.v1': 'test-package'}
    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'test-package'}
    }

    # Mock Git operations
    mock_gccmop.return_value = (
        {'mr_id': '123', 'mr_url': 'https://gitlab.com/merge_requests/123'},
        'commit-sha-123',
    )

    # Mock pipeline monitoring
    mock_mpaei.return_value = 'quay.io/konflux/bundle:sha256-abc123.att'

    # Mock image replication
    mock_ritd.return_value = [bundle_image]

    # Execute
    build_containerized_regenerate_bundle.handle_containerized_regenerate_bundle_request(
        from_bundle_image=from_bundle_image,
        organization=organization,
        request_id=request_id,
        bundle_replacements={},
        index_to_gitlab_push_map={'regenerate-bundle': bundle_git_repo},
        regenerate_bundle_repo_key='regenerate-bundle',
    )

    # Verify calls
    mock_gri.assert_called_once_with(from_bundle_image)
    mock_gia.assert_called_once_with(from_bundle_image_resolved)
    mock_gil.assert_called_once_with(from_bundle_image_resolved)

    # Verify Git operations
    mock_ggt.assert_called_once_with(bundle_git_repo)
    mock_cgr.assert_called_once()

    # Verify file extraction (manifests and metadata)
    assert mock_effinp.call_count == 2

    # Verify bundle adjustment
    mock_aob.assert_called_once()

    # Verify Git commit and MR creation
    mock_gccmop.assert_called_once()

    # Verify pipeline monitoring
    mock_mpaei.assert_called_once_with(
        request_id=request_id,
        last_commit_sha='commit-sha-123',
    )

    # Verify image replication
    mock_ritd.assert_called_once()

    # Verify MR cleanup
    mock_cmrie.assert_called_once()

    # Verify request updates
    assert mock_ur.call_count == 2

    # Verify _adjust_operator_bundle was called with correct pinned_by_iib value
    mock_aob.assert_called_once()
    call_kwargs = mock_aob.call_args[1]
    assert call_kwargs['pinned_by_iib'] == pinned_by_iib_bool

    # Verify Dockerfile creation
    dockerfile_path = tmpdir.join('git', 'regenerate-bundle', 'Dockerfile')
    assert dockerfile_path.check()

    # Verify metadata file creation and contents
    metadata_file = tmpdir.join('git', 'regenerate-bundle', '.iib-build-metadata.json')
    assert metadata_file.check()
    metadata_content = json.loads(metadata_file.read())
    assert metadata_content['request_id'] == request_id
    assert metadata_content['arches'] == sorted(list(arches))
    assert metadata_content['organization'] == organization
    assert metadata_content['package_name'] == 'test-package'


@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.cleanup_on_failure')
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.git_commit_and_create_mr_or_push'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.'
    'extract_files_from_image_non_privileged'
)
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.clone_git_repo')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_labels')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_arches')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.update_request')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_worker_config')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.tempfile.TemporaryDirectory')
def test_handle_containerized_regenerate_bundle_request_failure(
    mock_temp_dir,
    mock_gwc,
    mock_ur,
    mock_srs,
    mock_gri,
    mock_gia,
    mock_gil,
    mock_aob,
    mock_gpa,
    mock_ggt,
    mock_cgr,
    mock_effinp,
    mock_gccmop,
    mock_cof,
    tmpdir,
):
    """Test containerized regenerate bundle request failure triggers cleanup."""
    # Setup
    arches = ['amd64']
    from_bundle_image = 'bundle-image:latest'
    from_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    organization = 'acme'
    request_id = 99
    bundle_git_repo = 'https://gitlab.com/bundle/repo'

    # Use tmpdir with the mock
    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)

    # Mock worker config
    mock_gwc.return_value = {
        'iib_index_image_output_registry': None,
        'iib_registry': 'quay.io',
    }

    # Mock image resolution and metadata
    mock_gri.return_value = from_bundle_image_resolved
    mock_gia.return_value = list(arches)
    mock_gil.return_value = {'com.redhat.iib.pinned': 'false'}

    # Mock Git token
    mock_ggt.return_value = ('GITLAB_TOKEN', 'test-token')

    # Mock bundle adjustments
    mock_aob.return_value = {'operators.operatorframework.io.bundle.package.v1': 'test-package'}
    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'test-package'}
    }

    # Mock Git operations to fail
    mock_gccmop.side_effect = RuntimeError('Git operation failed')

    # Execute and expect failure
    with pytest.raises(IIBError, match='Failed to regenerate bundle'):
        build_containerized_regenerate_bundle.handle_containerized_regenerate_bundle_request(
            from_bundle_image=from_bundle_image,
            organization=organization,
            request_id=request_id,
            bundle_replacements={},
            index_to_gitlab_push_map={'regenerate-bundle': bundle_git_repo},
            regenerate_bundle_repo_key='regenerate-bundle',
        )

    # Verify cleanup was called
    mock_cof.assert_called_once()


@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_arches')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.update_request')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_worker_config')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.tempfile.TemporaryDirectory')
def test_handle_containerized_regenerate_bundle_request_no_arches(
    mock_temp_dir,
    mock_gwc,
    mock_ur,
    mock_srs,
    mock_gri,
    mock_gia,
    tmpdir,
):
    """Test that missing arches raises an error."""
    # Setup
    from_bundle_image = 'bundle-image:latest'
    from_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    organization = 'acme'
    request_id = 99

    # Use tmpdir with the mock
    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)

    # Mock worker config
    mock_gwc.return_value = {
        'iib_index_image_output_registry': None,
        'iib_registry': 'quay.io',
    }

    # Mock image resolution to return no arches
    mock_gri.return_value = from_bundle_image_resolved
    mock_gia.return_value = []

    # Execute and expect failure
    expected = (
        f'No arches were found in the resolved from_bundle_image {from_bundle_image_resolved}'
    )
    with pytest.raises(IIBError, match=expected):
        build_containerized_regenerate_bundle.handle_containerized_regenerate_bundle_request(
            from_bundle_image=from_bundle_image,
            organization=organization,
            request_id=request_id,
            bundle_replacements={},
            index_to_gitlab_push_map={'regenerate-bundle': 'https://gitlab.com/bundle/repo'},
            regenerate_bundle_repo_key='regenerate-bundle',
        )


@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_labels')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_arches')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.update_request')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_worker_config')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.tempfile.TemporaryDirectory')
def test_handle_containerized_regenerate_bundle_request_no_repo_key(
    mock_temp_dir,
    mock_gwc,
    mock_ur,
    mock_srs,
    mock_gri,
    mock_gia,
    mock_gil,
    tmpdir,
):
    """Test that missing repository key raises an error."""
    # Setup
    from_bundle_image = 'bundle-image:latest'
    from_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    organization = 'acme'
    request_id = 99

    # Use tmpdir with the mock
    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)

    # Mock worker config
    mock_gwc.return_value = {
        'iib_index_image_output_registry': None,
        'iib_registry': 'quay.io',
    }

    # Mock image resolution
    mock_gri.return_value = from_bundle_image_resolved
    mock_gia.return_value = ['amd64']
    mock_gil.return_value = {'com.redhat.iib.pinned': 'false'}

    # Execute and expect failure - using default 'regenerate-bundle' key but map has different key
    expected = 'Repository not found for key: regenerate-bundle'
    with pytest.raises(IIBError, match=expected):
        build_containerized_regenerate_bundle.handle_containerized_regenerate_bundle_request(
            from_bundle_image=from_bundle_image,
            organization=organization,
            request_id=request_id,
            bundle_replacements={},
            index_to_gitlab_push_map={'different_key': 'https://gitlab.com/bundle/repo'},
            # Not passing regenerate_bundle_repo_key, so it uses default 'regenerate-bundle'
        )


@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.cleanup_merge_request_if_exists'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.replicate_image_to_tagged_destinations'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.monitor_pipeline_and_extract_image'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.git_commit_and_create_mr_or_push'
)
@mock.patch(
    'iib.workers.tasks.build_containerized_regenerate_bundle.'
    'extract_files_from_image_non_privileged'
)
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.clone_git_repo')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_git_token')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_labels')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_image_arches')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.update_request')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.get_worker_config')
@mock.patch('iib.workers.tasks.build_containerized_regenerate_bundle.tempfile.TemporaryDirectory')
def test_handle_containerized_regenerate_bundle_request_with_output_registry(
    mock_temp_dir,
    mock_gwc,
    mock_ur,
    mock_srs,
    mock_gri,
    mock_gia,
    mock_gil,
    mock_aob,
    mock_gpa,
    mock_ggt,
    mock_cgr,
    mock_effinp,
    mock_gccmop,
    mock_mpaei,
    mock_ritd,
    mock_cmrie,
    tmpdir,
):
    """Test output registry replacement when iib_index_image_output_registry is configured."""
    # Setup
    arches = ['amd64']
    from_bundle_image = 'bundle-image:latest'
    from_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    original_bundle_image = 'quay.io/iib:99'
    replaced_bundle_image = 'registry.example.com/iib:99'
    organization = 'acme'
    request_id = 100
    bundle_git_repo = 'https://gitlab.com/bundle/repo'

    # Use tmpdir with the mock
    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)

    # Mock worker config WITH output registry replacement
    mock_gwc.return_value = {
        'iib_index_image_output_registry': 'registry.example.com',
        'iib_registry': 'quay.io',
    }

    # Mock image resolution and metadata
    mock_gri.return_value = from_bundle_image_resolved
    mock_gia.return_value = list(arches)
    mock_gil.return_value = {'com.redhat.iib.pinned': 'false'}

    # Mock Git token
    mock_ggt.return_value = ('GITLAB_TOKEN', 'test-token')

    # Mock bundle adjustments
    mock_aob.return_value = {'operators.operatorframework.io.bundle.package.v1': 'test-package'}
    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'test-package'}
    }

    # Mock Git operations
    mock_gccmop.return_value = (
        {'mr_id': '123', 'mr_url': 'https://gitlab.com/merge_requests/123'},
        'commit-sha-123',
    )

    # Mock pipeline monitoring
    mock_mpaei.return_value = 'quay.io/konflux/bundle:sha256-abc123.att'

    # Mock image replication - returns original registry
    mock_ritd.return_value = [original_bundle_image]

    # Execute
    build_containerized_regenerate_bundle.handle_containerized_regenerate_bundle_request(
        from_bundle_image=from_bundle_image,
        organization=organization,
        request_id=request_id,
        bundle_replacements={},
        index_to_gitlab_push_map={'regenerate-bundle': bundle_git_repo},
        regenerate_bundle_repo_key='regenerate-bundle',
    )

    # Verify the final update_request call used the REPLACED bundle_image
    final_update_call = mock_ur.call_args_list[-1]
    final_payload = final_update_call[0][1]
    assert final_payload['bundle_image'] == replaced_bundle_image
    assert final_payload['state'] == 'complete'
