# SPDX-License-Identifier: GPL-3.0-or-later
import json
from unittest.mock import patch

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks.containerized_utils import (
    pull_index_db_artifact,
    write_build_metadata,
    cleanup_on_failure,
    validate_bundles_in_parallel,
    wait_for_bundle_validation_threads,
)


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.refresh_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.verify_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_imagestream_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_imagestream_enabled_cache_synced(
    mock_get_oras_artifact,
    mock_get_imagestream_artifact_pullspec,
    mock_get_indexdb_artifact_pullspec,
    mock_verify_cache,
    mock_refresh_cache,
    mock_log,
    mock_get_worker_config,
):
    """When ImageStream cache enabled and synced, pull from ImageStream."""
    mock_get_worker_config.return_value = {'iib_use_imagestream_cache': True}
    mock_verify_cache.return_value = True

    from_index = 'quay.io/ns/index-image@sha256:abc'
    temp_dir = '/tmp/some-dir'
    imagestream_ref = 'imagestream-ref'
    artifact_dir = '/tmp/artifact-dir'

    mock_get_imagestream_artifact_pullspec.return_value = imagestream_ref
    mock_get_oras_artifact.return_value = artifact_dir

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_verify_cache.assert_called_once_with(from_index)
    mock_refresh_cache.assert_not_called()
    mock_get_imagestream_artifact_pullspec.assert_called_once_with(from_index)
    mock_get_indexdb_artifact_pullspec.assert_not_called()
    mock_get_oras_artifact.assert_called_once_with(imagestream_ref, temp_dir)
    mock_log.info.assert_any_call('ImageStream cache is enabled. Checking cache sync status.')
    mock_log.info.assert_any_call('Index.db cache is synced. Pulling from ImageStream.')


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.refresh_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.verify_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_imagestream_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_imagestream_enabled_cache_not_synced(
    mock_get_oras_artifact,
    mock_get_imagestream_artifact_pullspec,
    mock_get_indexdb_artifact_pullspec,
    mock_verify_cache,
    mock_refresh_cache,
    mock_log,
    mock_get_worker_config,
):
    """When ImageStream cache enabled but not synced, refresh and pull from registry."""
    mock_get_worker_config.return_value = {'iib_use_imagestream_cache': True}
    mock_verify_cache.return_value = False

    from_index = 'quay.io/ns/index-image@sha256:def'
    temp_dir = '/tmp/some-dir'
    artifact_ref = 'quay.io/ns/index-image-indexdb:v4.19'
    artifact_dir = '/tmp/artifact-dir'

    mock_get_indexdb_artifact_pullspec.return_value = artifact_ref
    mock_get_oras_artifact.return_value = artifact_dir

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_verify_cache.assert_called_once_with(from_index)
    mock_refresh_cache.assert_called_once_with(from_index)
    mock_get_imagestream_artifact_pullspec.assert_not_called()
    mock_get_indexdb_artifact_pullspec.assert_called_once_with(from_index)
    mock_get_oras_artifact.assert_called_once_with(artifact_ref, temp_dir)
    mock_log.info.assert_any_call('ImageStream cache is enabled. Checking cache sync status.')
    mock_log.info.assert_any_call('Index.db cache is not synced. Refreshing and pulling from Quay.')


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.refresh_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.verify_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_imagestream_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_imagestream_disabled(
    mock_get_oras_artifact,
    mock_get_imagestream_artifact_pullspec,
    mock_get_indexdb_artifact_pullspec,
    mock_verify_cache,
    mock_refresh_cache,
    mock_log,
    mock_get_worker_config,
):
    """When ImageStream cache disabled, pull directly from registry."""
    mock_get_worker_config.return_value = {'iib_use_imagestream_cache': False}

    from_index = 'quay.io/ns/index-image@sha256:ghi'
    temp_dir = '/tmp/some-dir'
    artifact_ref = 'quay.io/ns/index-image-indexdb:v4.20'
    artifact_dir = '/tmp/artifact-dir'

    mock_get_indexdb_artifact_pullspec.return_value = artifact_ref
    mock_get_oras_artifact.return_value = artifact_dir

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_verify_cache.assert_not_called()
    mock_refresh_cache.assert_not_called()
    mock_get_imagestream_artifact_pullspec.assert_not_called()
    mock_get_indexdb_artifact_pullspec.assert_called_once_with(from_index)
    mock_get_oras_artifact.assert_called_once_with(artifact_ref, temp_dir)
    mock_log.info.assert_any_call(
        'ImageStream cache is disabled. Pulling index.db artifact directly from registry.'
    )


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_default_config_behaves_as_disabled(
    mock_get_oras_artifact,
    mock_get_indexdb_artifact_pullspec,
    mock_get_worker_config,
):
    """If configuration lacks the key, default is to treat ImageStream as disabled."""
    mock_get_worker_config.return_value = {}
    from_index = 'quay.io/ns/index@sha256:jkl'
    temp_dir = '/tmp/some-dir'
    artifact_ref = 'artifact-ref'
    artifact_dir = '/tmp/artifact-dir'

    mock_get_indexdb_artifact_pullspec.return_value = artifact_ref
    mock_get_oras_artifact.return_value = artifact_dir

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_get_indexdb_artifact_pullspec.assert_called_once_with(from_index)
    mock_get_oras_artifact.assert_called_once_with(artifact_ref, temp_dir)


@patch('iib.workers.tasks.containerized_utils.log')
def test_write_build_metadata_creates_expected_json(mock_log, tmp_path):
    """write_build_metadata should create JSON file with expected content."""
    local_repo_path = tmp_path
    opm_version = 'opm-1.40.0'
    ocp_version = 'v4.19'
    distribution_scope = 'PROD'
    binary_image = 'quay.io/ns/binary-image:tag'
    request_id = 12345
    arches = {'amd64', 's390x'}

    write_build_metadata(
        str(local_repo_path),
        opm_version,
        ocp_version,
        distribution_scope,
        binary_image,
        request_id,
        arches,
    )

    metadata_path = local_repo_path / '.iib-build-metadata.json'
    assert metadata_path.exists()

    with open(metadata_path, 'r') as f:
        data = json.load(f)

    assert data == {
        'opm_version': opm_version,
        'labels': {
            'com.redhat.index.delivery.version': ocp_version,
            'com.redhat.index.delivery.distribution_scope': distribution_scope,
        },
        'binary_image': binary_image,
        'request_id': request_id,
        'arches': ['amd64', 's390x'],
    }

    mock_log.info.assert_called_once_with('Written build metadata to %s', str(metadata_path))


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.git_utils.close_mr')
def test_cleanup_on_failure_closes_mr_when_mr_details_and_repo_present(mock_close_mr, mock_log):
    """If MR details and index_git_repo are provided, close_mr should be called."""
    mr_details = {'mr_url': 'https://git.example.com/mr/1'}
    last_commit_sha = 'abc123'
    index_git_repo = 'https://git.example.com/repo.git'
    overwrite_from_index = False
    request_id = 1
    from_index = 'quay.io/ns/index:v4.19'
    index_repo_map = {'quay.io/ns/index:v4.19': 'https://git.example.com/repo.git'}

    cleanup_on_failure(
        mr_details=mr_details,
        last_commit_sha=last_commit_sha,
        index_git_repo=index_git_repo,
        overwrite_from_index=overwrite_from_index,
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
    )

    mock_close_mr.assert_called_once_with(mr_details, index_git_repo)
    mock_log.info.assert_any_call("Closing merge request due to %s", "error")
    mock_log.info.assert_any_call("Closed merge request: %s", mr_details.get('mr_url'))


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.git_utils.close_mr')
def test_cleanup_on_failure_close_mr_failure_is_logged(mock_close_mr, mock_log):
    """If closing MR fails, error should be logged but function should not raise."""
    mock_close_mr.side_effect = RuntimeError("close failed")

    mr_details = {'mr_url': 'https://git.example.com/mr/2'}
    last_commit_sha = 'abc123'
    index_git_repo = 'https://git.example.com/repo.git'
    overwrite_from_index = False
    request_id = 1
    from_index = 'quay.io/ns/index:v4.19'
    index_repo_map = {}

    cleanup_on_failure(
        mr_details=mr_details,
        last_commit_sha=last_commit_sha,
        index_git_repo=index_git_repo,
        overwrite_from_index=overwrite_from_index,
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
    )

    mock_close_mr.assert_called_once_with(mr_details, index_git_repo)
    mock_log.warning.assert_called_once()
    assert "Failed to close merge request" in mock_log.warning.call_args[0][0]


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.git_utils.revert_last_commit')
def test_cleanup_on_failure_reverts_commit_when_overwrite_and_commit_sha_present(
    mock_revert_last_commit, mock_log
):
    """If overwrite_from_index is True and last_commit_sha present, revert_last_commit is used."""
    mr_details = None
    last_commit_sha = 'abc123'
    index_git_repo = None
    overwrite_from_index = True
    request_id = 42
    from_index = 'quay.io/ns/index:v4.19'
    index_repo_map = {'quay.io/ns/index:v4.19': 'https://git.example.com/repo.git'}

    cleanup_on_failure(
        mr_details=mr_details,
        last_commit_sha=last_commit_sha,
        index_git_repo=index_git_repo,
        overwrite_from_index=overwrite_from_index,
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
    )

    mock_log.error.assert_any_call("Reverting commit due to %s", "error")
    mock_revert_last_commit.assert_called_once_with(
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
    )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.git_utils.revert_last_commit')
def test_cleanup_on_failure_revert_failure_is_logged(mock_revert_last_commit, mock_log):
    """If revert_last_commit fails, error should be logged."""
    mock_revert_last_commit.side_effect = RuntimeError("revert failed")

    mr_details = None
    last_commit_sha = 'abc123'
    index_git_repo = None
    overwrite_from_index = True
    request_id = 42
    from_index = 'quay.io/ns/index:v4.19'
    index_repo_map = {}

    cleanup_on_failure(
        mr_details=mr_details,
        last_commit_sha=last_commit_sha,
        index_git_repo=index_git_repo,
        overwrite_from_index=overwrite_from_index,
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
    )

    mock_revert_last_commit.assert_called_once()
    mock_log.error.assert_any_call(
        "Failed to revert commit: %s", mock_revert_last_commit.side_effect
    )


@patch('iib.workers.tasks.containerized_utils.log')
def test_cleanup_on_failure_no_mr_no_commit(mock_log):
    """If there is neither MR nor commit to revert, log that no cleanup is needed."""
    mr_details = None
    last_commit_sha = None
    index_git_repo = None
    overwrite_from_index = False
    request_id = 1
    from_index = 'quay.io/ns/index:v4.19'
    index_repo_map = {}

    cleanup_on_failure(
        mr_details=mr_details,
        last_commit_sha=last_commit_sha,
        index_git_repo=index_git_repo,
        overwrite_from_index=overwrite_from_index,
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
    )

    mock_log.error.assert_any_call(
        "Neither MR nor commit to revert. No cleanup needed for %s", "error"
    )


@patch('iib.workers.tasks.containerized_utils.run_cmd')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.log')
def test_cleanup_on_failure_restores_index_db_artifact(
    mock_log, mock_get_indexdb_artifact_pullspec, mock_run_cmd
):
    """If original_index_db_digest is provided, oras copy should be invoked correctly."""
    mr_details = None
    last_commit_sha = None
    index_git_repo = None
    overwrite_from_index = False
    request_id = 1
    from_index = 'quay.io/ns/index:v4.19'
    index_repo_map = {}
    original_digest = 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'

    v4x_artifact_ref = 'quay.io/ns/index-indexdb:v4.19'
    mock_get_indexdb_artifact_pullspec.return_value = v4x_artifact_ref

    cleanup_on_failure(
        mr_details=mr_details,
        last_commit_sha=last_commit_sha,
        index_git_repo=index_git_repo,
        overwrite_from_index=overwrite_from_index,
        request_id=request_id,
        from_index=from_index,
        index_repo_map=index_repo_map,
        original_index_db_digest=original_digest,
    )

    mock_log.info.assert_any_call(
        "Restoring index.db artifact to original digest due to %s", "error"
    )

    artifact_name = v4x_artifact_ref.rsplit(':', 1)[0]
    expected_source_ref = f'{artifact_name}@{original_digest}'

    mock_run_cmd.assert_called_once_with(
        ['oras', 'copy', expected_source_ref, v4x_artifact_ref],
        exc_msg=(
            f'Failed to restore index.db artifact from {expected_source_ref} '
            f'to {v4x_artifact_ref}'
        ),
    )
    mock_log.info.assert_any_call("Successfully restored index.db artifact to original digest")


@patch('iib.workers.tasks.containerized_utils.run_cmd')
@patch('iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.log')
def test_cleanup_on_failure_restore_failure_is_logged(
    mock_log, mock_get_indexdb_artifact_pullspec, mock_run_cmd
):
    """If restoring the artifact fails, error should be logged."""
    mock_get_indexdb_artifact_pullspec.return_value = 'quay.io/ns/index-indexdb:v4.19'
    mock_run_cmd.side_effect = RuntimeError("oras copy failed")

    cleanup_on_failure(
        mr_details=None,
        last_commit_sha=None,
        index_git_repo=None,
        overwrite_from_index=False,
        request_id=1,
        from_index='quay.io/ns/index:v4.19',
        index_repo_map={},
        original_index_db_digest='sha256:0123456789abcdef0123456789abcdef0123456789abcde',
    )

    mock_run_cmd.assert_called_once()
    mock_log.error.assert_any_call(
        "Failed to restore index.db artifact: %s", mock_run_cmd.side_effect
    )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.utils.run_cmd')
def test_cleanup_on_failure_no_restore_when_no_original_digest(
    mock_run_cmd, mock_get_indexdb_artifact_pullspec, mock_log
):
    """If original_index_db_digest is not provided, restoration must not be attempted."""
    cleanup_on_failure(
        mr_details=None,
        last_commit_sha=None,
        index_git_repo=None,
        overwrite_from_index=False,
        request_id=1,
        from_index='quay.io/ns/index:v4.19',
        index_repo_map={},
        original_index_db_digest=None,
    )

    mock_get_indexdb_artifact_pullspec.assert_not_called()
    mock_run_cmd.assert_not_called()


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_success_single_bundle(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with a single bundle successfully."""
    bundles = [{"bundlePath": 'quay.io/ns/bundle1:v1.0.0'}]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=1, wait=True)

    assert result is None
    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_success_multiple_bundles(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with multiple bundles successfully."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
        {"bundlePath": 'quay.io/ns/bundle3:v3.0.0'},
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=3, wait=True)

    assert result is None
    assert mock_skopeo_inspect.call_count == 3

    # Check that all bundles were validated (order may vary due to threading)
    actual_calls = [call[0] for call in mock_skopeo_inspect.call_args_list]
    assert len(actual_calls) == 3
    assert all('docker://quay.io/ns/bundle' in str(call[0]) for call in actual_calls)


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_empty_bundles(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with empty bundle list."""
    bundles = []

    result = validate_bundles_in_parallel(bundles, threads=5, wait=True)

    assert result is None
    mock_skopeo_inspect.assert_not_called()


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_custom_thread_count(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with custom thread count."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert result is None
    assert mock_skopeo_inspect.call_count == 2


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_wait_false_returns_threads(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with wait=False returns thread list."""
    bundles = [{"bundlePath": 'quay.io/ns/bundle1:v1.0.0'}]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=1, wait=False)

    assert result is not None
    assert len(result) == 1
    assert hasattr(result[0], 'join')
    # Wait for thread to complete to verify it worked
    result[0].join()
    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_failure_raises_error(mock_skopeo_inspect, mock_log):
    """Test validate_bundles_in_parallel raises IIBError when bundle validation fails."""
    bundles = [{"bundlePath": 'quay.io/ns/bundle1:v1.0.0'}]
    error = IIBError('Bundle not found')
    mock_skopeo_inspect.side_effect = error

    with pytest.raises(IIBError, match='Error validating bundle'):
        validate_bundles_in_parallel(bundles, threads=1, wait=True)

    assert mock_skopeo_inspect.called
    # Error should be logged in the thread
    assert mock_log.error.called


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_more_bundles_than_threads(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with more bundles than threads."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
        {"bundlePath": 'quay.io/ns/bundle3:v3.0.0'},
        {"bundlePath": 'quay.io/ns/bundle4:v4.0.0'},
        {"bundlePath": 'quay.io/ns/bundle5:v5.0.0'},
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert result is None
    assert mock_skopeo_inspect.call_count == 5


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_default_parameters(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with default parameters."""
    bundles = [{"bundlePath": 'quay.io/ns/bundle1:v1.0.0'}]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles)

    assert result is None
    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_multiple_threads_processing_queue(mock_skopeo_inspect):
    """Test that multiple threads properly process bundles from the queue."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert result is None
    # Both bundles should be validated
    assert mock_skopeo_inspect.call_count == 2
    # Verify all bundles were processed
    call_args = [call[0][0] for call in mock_skopeo_inspect.call_args_list]
    assert 'docker://quay.io/ns/bundle1:v1.0.0' in call_args
    assert 'docker://quay.io/ns/bundle2:v2.0.0' in call_args


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_one_bundle_fails_others_succeed(
    mock_skopeo_inspect, mock_log
):
    """Test that when one bundle fails, the error is logged and raised."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
    ]
    # First bundle succeeds, second fails
    mock_skopeo_inspect.side_effect = [None, IIBError('Bundle not found')]

    with pytest.raises(IIBError, match='Error validating bundle'):
        validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert mock_skopeo_inspect.call_count >= 1
    # Error should be logged in the thread
    assert mock_log.error.called


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_success(mock_skopeo_inspect):
    """Test wait_for_bundle_validation_threads with successful validation."""
    from iib.workers.tasks.containerized_utils import ValidateBundlesThread
    import queue

    bundles_queue = queue.Queue()
    bundles_queue.put({"bundlePath": 'quay.io/ns/bundle1:v1.0.0'})
    mock_skopeo_inspect.return_value = None

    thread = ValidateBundlesThread(bundles_queue)
    thread.start()

    # Wait for the thread using the function
    wait_for_bundle_validation_threads([thread])

    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )
    assert thread.exception is None


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_failure_raises_error(mock_skopeo_inspect, mock_log):
    """Test wait_for_bundle_validation_threads raises IIBError when validation fails."""
    from iib.workers.tasks.containerized_utils import ValidateBundlesThread
    import queue

    bundles_queue = queue.Queue()
    bundles_queue.put({"bundlePath": 'quay.io/ns/bundle1:v1.0.0'})
    error = IIBError('Bundle not found')
    mock_skopeo_inspect.side_effect = error

    thread = ValidateBundlesThread(bundles_queue)
    thread.start()

    with pytest.raises(IIBError, match='Error validating bundle quay.io/ns/bundle1:v1.0.0'):
        wait_for_bundle_validation_threads([thread])

    assert mock_skopeo_inspect.called
    assert thread.exception == error
    assert thread.bundle == {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'}
    mock_log.error.assert_called()


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_multiple_threads_one_fails(
    mock_skopeo_inspect, mock_log
):
    """Test wait_for_bundle_validation_threads with multiple threads where one fails."""
    from iib.workers.tasks.containerized_utils import ValidateBundlesThread
    import queue

    bundles_queue1 = queue.Queue()
    bundles_queue1.put({"bundlePath": 'quay.io/ns/bundle1:v1.0.0'})
    bundles_queue2 = queue.Queue()
    bundles_queue2.put({"bundlePath": 'quay.io/ns/bundle2:v2.0.0'})

    mock_skopeo_inspect.side_effect = [None, IIBError('Bundle not found')]

    thread1 = ValidateBundlesThread(bundles_queue1)
    thread2 = ValidateBundlesThread(bundles_queue2)
    thread1.start()
    thread2.start()

    with pytest.raises(IIBError, match='Error validating bundle quay.io/ns/bundle2:v2.0.0'):
        wait_for_bundle_validation_threads([thread1, thread2])

    assert mock_skopeo_inspect.call_count == 2
    assert thread1.exception is None
    assert thread2.exception is not None
    mock_log.error.assert_called()


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_wait_false_then_wait_manually(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with wait=False and then manually waiting."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
    ]
    mock_skopeo_inspect.return_value = None

    # Get threads without waiting
    threads = validate_bundles_in_parallel(bundles, threads=2, wait=False)

    assert threads is not None
    assert len(threads) == 2

    # Manually wait for threads
    wait_for_bundle_validation_threads(threads)

    # Verify all bundles were validated
    assert mock_skopeo_inspect.call_count == 2
    call_args = [call[0][0] for call in mock_skopeo_inspect.call_args_list]
    assert 'docker://quay.io/ns/bundle1:v1.0.0' in call_args
    assert 'docker://quay.io/ns/bundle2:v2.0.0' in call_args


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_wait_false_then_wait_manually_with_failure(
    mock_skopeo_inspect, mock_log
):
    """Test validate_bundles_in_parallel with wait=False, then manually waiting when one fails."""
    bundles = [
        {"bundlePath": 'quay.io/ns/bundle1:v1.0.0'},
        {"bundlePath": 'quay.io/ns/bundle2:v2.0.0'},
    ]
    mock_skopeo_inspect.side_effect = [None, IIBError('Bundle not found')]

    # Get threads without waiting
    threads = validate_bundles_in_parallel(bundles, threads=2, wait=False)

    assert threads is not None
    assert len(threads) == 2

    # Manually wait for threads - should raise error
    with pytest.raises(IIBError, match='Error validating bundle'):
        wait_for_bundle_validation_threads(threads)

    assert mock_skopeo_inspect.call_count == 2
    mock_log.error.assert_called()


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_empty_list(mock_skopeo_inspect):
    """Test wait_for_bundle_validation_threads with empty thread list."""
    wait_for_bundle_validation_threads([])
    mock_skopeo_inspect.assert_not_called()


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_unknown_bundle_on_error(mock_skopeo_inspect, mock_log):
    """Test wait_for_bundle_validation_threads when bundle is None in error case."""
    from iib.workers.tasks.containerized_utils import ValidateBundlesThread
    import queue

    bundles_queue = queue.Queue()
    # Add a bundle to the queue so the thread will process it
    bundles_queue.put({"bundlePath": 'quay.io/ns/bundle1:v1.0.0'})
    error = IIBError('Bundle not found')
    mock_skopeo_inspect.side_effect = error

    thread = ValidateBundlesThread(bundles_queue)
    thread.start()
    thread.join()

    # Manually set bundle to None after thread completes to test the "unknown" case
    thread.bundle = None

    with pytest.raises(IIBError, match='Error validating bundle unknown'):
        wait_for_bundle_validation_threads([thread])

    assert mock_skopeo_inspect.called
    assert thread.exception == error
    mock_log.error.assert_called()


# Tests for List[str] format (pullspec strings)
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_success_single_bundle_string(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with a single bundle string successfully."""
    bundles = ['quay.io/ns/bundle1:v1.0.0']
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=1, wait=True)

    assert result is None
    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_success_multiple_bundles_string(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with multiple bundle strings successfully."""
    bundles = [
        'quay.io/ns/bundle1:v1.0.0',
        'quay.io/ns/bundle2:v2.0.0',
        'quay.io/ns/bundle3:v3.0.0',
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=3, wait=True)

    assert result is None
    assert mock_skopeo_inspect.call_count == 3

    # Check that all bundles were validated (order may vary due to threading)
    actual_calls = [call[0] for call in mock_skopeo_inspect.call_args_list]
    assert len(actual_calls) == 3
    assert all('docker://quay.io/ns/bundle' in str(call[0]) for call in actual_calls)


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_custom_thread_count_string(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with custom thread count using string bundles."""
    bundles = [
        'quay.io/ns/bundle1:v1.0.0',
        'quay.io/ns/bundle2:v2.0.0',
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert result is None
    assert mock_skopeo_inspect.call_count == 2


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_wait_false_returns_threads_string(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with wait=False returns thread list for string bundles."""
    bundles = ['quay.io/ns/bundle1:v1.0.0']
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=1, wait=False)

    assert result is not None
    assert len(result) == 1
    assert hasattr(result[0], 'join')
    # Wait for thread to complete to verify it worked
    result[0].join()
    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_failure_raises_error_string(mock_skopeo_inspect, mock_log):
    """Test validate_bundles_in_parallel raises IIBError when bundle string validation fails."""
    bundles = ['quay.io/ns/bundle1:v1.0.0']
    error = IIBError('Bundle not found')
    mock_skopeo_inspect.side_effect = error

    with pytest.raises(IIBError, match='Error validating bundle'):
        validate_bundles_in_parallel(bundles, threads=1, wait=True)

    assert mock_skopeo_inspect.called
    # Error should be logged in the thread
    assert mock_log.error.called


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_more_bundles_than_threads_string(mock_skopeo_inspect):
    """Test validate_bundles_in_parallel with more bundle strings than threads."""
    bundles = [
        'quay.io/ns/bundle1:v1.0.0',
        'quay.io/ns/bundle2:v2.0.0',
        'quay.io/ns/bundle3:v3.0.0',
        'quay.io/ns/bundle4:v4.0.0',
        'quay.io/ns/bundle5:v5.0.0',
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert result is None
    assert mock_skopeo_inspect.call_count == 5


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_multiple_threads_processing_queue_string(mock_skopeo_inspect):
    """Test that multiple threads properly process bundle strings from the queue."""
    bundles = [
        'quay.io/ns/bundle1:v1.0.0',
        'quay.io/ns/bundle2:v2.0.0',
    ]
    mock_skopeo_inspect.return_value = None

    result = validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert result is None
    # Both bundles should be validated
    assert mock_skopeo_inspect.call_count == 2
    # Verify all bundles were processed
    call_args = [call[0][0] for call in mock_skopeo_inspect.call_args_list]
    assert 'docker://quay.io/ns/bundle1:v1.0.0' in call_args
    assert 'docker://quay.io/ns/bundle2:v2.0.0' in call_args


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_validate_bundles_in_parallel_one_bundle_fails_others_succeed_string(
    mock_skopeo_inspect, mock_log
):
    """Test that when one bundle string fails, the error is logged and raised."""
    bundles = [
        'quay.io/ns/bundle1:v1.0.0',
        'quay.io/ns/bundle2:v2.0.0',
    ]
    # First bundle succeeds, second fails
    mock_skopeo_inspect.side_effect = [None, IIBError('Bundle not found')]

    with pytest.raises(IIBError, match='Error validating bundle'):
        validate_bundles_in_parallel(bundles, threads=2, wait=True)

    assert mock_skopeo_inspect.call_count >= 1
    # Error should be logged in the thread
    assert mock_log.error.called


@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_success_string(mock_skopeo_inspect):
    """Test wait_for_bundle_validation_threads with successful validation for string bundles."""
    from iib.workers.tasks.containerized_utils import ValidateBundlesThread
    import queue

    bundles_queue = queue.Queue()
    bundles_queue.put('quay.io/ns/bundle1:v1.0.0')
    mock_skopeo_inspect.return_value = None

    thread = ValidateBundlesThread(bundles_queue)
    thread.start()

    # Wait for the thread using the function
    wait_for_bundle_validation_threads([thread])

    mock_skopeo_inspect.assert_called_once_with(
        'docker://quay.io/ns/bundle1:v1.0.0', '--raw', return_json=False
    )
    assert thread.exception is None


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.skopeo_inspect')
def test_wait_for_bundle_validation_threads_failure_raises_error_string(
    mock_skopeo_inspect, mock_log
):
    """Ensure it raises IIBError when string bundle validation fails."""
    from iib.workers.tasks.containerized_utils import ValidateBundlesThread
    import queue

    bundles_queue = queue.Queue()
    bundles_queue.put('quay.io/ns/bundle1:v1.0.0')
    error = IIBError('Bundle not found')
    mock_skopeo_inspect.side_effect = error

    thread = ValidateBundlesThread(bundles_queue)
    thread.start()

    with pytest.raises(IIBError, match='Error validating bundle quay.io/ns/bundle1:v1.0.0'):
        wait_for_bundle_validation_threads([thread])

    assert mock_skopeo_inspect.called
    assert thread.exception == error
    assert thread.bundle == 'quay.io/ns/bundle1:v1.0.0'
    mock_log.error.assert_called()
