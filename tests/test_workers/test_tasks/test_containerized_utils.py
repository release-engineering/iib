# SPDX-License-Identifier: GPL-3.0-or-later
import inspect
import json
import os
import tarfile
from unittest import mock
from unittest.mock import patch

import pytest

from iib.exceptions import IIBError, FileNotFoundInImageError
from iib.workers.tasks import containerized_utils as cu
from iib.workers.tasks.containerized_utils import (
    extract_catalog_and_db_from_image,
    extract_files_from_image_non_privileged,
    pull_index_db_artifact,
    push_index_db_artifact,
    write_build_metadata,
    cleanup_on_failure,
    validate_bundles_in_parallel,
    wait_for_bundle_validation_threads,
    git_commit_and_create_mr,
    merge_mr_after_build,
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
    mock_log.info.assert_any_call('Index.db cache is not synced. Refreshing cache.')


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


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.refresh_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.verify_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_imagestream_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_verify_cache_fails_falls_back_to_quay(
    mock_get_oras_artifact,
    mock_get_imagestream_artifact_pullspec,
    mock_get_indexdb_artifact_pullspec,
    mock_verify_cache,
    mock_refresh_cache,
    mock_log,
    mock_get_worker_config,
):
    """When ImageStream verify raises IIBError, fall back to Quay."""
    mock_get_worker_config.return_value = {'iib_use_imagestream_cache': True}
    error = IIBError('imagestreams.image.openshift.io not found')
    mock_verify_cache.side_effect = error

    from_index = 'quay.io/ns/index-image@sha256:abc'
    temp_dir = '/tmp/some-dir'
    artifact_ref = 'quay.io/ns/index-image-indexdb:v4.19'
    artifact_dir = '/tmp/artifact-dir'

    mock_get_indexdb_artifact_pullspec.return_value = artifact_ref
    mock_get_oras_artifact.return_value = artifact_dir

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_verify_cache.assert_called_once_with(from_index)
    mock_get_indexdb_artifact_pullspec.assert_called_once_with(from_index)
    mock_get_oras_artifact.assert_called_once_with(artifact_ref, temp_dir)
    mock_log.warning.assert_called_once_with(
        'ImageStream cache access failed, falling back to Quay: %s', error
    )


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.refresh_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.verify_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_imagestream_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_imagestream_pull_fails_falls_back_to_quay(
    mock_get_oras_artifact,
    mock_get_imagestream_artifact_pullspec,
    mock_get_indexdb_artifact_pullspec,
    mock_verify_cache,
    mock_refresh_cache,
    mock_log,
    mock_get_worker_config,
):
    """When ImageStream pull raises IIBError, fall back to Quay."""
    mock_get_worker_config.return_value = {'iib_use_imagestream_cache': True}
    mock_verify_cache.return_value = True
    mock_get_imagestream_artifact_pullspec.return_value = 'imagestream-ref'
    error = IIBError('Failed to pull from ImageStream registry')
    artifact_ref = 'quay.io/ns/index-image-indexdb:v4.19'
    artifact_dir = '/tmp/artifact-dir'
    mock_get_oras_artifact.side_effect = [error, artifact_dir]
    mock_get_indexdb_artifact_pullspec.return_value = artifact_ref

    from_index = 'quay.io/ns/index-image@sha256:abc'
    temp_dir = '/tmp/some-dir'

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_get_oras_artifact.assert_any_call('imagestream-ref', temp_dir)
    mock_get_oras_artifact.assert_any_call(artifact_ref, temp_dir)
    mock_log.warning.assert_called_once_with(
        'ImageStream cache access failed, falling back to Quay: %s', error
    )


@patch('iib.workers.tasks.containerized_utils.get_worker_config')
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils.refresh_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.verify_indexdb_cache_for_image')
@patch('iib.workers.tasks.containerized_utils.get_indexdb_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_imagestream_artifact_pullspec')
@patch('iib.workers.tasks.containerized_utils.get_oras_artifact')
def test_pull_index_db_artifact_refresh_cache_fails_falls_back_to_quay(
    mock_get_oras_artifact,
    mock_get_imagestream_artifact_pullspec,
    mock_get_indexdb_artifact_pullspec,
    mock_verify_cache,
    mock_refresh_cache,
    mock_log,
    mock_get_worker_config,
):
    """When cache refresh raises IIBError, fall back to Quay."""
    mock_get_worker_config.return_value = {'iib_use_imagestream_cache': True}
    mock_verify_cache.return_value = False
    error = IIBError('oc import-image failed')
    mock_refresh_cache.side_effect = error

    from_index = 'quay.io/ns/index-image@sha256:abc'
    temp_dir = '/tmp/some-dir'
    artifact_ref = 'quay.io/ns/index-image-indexdb:v4.19'
    artifact_dir = '/tmp/artifact-dir'

    mock_get_indexdb_artifact_pullspec.return_value = artifact_ref
    mock_get_oras_artifact.return_value = artifact_dir

    result = pull_index_db_artifact(from_index, temp_dir)

    assert result == artifact_dir
    mock_refresh_cache.assert_called_once_with(from_index)
    mock_get_indexdb_artifact_pullspec.assert_called_once_with(from_index)
    mock_get_oras_artifact.assert_called_once_with(artifact_ref, temp_dir)
    mock_log.warning.assert_called_once_with(
        'ImageStream cache access failed, falling back to Quay: %s', error
    )


@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils._get_index_digest')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('pathlib.Path.exists', return_value=True)
def test_push_keys_current_artifact_on_output_digest(
    m_exists, m_state, m_gwc, m_digest, m_push, tmp_path
):
    m_gwc.return_value = {
        'iib_index_db_artifact_registry': 'quay.io/iib',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }
    # digest resolved from the OUTPUT image, not from_index
    m_digest.return_value = 'f' * 64
    db = tmp_path / 'index.db'
    db.write_text('x')
    result = push_index_db_artifact(
        request_id=42,
        from_index='quay.io/ns/foo:v4.17',
        index_db_path=str(db),
        operators=['op1'],
        output_image='quay.io/ns/foo@sha256:' + 'f' * 64,
        overwrite_from_index=True,
        request_type='add',
    )
    assert result is None
    m_digest.assert_called_with('quay.io/ns/foo@sha256:' + 'f' * 64)
    pushed_refs = {c.kwargs['artifact_ref'] for c in m_push.call_args_list}
    assert 'quay.io/iib/index-db:idb-' + 'f' * 64 in pushed_refs          # warm-push (overwrite)
    assert 'quay.io/iib/index-db:idb-' + 'f' * 64 + '-42' in pushed_refs   # per-request tag


@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils._get_index_digest')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('pathlib.Path.exists', return_value=True)
def test_push_throwaway_skips_current_artifact(
    m_exists, m_state, m_gwc, m_digest, m_push, tmp_path
):
    m_gwc.return_value = {
        'iib_index_db_artifact_registry': 'quay.io/iib',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }
    m_digest.return_value = 'a' * 64
    db = tmp_path / 'index.db'
    db.write_text('x')
    push_index_db_artifact(
        request_id=7,
        from_index='quay.io/ns/foo:v4.17',
        index_db_path=str(db),
        operators=[],
        output_image='quay.io/ns/foo@sha256:' + 'a' * 64,
        overwrite_from_index=False,
        request_type='add',
    )
    pushed_refs = {c.kwargs['artifact_ref'] for c in m_push.call_args_list}
    assert pushed_refs == {'quay.io/iib/index-db:idb-' + 'a' * 64 + '-7'}   # only per-request tag


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
@patch('iib.workers.tasks.containerized_utils.close_mr')
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
@patch('iib.workers.tasks.containerized_utils.close_mr')
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
@patch('iib.workers.tasks.containerized_utils.revert_last_commit')
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
@patch('iib.workers.tasks.containerized_utils.revert_last_commit')
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


def test_cleanup_on_failure_has_no_rollback_param():
    """Content keys are immutable: cleanup_on_failure no longer restores artifacts."""
    params = inspect.signature(cleanup_on_failure).parameters
    assert 'original_index_db_digest' not in params


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


# Tests for extract_files_from_image_non_privileged
@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_success_directory(
    mock_skopeo_copy, mock_log, tmpdir
):
    """Test successful extraction of a directory from container image."""
    import os
    import tarfile

    # Setup destination directory
    dest_dir = tmpdir.join('dest')

    # Mock skopeo_copy to create proper OCI layout
    def mock_copy(source, destination, copy_all, exc_msg):
        # Extract OCI directory path from destination (format: oci:/path/to/oci)
        oci_path = destination.replace('oci:', '')

        # Create OCI layout structure
        os.makedirs(oci_path, exist_ok=True)
        blobs_dir = os.path.join(oci_path, 'blobs', 'sha256')
        os.makedirs(blobs_dir, exist_ok=True)

        # Create index.json
        index_json = {
            'manifests': [
                {
                    'digest': 'sha256:abc123',
                    'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                }
            ]
        }
        with open(os.path.join(oci_path, 'index.json'), 'w') as f:
            json.dump(index_json, f)

        # Create manifest
        manifest_json = {
            'layers': [
                {
                    'digest': 'sha256:layer1',
                    'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
                }
            ]
        }
        with open(os.path.join(blobs_dir, 'abc123'), 'w') as f:
            json.dump(manifest_json, f)

        # Create layer tar.gz with /manifests directory
        layer_path = os.path.join(blobs_dir, 'layer1')
        with tarfile.open(layer_path, 'w:gz') as tar:
            # Create a temporary test file
            test_file = tmpdir.join('temp_test_manifest.yaml')
            test_file.write('test: data')
            # Add it to the tar with the path we expect in the image
            tar.add(str(test_file), arcname='manifests/test_manifest.yaml')

    mock_skopeo_copy.side_effect = mock_copy

    # Call the function under test
    extract_files_from_image_non_privileged('quay.io/ns/test:v1', '/manifests', str(dest_dir))

    # Verify the extraction succeeded
    assert dest_dir.check(dir=True)
    extracted_file = dest_dir.join('test_manifest.yaml')
    assert extracted_file.check(file=True)
    assert extracted_file.read() == 'test: data'

    # Verify skopeo was called
    mock_skopeo_copy.assert_called_once()
    call_args = mock_skopeo_copy.call_args
    assert call_args[1]['source'] == 'docker://quay.io/ns/test:v1'
    assert 'oci:' in call_args[1]['destination']
    assert call_args[1]['copy_all'] is False


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_missing_index(mock_skopeo_copy, mock_log, tmpdir):
    """Test extraction fails when OCI index.json is missing."""
    # Mock skopeo_copy to create OCI dir without index.json
    def mock_copy(source, destination, copy_all, exc_msg):
        # Extract OCI directory path from destination
        oci_path = destination.replace('oci:', '')
        os.makedirs(oci_path, exist_ok=True)
        # Don't create index.json to simulate error

    mock_skopeo_copy.side_effect = mock_copy

    with pytest.raises(IIBError, match='OCI index.json not found'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_no_manifests(mock_skopeo_copy, mock_log, tmpdir):
    """Test extraction fails when no manifests in OCI index."""

    def mock_copy(source, destination, copy_all, exc_msg):
        oci_path = destination.replace('oci:', '')
        os.makedirs(oci_path, exist_ok=True)
        # Create index.json with empty manifests
        index_json = {'manifests': []}
        with open(os.path.join(oci_path, 'index.json'), 'w') as f:
            json.dump(index_json, f)

    mock_skopeo_copy.side_effect = mock_copy

    with pytest.raises(IIBError, match='No manifests found in OCI index'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_no_layers(mock_skopeo_copy, mock_log, tmpdir):
    """Test extraction fails when no layers in manifest."""

    def mock_copy(source, destination, copy_all, exc_msg):
        oci_path = destination.replace('oci:', '')
        blobs_dir = os.path.join(oci_path, 'blobs', 'sha256')
        os.makedirs(blobs_dir, exist_ok=True)

        # Create index.json
        index_json = {'manifests': [{'digest': 'sha256:abc123'}]}
        with open(os.path.join(oci_path, 'index.json'), 'w') as f:
            json.dump(index_json, f)

        # Create manifest with no layers
        manifest_json = {'layers': []}
        with open(os.path.join(blobs_dir, 'abc123'), 'w') as f:
            json.dump(manifest_json, f)

    mock_skopeo_copy.side_effect = mock_copy

    with pytest.raises(IIBError, match='No layers found in manifest'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_missing_layer_blob(
    mock_skopeo_copy, mock_log, tmpdir
):
    """Test extraction fails when layer blob file is missing."""

    def mock_copy(source, destination, copy_all, exc_msg):
        oci_path = destination.replace('oci:', '')
        blobs_dir = os.path.join(oci_path, 'blobs', 'sha256')
        os.makedirs(blobs_dir, exist_ok=True)

        # Create index.json
        index_json = {'manifests': [{'digest': 'sha256:abc123'}]}
        with open(os.path.join(oci_path, 'index.json'), 'w') as f:
            json.dump(index_json, f)

        # Create manifest with layer reference
        manifest_json = {'layers': [{'digest': 'sha256:missing_layer'}]}
        with open(os.path.join(blobs_dir, 'abc123'), 'w') as f:
            json.dump(manifest_json, f)
        # Don't create the layer blob file

    mock_skopeo_copy.side_effect = mock_copy

    with pytest.raises(IIBError, match='Layer blob not found'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_path_not_found(mock_skopeo_copy, mock_log, tmpdir):
    """Test extraction fails when requested path doesn't exist in image."""

    def mock_copy(source, destination, copy_all, exc_msg):
        oci_path = destination.replace('oci:', '')
        blobs_dir = os.path.join(oci_path, 'blobs', 'sha256')
        os.makedirs(blobs_dir, exist_ok=True)

        # Create index.json
        index_json = {'manifests': [{'digest': 'sha256:abc123'}]}
        with open(os.path.join(oci_path, 'index.json'), 'w') as f:
            json.dump(index_json, f)

        # Create manifest
        manifest_json = {'layers': [{'digest': 'sha256:layer1'}]}
        with open(os.path.join(blobs_dir, 'abc123'), 'w') as f:
            json.dump(manifest_json, f)

        # Create empty layer tar.gz (no content)
        layer_path = os.path.join(blobs_dir, 'layer1')
        with tarfile.open(layer_path, 'w:gz'):
            pass  # Empty tar

    mock_skopeo_copy.side_effect = mock_copy

    with pytest.raises(IIBError, match='Path /manifests not found in image'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_invalid_layer_tarball(
    mock_skopeo_copy, mock_log, tmpdir
):
    """Test extraction fails when layer tarball is corrupted."""

    def mock_copy(source, destination, copy_all, exc_msg):
        oci_path = destination.replace('oci:', '')
        blobs_dir = os.path.join(oci_path, 'blobs', 'sha256')
        os.makedirs(blobs_dir, exist_ok=True)

        # Create index.json
        index_json = {'manifests': [{'digest': 'sha256:abc123'}]}
        with open(os.path.join(oci_path, 'index.json'), 'w') as f:
            json.dump(index_json, f)

        # Create manifest
        manifest_json = {'layers': [{'digest': 'sha256:corrupted_layer'}]}
        with open(os.path.join(blobs_dir, 'abc123'), 'w') as f:
            json.dump(manifest_json, f)

        # Create corrupted layer (not a valid tar.gz)
        layer_path = os.path.join(blobs_dir, 'corrupted_layer')
        with open(layer_path, 'w') as f:
            f.write('not a valid tar.gz file')

    mock_skopeo_copy.side_effect = mock_copy

    with pytest.raises(IIBError, match='Failed to extract layer'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )


@patch('iib.workers.tasks.containerized_utils.log')
@patch('iib.workers.tasks.containerized_utils._skopeo_copy')
def test_extract_files_from_image_non_privileged_skopeo_copy_failure(
    mock_skopeo_copy, mock_log, tmpdir
):
    """Test extraction fails when skopeo copy fails."""
    mock_skopeo_copy.side_effect = IIBError('Failed to download image')

    with pytest.raises(IIBError, match='Failed to download image'):
        extract_files_from_image_non_privileged(
            'quay.io/ns/test:v1', '/manifests', str(tmpdir.join('dest'))
        )

    mock_skopeo_copy.assert_called_once()
    # Verify the call was made with correct parameters
    call_args = mock_skopeo_copy.call_args
    assert call_args[1]['source'] == 'docker://quay.io/ns/test:v1'
    assert 'oci:' in call_args[1]['destination']
    assert call_args[1]['copy_all'] is False


@patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@patch('iib.workers.tasks.containerized_utils.create_mr')
@patch('iib.workers.tasks.containerized_utils.set_request_state')
@patch('iib.workers.tasks.containerized_utils.get_worker_config')
def test_git_commit_always_creates_mr_for_overwrite(
    mock_config, mock_set_state, mock_create_mr, mock_get_sha
):
    """Test that git_commit_and_create_mr always creates MR."""
    mock_config.return_value = type(
        'obj', (object,), {'get': lambda self, key: 'qe' if key == 'iib_environment_name' else None}
    )()
    mock_create_mr.return_value = {
        'mr_id': '123',
        'mr_url': 'https://gitlab.example.com/merge_requests/123',
        'source_branch': 'iib-qe-request-1-v4.14',
    }
    mock_get_sha.return_value = 'abc123'

    mr_details, sha = git_commit_and_create_mr(
        request_id=1,
        local_git_repo_path='/tmp/repo',
        index_git_repo='https://gitlab.example.com/project',
        branch='v4.14',
        commit_message='test',
    )

    assert mr_details is not None
    assert mr_details['mr_id'] == '123'
    mock_create_mr.assert_called_once_with(
        request_id=1,
        local_repo_path='/tmp/repo',
        repo_url='https://gitlab.example.com/project',
        branch='v4.14',
        commit_message='test',
        environment_name='qe',
    )


@patch('iib.workers.tasks.containerized_utils.close_mr')
@patch('iib.workers.tasks.containerized_utils.merge_mr')
def test_merge_mr_after_build_success(mock_merge_mr, mock_close_mr):
    """Test successful MR merge after build."""
    mock_merge_mr.return_value = 'merge_sha_123'
    mr_details = {'mr_id': '123', 'mr_url': 'https://example.com/mr/123'}

    result = merge_mr_after_build(mr_details, 'https://gitlab.example.com/project')

    assert result == 'merge_sha_123'
    mock_merge_mr.assert_called_once_with(mr_details, 'https://gitlab.example.com/project')
    mock_close_mr.assert_not_called()


@patch('iib.workers.tasks.containerized_utils.close_mr')
@patch('iib.workers.tasks.containerized_utils.merge_mr')
def test_merge_mr_after_build_failure_closes_mr(mock_merge_mr, mock_close_mr):
    """Test that merge failure closes the MR and raises IIBError."""
    mock_merge_mr.side_effect = IIBError("Failed to merge")
    mr_details = {'mr_id': '123', 'mr_url': 'https://example.com/mr/123'}

    with pytest.raises(IIBError, match="Failed to merge MR after successful build"):
        merge_mr_after_build(mr_details, 'https://gitlab.example.com/project')

    mock_close_mr.assert_called_once_with(mr_details, 'https://gitlab.example.com/project')


@patch('iib.workers.tasks.containerized_utils.get_image_label')
@patch('iib.workers.tasks.containerized_utils.extract_files_from_image_non_privileged')
def test_extract_catalog_and_db_prefers_hidden_db(mock_extract, mock_label, tmp_path):
    """When a hidden index.db exists, it is preferred over the labeled db."""

    def label_side_effect(image, label):
        return {
            'operators.operatorframework.io.index.configs.v1': '/configs',
            'operators.operatorframework.io.index.database.v1': '/database/index.db',
        }[label]

    mock_label.side_effect = label_side_effect

    configs_dir, index_db = extract_catalog_and_db_from_image(
        'quay.io/redhat/my-index:test', str(tmp_path)
    )

    assert configs_dir.endswith('configs')
    assert index_db.endswith('index.db')
    # Two extractions: configs dir and the hidden db file.
    assert mock_extract.call_count == 2


@patch('iib.workers.tasks.containerized_utils.get_image_label')
@patch('iib.workers.tasks.containerized_utils.extract_files_from_image_non_privileged')
def test_extract_catalog_and_db_falls_back_to_labeled_db(mock_extract, mock_label, tmp_path):
    """When the hidden db is missing, fall back to the labeled database.v1 path."""
    mock_label.side_effect = lambda image, label: {
        'operators.operatorframework.io.index.configs.v1': '/configs',
        'operators.operatorframework.io.index.database.v1': '/database/index.db',
    }[label]
    # First call (configs) ok; second call (hidden db) raises FileNotFoundInImageError
    # (path genuinely absent) -> fall back to labeled db
    mock_extract.side_effect = [None, FileNotFoundInImageError('no hidden db'), None]

    _, index_db = extract_catalog_and_db_from_image('quay.io/redhat/my-index:test', str(tmp_path))

    assert index_db.endswith('index.db')
    assert mock_extract.call_count == 3


@patch('iib.workers.tasks.containerized_utils.get_image_label')
@patch('iib.workers.tasks.containerized_utils.extract_files_from_image_non_privileged')
def test_extract_catalog_and_db_pure_fbc_creates_empty_db(mock_extract, mock_label, tmp_path):
    """Pure-FBC image (no hidden or labeled db) results in an empty index.db."""
    mock_label.side_effect = lambda image, label: {
        'operators.operatorframework.io.index.configs.v1': '/configs',
        'operators.operatorframework.io.index.database.v1': '',
    }[label]
    # First call (configs) ok; second call (hidden db) raises FileNotFoundInImageError
    # (path genuinely absent) -> no labeled db either
    mock_extract.side_effect = [None, FileNotFoundInImageError('no hidden db')]

    configs_dir, index_db = extract_catalog_and_db_from_image(
        'quay.io/redhat/my-index:test', str(tmp_path)
    )

    assert configs_dir.endswith('configs')
    assert index_db.endswith('index.db')
    assert os.path.exists(index_db)
    assert os.path.getsize(index_db) == 0
    # Only two extraction attempts: configs dir and the failed hidden db lookup.
    # No opm_migrate / privileged call is ever invoked for the pure-FBC fallback.
    assert mock_extract.call_count == 2


@patch('iib.workers.tasks.containerized_utils.get_image_label')
@patch('iib.workers.tasks.containerized_utils.extract_files_from_image_non_privileged')
def test_extract_catalog_and_db_propagates_real_extraction_error(
    mock_extract, mock_label, tmp_path
):
    """A genuine extraction failure (not a missing path) must propagate, not degrade.

    Only FileNotFoundInImageError signals an absent hidden db; a plain IIBError
    (registry/OCI/layer/tar failure) must not be silently swallowed as "no hidden
    db", which would build an image from an incomplete index.db.
    """
    mock_label.side_effect = lambda image, label: {
        'operators.operatorframework.io.index.configs.v1': '/configs',
        'operators.operatorframework.io.index.database.v1': '/database/index.db',
    }[label]
    # First call (configs) ok; second call (hidden db) raises a real error.
    mock_extract.side_effect = [None, IIBError('registry unreachable')]

    with pytest.raises(IIBError, match='registry unreachable'):
        extract_catalog_and_db_from_image('quay.io/redhat/my-index:test', str(tmp_path))


@patch('iib.workers.tasks.containerized_utils.get_image_label')
def test_extract_catalog_and_db_raises_without_configs_label(mock_label):
    """If the image has no FBC configs label, an IIBError is raised."""
    mock_label.return_value = ''

    with pytest.raises(IIBError, match='does not contain a file-based catalog'):
        extract_catalog_and_db_from_image('quay.io/redhat/my-index:test', '/tmp/does-not-matter')


@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.remote_branch_exists', return_value=True)
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token', return_value=('n', 't'))
@mock.patch(
    'iib.workers.tasks.containerized_utils.resolve_git_url', return_value='https://gitlab/x.git'
)
def test_prepare_build_sources_normal(
    mock_url, mock_tok, mock_exists, mock_clone, mock_state, tmp_path
):
    (tmp_path / 'git' / 'v4.14' / 'configs').mkdir(parents=True)
    src = cu.prepare_build_sources(
        request_id=1,
        from_index='quay.io/redhat/my-index:v4.14',
        from_index_resolved='quay.io/redhat/my-index@sha256:deadbeef',
        temp_dir=str(tmp_path),
        ocp_version='v4.14',
        index_to_gitlab_push_map={'quay.io/redhat/my-index': 'https://gitlab/x.git'},
        overwrite_from_index=True,
    )
    assert src.is_divergent is False
    assert src.target_branch == 'v4.14'
    assert src.index_db_path is None  # normal path pulls from ORAS


@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token', return_value=('n', 't'))
@mock.patch('iib.workers.tasks.containerized_utils.remote_branch_exists', return_value=False)
@mock.patch(
    'iib.workers.tasks.containerized_utils.resolve_git_url', return_value='https://gitlab/x.git'
)
def test_prepare_build_sources_divergent_rejects_overwrite(
    mock_url, mock_exists, mock_tok, mock_state, tmp_path
):
    with pytest.raises(IIBError, match='overwrite'):
        cu.prepare_build_sources(
            request_id=1,
            from_index='quay.io/redhat/my-index:test',
            from_index_resolved='quay.io/redhat/my-index@sha256:deadbeef',
            temp_dir=str(tmp_path),
            ocp_version='v4.14',
            index_to_gitlab_push_map={'quay.io/redhat/my-index': 'https://gitlab/x.git'},
            overwrite_from_index=True,
        )


@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.extract_catalog_and_db_from_image')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token', return_value=('n', 't'))
@mock.patch(
    'iib.workers.tasks.containerized_utils.resolve_git_url', return_value='https://gitlab/x.git'
)
def test_prepare_build_sources_divergent_extracts(
    mock_url, mock_tok, mock_clone, mock_extract, mock_state, tmp_path
):
    # tag branch missing, ocp branch present
    with mock.patch(
        'iib.workers.tasks.containerized_utils.remote_branch_exists',
        side_effect=[False, True],
    ):
        cfg = tmp_path / 'ex_configs'
        cfg.mkdir()
        (cfg / 'op').mkdir()
        mock_extract.return_value = (str(cfg), str(tmp_path / 'ex.db'))
        src = cu.prepare_build_sources(
            request_id=1,
            from_index='quay.io/redhat/my-index:test',
            from_index_resolved='quay.io/redhat/my-index@sha256:deadbeef',
            temp_dir=str(tmp_path),
            ocp_version='v4.14',
            index_to_gitlab_push_map={'quay.io/redhat/my-index': 'https://gitlab/x.git'},
            overwrite_from_index=False,
        )
    assert src.is_divergent is True
    assert src.target_branch == 'v4.14'
    assert src.index_db_path == str(tmp_path / 'ex.db')
    # Divergent extraction must read the resolved digest, not the mutable tag, so
    # the index.db matches the exact image the request resolved to.
    mock_extract.assert_called_once()
    assert mock_extract.call_args.args[0] == 'quay.io/redhat/my-index@sha256:deadbeef'
