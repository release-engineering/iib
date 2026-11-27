# SPDX-License-Identifier: GPL-3.0-or-later
"""Basic unit tests for oras_utils."""

import logging
import hashlib
import json
import re

import pytest
from unittest import mock

from iib.exceptions import ArtifactNotFoundError, IIBError
from iib.workers.tasks.oras_utils import (
    get_oras_artifact,
    push_oras_artifact,
    verify_indexdb_cache_sync,
    get_image_stream_digest,
    refresh_indexdb_cache,
    _get_index_digest,
    _get_content_addressed_artifact_tag,
    get_indexdb_artifact_pullspec,
    get_request_indexdb_artifact_pullspec,
    get_index_tag,
    read_published_index_db_artifact,
    find_existing_index_db_artifact,
    verify_existing_index_db_artifact,
    PublishedIndexDbArtifact,
    _ArtifactResolutionError,
    _MissingLegacyBindingError,
    _manifest_annotations,
    _resolve_oras_manifest,
    publish_verified_legacy_binding,
    _verified_git_ref,
)


def _binding_annotations(payload):
    return {
        'request_id': '42',
        'iib.git_commit': 'a' * 40,
        'iib.configs_tree': 'b' * 40,
        'iib.index_db_sha256': hashlib.sha256(payload).hexdigest(),
    }


def test_read_published_index_db_artifact_pins_manifest_and_payload(tmp_path):
    payload = b'published SQLite database'
    index_db_path = tmp_path / 'index.db'
    index_db_path.write_bytes(payload)
    annotations = _binding_annotations(payload)
    artifact_ref = 'quay.io/team/index-db:idb-abc'
    pinned_ref = 'quay.io/team/index-db@sha256:' + 'c' * 64
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec',
        return_value=artifact_ref,
    ), mock.patch(
        'iib.workers.tasks.oras_utils._resolve_oras_manifest', return_value=pinned_ref
    ), mock.patch(
        'iib.workers.tasks.oras_utils._manifest_annotations', return_value=annotations
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact', return_value=str(tmp_path)
    ) as mock_pull:
        result = read_published_index_db_artifact('quay.io/team/index@sha256:abc', str(tmp_path))

    assert result == PublishedIndexDbArtifact(
        artifact_ref=artifact_ref,
        manifest_ref=pinned_ref,
        index_db_path=str(index_db_path),
        request_id=42,
        git_commit='a' * 40,
        configs_tree='b' * 40,
        database_sha256=annotations['iib.index_db_sha256'],
    )
    mock_pull.assert_called_once_with(pinned_ref, str(tmp_path))


def test_read_published_index_db_artifact_rejects_missing_binding_before_pull(tmp_path):
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec', return_value='artifact'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._resolve_oras_manifest', return_value='artifact@sha256:abc'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._manifest_annotations',
        return_value={'request_id': '42'},
    ), mock.patch(
        'iib.workers.tasks.oras_utils._read_legacy_binding',
        side_effect=IIBError('Published legacy index.db artifact has no verified sidecar'),
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact'
    ) as mock_pull:
        with pytest.raises(IIBError, match='no verified sidecar'):
            read_published_index_db_artifact('image', str(tmp_path))
    mock_pull.assert_not_called()


def test_read_published_index_db_artifact_rejects_conflicting_payload(tmp_path):
    (tmp_path / 'index.db').write_bytes(b'wrong database')
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec', return_value='artifact'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._resolve_oras_manifest', return_value='artifact@sha256:abc'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._manifest_annotations',
        return_value=_binding_annotations(b'correct database'),
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact', return_value=str(tmp_path)
    ):
        with pytest.raises(IIBError, match='database digest conflicts'):
            read_published_index_db_artifact('image', str(tmp_path))


def test_read_published_legacy_binding_requires_exact_artifact_and_payload(tmp_path):
    image = 'quay.io/team/index@sha256:' + 'a' * 64
    payload = b'legacy database'
    (tmp_path / 'index.db').write_bytes(payload)
    manifest_ref = 'quay.io/team/db@sha256:' + 'c' * 64
    binding = {
        'version': 1,
        'image_digest': 'a' * 64,
        'original_manifest_digest': 'sha256:' + 'c' * 64,
        'database_sha256': hashlib.sha256(payload).hexdigest(),
        'request_id': 42,
        'git_commit': 'd' * 40,
        'configs_tree': 'e' * 40,
    }
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec', return_value='artifact'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._resolve_oras_manifest', return_value=manifest_ref
    ), mock.patch(
        'iib.workers.tasks.oras_utils._manifest_annotations', return_value={'request_id': '42'}
    ) as mock_annotations, mock.patch(
        'iib.workers.tasks.oras_utils._read_legacy_binding', return_value=binding
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact', return_value=str(tmp_path)
    ) as mock_pull:
        result = read_published_index_db_artifact(image, str(tmp_path))
        assert result.git_commit == 'd' * 40
        assert result.configs_tree == 'e' * 40
        mock_pull.assert_called_once_with(manifest_ref, str(tmp_path))

        mock_annotations.return_value = {}
        assert read_published_index_db_artifact(image, str(tmp_path)).request_id == 42
        mock_annotations.return_value = {'request_id': '42'}

        binding['original_manifest_digest'] = 'sha256:' + 'f' * 64
        with pytest.raises(IIBError, match='sidecar conflicts'):
            read_published_index_db_artifact(image, str(tmp_path))
        binding['original_manifest_digest'] = 'sha256:' + 'c' * 64
        binding['database_sha256'] = hashlib.sha256(b'wrong').hexdigest()
        with pytest.raises(IIBError, match='database digest conflicts'):
            read_published_index_db_artifact(image, str(tmp_path))


def test_verified_git_ref_rejects_conflict_before_push():
    image = 'quay.io/team/index@sha256:' + 'a' * 64
    commit = 'b' * 40
    tree = 'c' * 40
    outputs = ['commit', tree, json.dumps({'request_id': 42}), 'd' * 40 + '\trefs/tags/x']
    with mock.patch('iib.workers.tasks.oras_utils.run_cmd', side_effect=outputs) as mock_run:
        with pytest.raises(IIBError, match='another commit'):
            _verified_git_ref(image, '/repo', commit, tree, 42)
    assert not any('push' in call.args[0] for call in mock_run.call_args_list)


def test_verified_git_ref_retains_and_confirms_missing_ref():
    image = 'quay.io/team/index@sha256:' + 'a' * 64
    commit = 'b' * 40
    tree = 'c' * 40
    outputs = [
        'commit',
        tree,
        json.dumps({'request_id': 42}),
        '',
        'pushed',
        commit + '\trefs/tags/iib-index-' + 'a' * 64,
    ]
    with mock.patch('iib.workers.tasks.oras_utils.run_cmd', side_effect=outputs) as mock_run:
        _verified_git_ref(image, '/repo', commit, tree, 42)
    assert mock_run.call_args_list[4].args[0] == [
        'git',
        '-C',
        '/repo',
        'push',
        'origin',
        f'{commit}:refs/tags/iib-index-' + 'a' * 64,
    ]


def test_publish_verified_legacy_binding_checks_original_before_sidecar(tmp_path):
    image = 'quay.io/team/index@sha256:' + 'a' * 64
    payload = b'legacy database'
    (tmp_path / 'index.db').write_bytes(payload)
    original = 'sha256:' + 'c' * 64
    kwargs = {
        'git_repo_path': '/verified/repo',
        'git_commit': 'd' * 40,
        'configs_tree': 'e' * 40,
        'request_id': 42,
        'database_sha256': hashlib.sha256(payload).hexdigest(),
        'original_manifest_digest': original,
    }
    manifest_ref = 'quay.io/team/db@' + original
    binding = {
        'version': 1,
        'image_digest': 'a' * 64,
        'original_manifest_digest': original,
        'database_sha256': kwargs['database_sha256'],
        'request_id': 42,
        'git_commit': 'd' * 40,
        'configs_tree': 'e' * 40,
    }
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec', return_value='artifact'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._resolve_oras_manifest', return_value=manifest_ref
    ), mock.patch(
        'iib.workers.tasks.oras_utils._manifest_annotations', return_value={'request_id': '42'}
    ) as mock_annotations, mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact', return_value=str(tmp_path)
    ), mock.patch(
        'iib.workers.tasks.oras_utils._verified_git_ref'
    ) as mock_git, mock.patch(
        'iib.workers.tasks.oras_utils.push_oras_artifact'
    ) as mock_push, mock.patch(
        'iib.workers.tasks.oras_utils._legacy_binding_ref', return_value='artifact-binding'
    ), mock.patch(
        'iib.workers.tasks.oras_utils._read_legacy_binding',
        side_effect=[
            _MissingLegacyBindingError(
                'Published legacy index.db artifact has no verified sidecar'
            ),
            binding,
        ],
    ) as mock_read:
        publish_verified_legacy_binding(image, str(tmp_path), **kwargs)
        mock_git.assert_called_once_with(image, '/verified/repo', 'd' * 40, 'e' * 40, 42)
        mock_push.assert_called_once()

        mock_read.side_effect = None
        mock_read.return_value = binding
        mock_annotations.return_value = {}
        publish_verified_legacy_binding(image, str(tmp_path), **kwargs)
        mock_push.assert_called_once()

        mock_git.reset_mock()
        mock_push.reset_mock()
        kwargs['original_manifest_digest'] = 'sha256:' + 'f' * 64
        with pytest.raises(IIBError, match='manifest differs'):
            publish_verified_legacy_binding(image, str(tmp_path), **kwargs)
        mock_git.assert_not_called()
        mock_push.assert_not_called()


def test_find_existing_artifact_only_accepts_explicit_missing_signal(tmp_path):
    with mock.patch(
        'iib.workers.tasks.oras_utils.read_published_index_db_artifact',
        side_effect=_ArtifactResolutionError('resolution failed'),
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec', return_value='artifact'
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact',
        side_effect=ArtifactNotFoundError('missing'),
    ) as mock_pull:
        assert find_existing_index_db_artifact('image', str(tmp_path)) is None
    mock_pull.assert_called_once_with('artifact', str(tmp_path))


def test_find_existing_artifact_propagates_registry_failure(tmp_path):
    with mock.patch(
        'iib.workers.tasks.oras_utils.read_published_index_db_artifact',
        side_effect=_ArtifactResolutionError('resolution failed'),
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_indexdb_artifact_pullspec', return_value='artifact'
    ), mock.patch(
        'iib.workers.tasks.oras_utils.get_oras_artifact', side_effect=IIBError('unavailable')
    ):
        with pytest.raises(IIBError, match='unavailable'):
            find_existing_index_db_artifact('image', str(tmp_path))


def test_verify_existing_artifact_rejects_producer_or_database_conflicts(tmp_path):
    db_path = tmp_path / 'index.db'
    db_path.write_bytes(b'database')
    artifact = PublishedIndexDbArtifact(
        'artifact',
        'artifact@sha256:abc',
        str(db_path),
        42,
        'a' * 40,
        'b' * 40,
        hashlib.sha256(b'database').hexdigest(),
    )
    verify_existing_index_db_artifact(
        artifact,
        request_id=42,
        git_commit='a' * 40,
        configs_tree='b' * 40,
        index_db_path=str(db_path),
    )
    with pytest.raises(IIBError, match='conflicting binding'):
        verify_existing_index_db_artifact(
            artifact,
            request_id=43,
            git_commit='a' * 40,
            configs_tree='b' * 40,
            index_db_path=str(db_path),
        )
    db_path.write_bytes(b'changed')
    with pytest.raises(IIBError, match='conflicting binding'):
        verify_existing_index_db_artifact(
            artifact,
            request_id=42,
            git_commit='a' * 40,
            configs_tree='b' * 40,
            index_db_path=str(db_path),
        )


def test_resolve_oras_manifest_uses_digest_instead_of_tag():
    artifact = 'quay.io/team/index-db:idb-abc'
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_worker_config', return_value=_oras_worker_config_minimal()
    ), mock.patch(
        'iib.workers.tasks.oras_utils.run_cmd', return_value='sha256:' + 'c' * 64
    ) as mock_run:
        assert _resolve_oras_manifest(artifact) == 'quay.io/team/index-db@sha256:' + 'c' * 64
    mock_run.assert_called_once_with(
        ['oras', 'resolve', artifact], exc_msg=f'Failed to resolve OCI artifact {artifact}'
    )


def test_manifest_annotations_require_single_database_layer():
    manifest = {
        'annotations': _binding_annotations(b'database'),
        'layers': [
            {
                'annotations': {'org.opencontainers.image.title': 'index.db'},
                'digest': 'sha256:' + hashlib.sha256(b'database').hexdigest(),
            }
        ],
    }
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_worker_config', return_value=_oras_worker_config_minimal()
    ), mock.patch('iib.workers.tasks.oras_utils.run_cmd', return_value=json.dumps(manifest)):
        assert (
            _manifest_annotations('quay.io/team/db@sha256:' + 'c' * 64) == manifest['annotations']
        )
    manifest['layers'][0]['digest'] = 'sha256:' + 'd' * 64
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_worker_config', return_value=_oras_worker_config_minimal()
    ), mock.patch('iib.workers.tasks.oras_utils.run_cmd', return_value=json.dumps(manifest)):
        with pytest.raises(IIBError, match='layer conflicts'):
            _manifest_annotations('quay.io/team/db@sha256:' + 'c' * 64)
    manifest['layers'].append(manifest['layers'][0])
    with mock.patch(
        'iib.workers.tasks.oras_utils.get_worker_config', return_value=_oras_worker_config_minimal()
    ), mock.patch('iib.workers.tasks.oras_utils.run_cmd', return_value=json.dumps(manifest)):
        with pytest.raises(IIBError, match='ambiguous manifest content'):
            _manifest_annotations('quay.io/team/db@sha256:' + 'c' * 64)


@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_get_index_digest_strips_algo_prefix(mock_digest):
    mock_digest.return_value = 'sha256:' + 'a' * 64
    assert _get_index_digest('quay.io/ns/repo:v4.17') == 'a' * 64
    mock_digest.assert_called_once_with('quay.io/ns/repo:v4.17')


@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_get_index_digest_uses_known_digest_without_registry_lookup(mock_digest):
    assert _get_index_digest(f"quay.io/ns/repo@sha256:{'a' * 64}") == 'a' * 64
    mock_digest.assert_not_called()


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_combined_tag_is_digest_only(mock_digest, mock_gwc):
    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.return_value = 'sha256:' + 'b' * 64
    assert _get_content_addressed_artifact_tag('quay.io/my-ns/iib-pub:v4.17') == 'idb-' + 'b' * 64


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_same_content_different_pullspec_same_tag(mock_digest, mock_gwc):
    """Problem 1b: promotion — identical digest under two pullspecs => identical tag."""
    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.return_value = 'sha256:' + 'c' * 64
    staging = _get_content_addressed_artifact_tag('quay.io/my-ns/iib-pub:v4.17')
    released = _get_content_addressed_artifact_tag(
        'registry.access.redhat.com/some-ns/operator-index:v4.17'
    )
    assert staging == released == 'idb-' + 'c' * 64


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_different_content_different_tag(mock_digest, mock_gwc):
    """Problem 1: different images (different digests) => different tags."""
    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.side_effect = ['sha256:' + 'd' * 64, 'sha256:' + 'e' * 64]
    a = _get_content_addressed_artifact_tag('quay.io/redhat/foo:v4.17')
    b = _get_content_addressed_artifact_tag('quay.io/redhat-pending/foo:v4.17')
    assert a != b


@pytest.fixture()
def registry_auths():
    return {'auths': {'quay.io': {'auth': 'dXNlcjpwYXNz'}}}  # base64 encoded user:pass


def _oras_worker_config_minimal(**extra):
    cfg = {'iib_index_db_oras_auth_path': ''}
    cfg.update(extra)
    return cfg


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_success(mock_run_cmd, mock_mkdtemp, mock_gwc):
    """Test successful artifact pull."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    mock_run_cmd.return_value = 'Success'
    mock_mkdtemp.return_value = '/tmp/test-dir'

    result = get_oras_artifact(artifact_ref, base_dir)

    assert result == '/tmp/test-dir'
    mock_mkdtemp.assert_called_once_with(prefix='iib-oras-', dir=base_dir)
    mock_run_cmd.assert_called_once_with(
        ['oras', 'pull', artifact_ref, '-o', '/tmp/test-dir'],
        exc_msg=f'Failed to pull OCI artifact {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_with_auth(
    mock_run_cmd, mock_mkdtemp, mock_gwc, mock_auth, registry_auths
):
    """Explicit registry_auths are passed through to set_registry_auths."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    mock_run_cmd.return_value = 'Success'
    mock_mkdtemp.return_value = '/tmp/test-dir'

    result = get_oras_artifact(artifact_ref, base_dir, registry_auths=registry_auths)

    assert result == '/tmp/test-dir'
    mock_auth.assert_called_once_with(registry_auths, use_empty_config=True)
    mock_mkdtemp.assert_called_once_with(prefix='iib-oras-', dir=base_dir)
    mock_run_cmd.assert_called_once_with(
        ['oras', 'pull', artifact_ref, '-o', '/tmp/test-dir'],
        exc_msg=f'Failed to pull OCI artifact {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
@mock.patch('shutil.rmtree')
def test_get_oras_artifact_failure(mock_rmtree, mock_run_cmd, mock_mkdtemp, mock_exists, mock_gwc):
    """Test artifact pull failure."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    mock_run_cmd.side_effect = IIBError('Pull failed')
    mock_mkdtemp.return_value = '/tmp/test-dir'
    mock_exists.return_value = True

    with pytest.raises(IIBError, match='Pull failed') as exc_info:
        get_oras_artifact(artifact_ref, base_dir)
    mock_rmtree.assert_called_once_with('/tmp/test-dir')
    assert not isinstance(exc_info.value, ArtifactNotFoundError)


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
@mock.patch('shutil.rmtree')
def test_get_oras_artifact_missing_preserves_not_found_type(
    mock_rmtree, mock_run_cmd, mock_mkdtemp, mock_exists, mock_gwc
):
    """A missing artifact must stay distinguishable after the cleanup wrapper re-raises."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    mock_run_cmd.side_effect = ArtifactNotFoundError('Failed to pull OCI artifact')
    mock_mkdtemp.return_value = '/tmp/test-dir'
    mock_exists.return_value = True

    with pytest.raises(ArtifactNotFoundError, match='Failed to pull OCI artifact') as exc_info:
        get_oras_artifact(artifact_ref, '/tmp/base')

    mock_rmtree.assert_called_once_with('/tmp/test-dir')
    assert isinstance(exc_info.value.__cause__, ArtifactNotFoundError)


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_custom_prefix(mock_run_cmd, mock_mkdtemp, mock_gwc):
    """Test artifact pull with custom temp directory prefix."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    custom_prefix = 'custom-prefix-'
    mock_run_cmd.return_value = 'Success'
    mock_mkdtemp.return_value = '/tmp/custom-dir'

    result = get_oras_artifact(artifact_ref, base_dir, temp_dir_prefix=custom_prefix)

    assert result == '/tmp/custom-dir'
    mock_mkdtemp.assert_called_once_with(prefix=custom_prefix, dir=base_dir)


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_with_custom_base_dir(mock_run_cmd, mock_mkdtemp, mock_gwc):
    """Test artifact pull with custom base directory."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/iib-123'
    mock_run_cmd.return_value = 'Success'
    mock_mkdtemp.return_value = '/tmp/iib-123/iib-oras-abc123'

    result = get_oras_artifact(artifact_ref, base_dir)

    assert result == '/tmp/iib-123/iib-oras-abc123'
    mock_mkdtemp.assert_called_once_with(prefix='iib-oras-', dir=base_dir)
    mock_run_cmd.assert_called_once_with(
        ['oras', 'pull', artifact_ref, '-o', '/tmp/iib-123/iib-oras-abc123'],
        exc_msg=f'Failed to pull OCI artifact {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_success(mock_run_cmd, mock_exists, mock_gwc):
    """Test successful artifact push. Updated local_path to be relative."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = './test.db'
    artifact_type = 'application/vnd.sqlite'
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type)

    mock_run_cmd.assert_called_once_with(
        [
            'oras',
            'push',
            artifact_ref,
            f'{local_path}:{artifact_type}',
        ],
        exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_auth(
    mock_run_cmd, mock_exists, mock_gwc, mock_auth, registry_auths
):
    """Explicit registry_auths are passed through to set_registry_auths."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = './test.db'
    artifact_type = 'application/vnd.sqlite'
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type, registry_auths=registry_auths)

    mock_auth.assert_called_once_with(registry_auths, use_empty_config=True)
    mock_run_cmd.assert_called_once_with(
        [
            'oras',
            'push',
            artifact_ref,
            f'{local_path}:{artifact_type}',
        ],
        exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_annotations(mock_run_cmd, mock_exists, mock_gwc):
    """Test artifact push with annotations. Updated local_path to be relative."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = './test.db'
    artifact_type = 'application/vnd.sqlite'
    annotations = {'key1': 'value1', 'key2': 'value2'}
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type, annotations=annotations)

    expected_cmd = [
        'oras',
        'push',
        artifact_ref,
        f'{local_path}:{artifact_type}',
    ]
    for key, value in annotations.items():
        expected_cmd.extend(['--annotation', f'{key}={value}'])

    mock_run_cmd.assert_called_once_with(
        expected_cmd, exc_msg=f'Failed to push OCI artifact to {artifact_ref}'
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_failure(mock_run_cmd, mock_exists, mock_gwc):
    """Test artifact push failure."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = './test.db'
    artifact_type = 'application/vnd.sqlite'
    mock_run_cmd.side_effect = IIBError('Push failed')
    mock_exists.return_value = True

    with pytest.raises(IIBError, match='Push failed'):
        push_oras_artifact(artifact_ref, local_path, artifact_type)


@mock.patch('os.path.exists')
def test_push_oras_artifact_file_not_found(mock_exists):
    """Test artifact push with non-existent file."""
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = '/tmp/test.db'
    artifact_type = 'application/vnd.sqlite'
    mock_exists.return_value = False

    with pytest.raises(IIBError, match=f'Local artifact path does not exist: {local_path}'):
        push_oras_artifact(artifact_ref, local_path, artifact_type)


@pytest.mark.parametrize(
    "artifact_ref,local_path,artifact_type,expected_cmd",
    [
        (
            "quay.io/test/repo:latest",
            "./test.db",
            "application/vnd.sqlite",
            [
                "oras",
                "push",
                "quay.io/test/repo:latest",
                "./test.db:application/vnd.sqlite",
            ],
        ),
        (
            "registry.example.com/myapp:v1.0",
            "./config.yaml",
            "application/vnd.yaml",
            [
                "oras",
                "push",
                "registry.example.com/myapp:v1.0",
                "./config.yaml:application/vnd.yaml",
            ],
        ),
        (
            "docker.io/library/nginx:latest",
            "./nginx.conf",
            "application/vnd.config",
            [
                "oras",
                "push",
                "docker.io/library/nginx:latest",
                "./nginx.conf:application/vnd.config",
            ],
        ),
    ],
)
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_various_types(
    mock_run_cmd, mock_exists, mock_gwc, artifact_ref, local_path, artifact_type, expected_cmd
):
    """Test artifact push with various artifact types. Updated local_path to be relative."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type)

    mock_run_cmd.assert_called_once_with(
        expected_cmd, exc_msg=f'Failed to push OCI artifact to {artifact_ref}'
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_relative_path(mock_run_cmd, mock_exists, mock_gwc):
    """Test artifact push with relative path (should not add --disable-path-validation)."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = './test.db'  # Relative path
    artifact_type = 'application/vnd.sqlite'
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type)

    mock_run_cmd.assert_called_once_with(
        ['oras', 'push', artifact_ref, f'{local_path}:{artifact_type}'],
        exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('os.path.exists')
def test_push_oras_artifact_rejects_absolute_local_path(mock_exists, mock_gwc):
    """Absolute local_path is rejected after existence check (ORAS path collision avoidance)."""
    mock_gwc.return_value = _oras_worker_config_minimal()
    mock_exists.return_value = True

    with pytest.raises(IIBError, match='Local artifact path must be relative: /abs/test.db'):
        push_oras_artifact('quay.io/test/repo:latest', '/abs/test.db', 'application/vnd.sqlite')


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('tempfile.mkdtemp')
@mock.patch("iib.workers.tasks.utils.subprocess")
def test_get_oras_artifact_with_base_dir_wont_leak_credentials(
    mock_subprocess,
    mock_mkdtemp,
    mock_gwc,
    mock_auth,
    registry_auths,
    caplog,
):
    """Ensure get_oras_artifact with base_dir won't leak credentials in logs."""
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    oras_logger = logging.getLogger('iib.workers.tasks.utils')
    oras_logger.disabled = False
    oras_logger.setLevel(logging.DEBUG)

    mock_gwc.return_value = _oras_worker_config_minimal()

    # Prepare the subprocess mock
    mock_run_result = mock.MagicMock()
    mock_run_result.returncode = 0
    mock_subprocess.run.return_value = mock_run_result
    default_run_cmd_args = {
        "universal_newlines": True,
        "encoding": "utf-8",
        "stderr": mock_subprocess.PIPE,
        "stdout": mock_subprocess.PIPE,
    }

    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/iib-123'
    mock_mkdtemp.return_value = '/tmp/iib-123/iib-oras-abc123'

    get_oras_artifact(artifact_ref, base_dir, registry_auths=registry_auths)

    mock_auth.assert_called_once_with(registry_auths, use_empty_config=True)
    mock_subprocess.run.assert_called_with(
        ['oras', 'pull', artifact_ref, '-o', '/tmp/iib-123/iib-oras-abc123'],
        **default_run_cmd_args,
    )

    # Ensure the credentials aren't leaked
    all_messages = ' '.join(caplog.messages)
    assert 'dXNlcjpwYXNz' not in all_messages  # base64 encoded credentials
    assert 'user:pass' not in all_messages  # decoded credentials


@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_image_stream_digest(mock_run_cmd):
    """Test successful retrieval of image digest from ImageStream."""
    mock_run_cmd.return_value = 'sha256:12345'
    tag = 'test-tag'

    digest = get_image_stream_digest(tag)

    assert digest == 'sha256:12345'
    mock_run_cmd.assert_called_once_with(
        [
            'oc',
            'get',
            'imagestream',
            'index-db-cache',
            '-o',
            'jsonpath=\'{.status.tags[?(@.tag=="test-tag")].items[0].image}\'',
        ],
        exc_msg='Failed to get digest for ImageStream tag test-tag.',
    )


@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_image_stream_digest_empty_string(mock_run_cmd):
    """Test get_image_stream_digest with empty string output."""
    mock_run_cmd.return_value = ''
    tag = 'test-tag'
    digest = get_image_stream_digest(tag)

    assert digest is None or digest == '', "Expected None or empty digest for empty string output"


@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_image_stream_digest_invalid_format(mock_run_cmd):
    """Test get_image_stream_digest with non-digest output."""
    mock_run_cmd.return_value = 'not-a-digest'
    tag = 'test-tag'
    digest = get_image_stream_digest(tag)

    assert (
        digest is None or digest == 'not-a-digest'
    ), "Expected None or raw output for invalid digest format"


@mock.patch('iib.workers.tasks.oras_utils.run_cmd', side_effect=IIBError('cmd failed'))
def test_get_image_stream_digest_failure(mock_run_cmd):
    """Test failure during retrieval of image digest from ImageStream."""
    with pytest.raises(IIBError, match='cmd failed'):
        get_image_stream_digest('test-tag')


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_stream_digest')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_verify_indexdb_cache_sync_match(mock_run_cmd, mock_get_is_digest, mock_gwc):
    """Test successful verification when digests match."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_oras_auth_path': '',
    }
    mock_run_cmd.return_value = '{"digest": "sha256:abc"}'
    mock_get_is_digest.return_value = 'sha256:abc'
    tag = 'test-tag'

    result = verify_indexdb_cache_sync(tag)

    assert result is True
    mock_run_cmd.assert_called_once_with(
        ['oras', 'manifest', 'fetch', '--descriptor', 'test-artifact-registry/index-db:test-tag'],
        exc_msg='Failed to fetch manifest descriptor for test-artifact-registry/index-db:test-tag',
    )
    mock_get_is_digest.assert_called_once_with(tag)


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_stream_digest')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_verify_indexdb_cache_sync_no_match(mock_run_cmd, mock_get_is_digest, mock_gwc):
    """Test successful verification when digests don't match."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_oras_auth_path': '',
    }
    mock_run_cmd.return_value = '{"digest": "sha256:abc"}'
    mock_get_is_digest.return_value = 'sha256:xyz'
    tag = 'test-tag'

    result = verify_indexdb_cache_sync(tag)

    assert result is False
    mock_run_cmd.assert_called_once_with(
        ['oras', 'manifest', 'fetch', '--descriptor', 'test-artifact-registry/index-db:test-tag'],
        exc_msg='Failed to fetch manifest descriptor for test-artifact-registry/index-db:test-tag',
    )
    mock_get_is_digest.assert_called_once_with(tag)


@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_stream_digest')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_verify_indexdb_cache_sync_with_oras_auth(
    mock_run_cmd, mock_get_is_digest, mock_gwc, mock_exists
):
    """Test that --registry-config is passed when oras auth path exists."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_oras_auth_path': '/path/to/oras/config.json',
    }
    mock_exists.return_value = True
    mock_run_cmd.return_value = '{"digest": "sha256:abc"}'
    mock_get_is_digest.return_value = 'sha256:abc'
    tag = 'test-tag'

    result = verify_indexdb_cache_sync(tag)

    assert result is True
    mock_run_cmd.assert_called_once_with(
        [
            'oras',
            'manifest',
            'fetch',
            '--descriptor',
            '--registry-config',
            '/path/to/oras/config.json',
            'test-artifact-registry/index-db:test-tag',
        ],
        exc_msg='Failed to fetch manifest descriptor for test-artifact-registry/index-db:test-tag',
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_refresh_indexdb_cache_success(mock_run_cmd, mock_auth, mock_gwc, registry_auths):
    """Test successful cache refresh."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }
    tag = 'test-tag'

    refresh_indexdb_cache(tag, registry_auths)

    mock_auth.assert_called_once_with(registry_auths, use_empty_config=True)
    mock_run_cmd.assert_called_once_with(
        [
            'oc',
            'import-image',
            'index-db-cache:test-tag',
            '--from=test-artifact-registry/index-db:test-tag',
            '--confirm',
        ],
        exc_msg='Failed to refresh OCI artifact test-tag.',
    )


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd', side_effect=IIBError('refresh failed'))
def test_refresh_indexdb_cache_failure(mock_run_cmd, mock_gwc):
    """Test cache refresh failure."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }
    tag = 'test-tag'

    with pytest.raises(IIBError, match='refresh failed'):
        refresh_indexdb_cache(tag)


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_refresh_indexdb_cache_with_empty_registry_auths(mock_run_cmd, mock_auth, mock_gwc):
    """Test that refresh_indexdb_cache works correctly when registry_auths is an empty dict."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }
    tag = 'v4.15'
    empty_auths = {}

    # Call the function with empty registry_auths
    refresh_indexdb_cache(tag, registry_auths=empty_auths)

    # Verify set_registry_auths was called with empty dict as argument
    mock_auth.assert_called_once_with(empty_auths, use_empty_config=True)

    # Verify the oc command was executed
    mock_run_cmd.assert_called_once()


@pytest.mark.parametrize(
    "pullspec,expected_name,expected_tag",
    [
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17",
            "iib-pub-pending",
            "v4.17",
        ),
        (
            "quay.io/namespace/my-image:latest",
            "my-image",
            "latest",
        ),
        (
            "registry.io/org/repo/index-image:v1.0.0",
            "index-image",
            "v1.0.0",
        ),
        (
            "docker.io/library/nginx:1.21.0",
            "nginx",
            "1.21.0",
        ),
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17@sha256:abc123",
            "iib-pub-pending",
            "v4.17",
        ),
        (
            "quay.io/namespace/image-name:tag-with-dashes",
            "image-name",
            "tag-with-dashes",
        ),
        (
            "registry.io/namespace/image:v1.0.0-rc1",
            "image",
            "v1.0.0-rc1",
        ),
    ],
)
def test_get_name_and_tag_from_pullspec_valid(pullspec, expected_name, expected_tag):
    """Test parsing valid pullspec strings."""
    from iib.workers.tasks.oras_utils import _get_name_and_tag_from_pullspec

    name, tag = _get_name_and_tag_from_pullspec(pullspec)

    assert name == expected_name
    assert tag == expected_tag


@pytest.mark.parametrize(
    "invalid_pullspec,expected_error_msg",
    [
        (
            "registry.example.com/namespace/iib-pub-pending",
            "Invalid pullspec format: 'registry.example.com/namespace/iib-pub-pending'. "
            "Missing tag (':') delimiter.",
        ),
        (
            "registry.example.com/namespace/image:",
            "Invalid pullspec format: 'registry.example.com/namespace/image:'. "
            "Could not parse name:tag structure.",
        ),
        (
            "invalid-pullspec-format",
            "Invalid pullspec format: 'invalid-pullspec-format'. " "Missing tag (':') delimiter.",
        ),
    ],
)
def test_get_name_and_tag_from_pullspec_invalid(invalid_pullspec, expected_error_msg):
    """Test parsing invalid pullspec strings."""
    from iib.workers.tasks.oras_utils import _get_name_and_tag_from_pullspec

    with pytest.raises(IIBError, match=re.escape(expected_error_msg)):
        _get_name_and_tag_from_pullspec(invalid_pullspec)


@pytest.mark.parametrize(
    "pullspec,digest,expected_tag",
    [
        ("quay.io/ns/iib-pub-pending:v4.17", "a" * 64, "idb-" + "a" * 64),
        ("quay.io/ns/my-image:latest", "b" * 64, "idb-" + "b" * 64),
        ("quay.io/ns/test-index:v1.0.0", "c" * 64, "idb-" + "c" * 64),
    ],
)
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_get_content_addressed_artifact_tag(mock_digest, mock_gwc, pullspec, digest, expected_tag):
    """Test generating combined artifact tags."""
    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.return_value = f'sha256:{digest}'

    result = _get_content_addressed_artifact_tag(pullspec)

    assert result == expected_tag


@pytest.mark.parametrize(
    "from_index,digest",
    [
        ("registry.example.com/namespace/iib-pub-pending:v4.17", "a" * 64),
        ("quay.io/namespace/my-image:latest", "b" * 64),
        ("registry.io/org/repo/index-image:v1.0.0", "c" * 64),
        ("registry.example.com/namespace/iib-pub-pending:v4.17@sha256:abc123", "d" * 64),
    ],
)
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_get_indexdb_artifact_pullspec(mock_digest, mock_gwc, from_index, digest):
    """Test constructing index DB artifact pullspecs."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_tag_template': 'idb-{digest}',
    }
    mock_digest.return_value = f'sha256:{digest}'

    result = get_indexdb_artifact_pullspec(from_index)

    assert result == f'test-artifact-registry/index-db:idb-{digest}'
    mock_digest.assert_called_once_with(from_index)


@mock.patch('iib.workers.tasks.oras_utils._get_content_addressed_artifact_tag')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
def test_get_request_indexdb_artifact_pullspec(mock_config, mock_tag):
    mock_config.return_value = {
        'iib_index_db_artifact_registry': 'quay.io/iib',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
    }
    mock_tag.return_value = 'idb-' + 'a' * 64
    result = get_request_indexdb_artifact_pullspec(
        'registry.internal/iib-build:42@sha256:' + 'a' * 64, 42
    )
    assert result == 'quay.io/iib/index-db:idb-' + 'a' * 64 + '-42'


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_get_indexdb_artifact_pullspec_digest_resolution_failure(mock_digest, mock_gwc):
    """Test get_indexdb_artifact_pullspec propagates digest resolution failures."""
    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_tag_template': 'idb-{digest}',
    }
    mock_digest.side_effect = IIBError('Failed to inspect image')

    with pytest.raises(IIBError, match="Failed to inspect image"):
        get_indexdb_artifact_pullspec("registry.example.com/namespace/image:v1.0.0")


@mock.patch('iib.workers.tasks.oras_utils.verify_indexdb_cache_sync')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
@pytest.mark.parametrize(
    "pullspec,digest,sync_result",
    [
        ("registry.example.com/namespace/iib-pub-pending:v4.17", "a" * 64, True),
        ("quay.io/namespace/my-image:latest", "b" * 64, False),
        ("registry.io/org/repo/index-image:v1.0.0@sha256:abc123", "c" * 64, True),
    ],
)
def test_verify_indexdb_cache_for_image(
    mock_digest, mock_gwc, mock_verify_sync, pullspec, digest, sync_result
):
    """Test verify_indexdb_cache_for_image with various pullspecs."""
    from iib.workers.tasks.oras_utils import verify_indexdb_cache_for_image

    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.return_value = f'sha256:{digest}'
    mock_verify_sync.return_value = sync_result

    result = verify_indexdb_cache_for_image(pullspec)

    assert result == sync_result
    mock_verify_sync.assert_called_once_with(f'idb-{digest}')


@mock.patch('iib.workers.tasks.oras_utils.verify_indexdb_cache_sync')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_verify_indexdb_cache_for_image_digest_resolution_failure(mock_digest, mock_verify_sync):
    """Test verify_indexdb_cache_for_image propagates digest resolution failures."""
    from iib.workers.tasks.oras_utils import verify_indexdb_cache_for_image

    mock_digest.side_effect = IIBError('Failed to inspect image')

    with pytest.raises(IIBError, match="Failed to inspect image"):
        verify_indexdb_cache_for_image("registry.example.com/namespace/image:v1.0.0")

    mock_verify_sync.assert_not_called()


@mock.patch('iib.workers.tasks.oras_utils.refresh_indexdb_cache')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
@pytest.mark.parametrize(
    "pullspec,digest",
    [
        ("registry.example.com/namespace/iib-pub-pending:v4.17", "a" * 64),
        ("quay.io/namespace/my-image:latest", "b" * 64),
        ("registry.io/org/repo/index-image:v1.0.0", "c" * 64),
        ("registry.example.com/namespace/iib-pub-pending:v4.17@sha256:abc123", "d" * 64),
    ],
)
def test_refresh_indexdb_cache_for_image(
    mock_digest, mock_gwc, mock_refresh_cache, pullspec, digest
):
    """Test refresh_indexdb_cache_for_image with various pullspecs."""
    from iib.workers.tasks.oras_utils import refresh_indexdb_cache_for_image

    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.return_value = f'sha256:{digest}'

    refresh_indexdb_cache_for_image(pullspec)

    mock_refresh_cache.assert_called_once_with(f'idb-{digest}')


@mock.patch('iib.workers.tasks.oras_utils.refresh_indexdb_cache')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_refresh_indexdb_cache_for_image_digest_resolution_failure(mock_digest, mock_refresh_cache):
    """Test refresh_indexdb_cache_for_image propagates digest resolution failures."""
    from iib.workers.tasks.oras_utils import refresh_indexdb_cache_for_image

    mock_digest.side_effect = IIBError('Failed to inspect image')

    with pytest.raises(IIBError, match="Failed to inspect image"):
        refresh_indexdb_cache_for_image("registry.example.com/namespace/image:v1.0.0")

    mock_refresh_cache.assert_not_called()


@mock.patch('iib.workers.tasks.oras_utils.refresh_indexdb_cache')
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_refresh_indexdb_cache_for_image_propagates_exception(
    mock_digest, mock_gwc, mock_refresh_cache
):
    """Test if refresh_indexdb_cache_for_image propagates exceptions from refresh_indexdb_cache."""
    from iib.workers.tasks.oras_utils import refresh_indexdb_cache_for_image

    mock_gwc.return_value = {'iib_index_db_artifact_tag_template': 'idb-{digest}'}
    mock_digest.return_value = 'sha256:' + 'e' * 64
    mock_refresh_cache.side_effect = IIBError('Refresh failed')

    with pytest.raises(IIBError, match='Refresh failed'):
        refresh_indexdb_cache_for_image("registry.example.com/namespace/image:v1.0.0")


def test_get_index_tag():
    assert get_index_tag('quay.io/redhat/my-index:v4.17') == 'v4.17'
