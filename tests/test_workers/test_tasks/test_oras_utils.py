# SPDX-License-Identifier: GPL-3.0-or-later
"""Basic unit tests for oras_utils."""
import logging
import re

import pytest
from unittest import mock

from iib.exceptions import IIBError
from iib.workers.tasks.oras_utils import (
    get_oras_artifact,
    push_oras_artifact,
    verify_indexdb_cache_sync,
    get_image_stream_digest,
    refresh_indexdb_cache,
)


@pytest.fixture()
def registry_auths():
    return {'auths': {'quay.io': {'auth': 'dXNlcjpwYXNz'}}}  # base64 encoded user:pass


@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_success(mock_run_cmd, mock_mkdtemp):
    """Test successful artifact pull."""
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
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_with_auth(mock_run_cmd, mock_mkdtemp, mock_auth, registry_auths):
    """Test artifact pull with authentication."""
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    mock_run_cmd.return_value = 'Success'
    mock_mkdtemp.return_value = '/tmp/test-dir'

    result = get_oras_artifact(artifact_ref, base_dir, registry_auths)

    assert result == '/tmp/test-dir'
    mock_auth.assert_called_once_with(registry_auths, use_empty_config=True)
    mock_mkdtemp.assert_called_once_with(prefix='iib-oras-', dir=base_dir)
    mock_run_cmd.assert_called_once_with(
        ['oras', 'pull', artifact_ref, '-o', '/tmp/test-dir'],
        exc_msg=f'Failed to pull OCI artifact {artifact_ref}',
    )


@mock.patch('os.path.exists')
@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
@mock.patch('shutil.rmtree')
def test_get_oras_artifact_failure(mock_rmtree, mock_run_cmd, mock_mkdtemp, mock_exists):
    """Test artifact pull failure."""
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    mock_run_cmd.side_effect = IIBError('Pull failed')
    mock_mkdtemp.return_value = '/tmp/test-dir'
    mock_exists.return_value = True

    with pytest.raises(IIBError, match='Pull failed'):
        get_oras_artifact(artifact_ref, base_dir)
    mock_rmtree.assert_called_once_with('/tmp/test-dir')


@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_custom_prefix(mock_run_cmd, mock_mkdtemp):
    """Test artifact pull with custom temp directory prefix."""
    artifact_ref = 'quay.io/test/repo:latest'
    base_dir = '/tmp/base'
    custom_prefix = 'custom-prefix-'
    mock_run_cmd.return_value = 'Success'
    mock_mkdtemp.return_value = '/tmp/custom-dir'

    result = get_oras_artifact(artifact_ref, base_dir, temp_dir_prefix=custom_prefix)

    assert result == '/tmp/custom-dir'
    mock_mkdtemp.assert_called_once_with(prefix=custom_prefix, dir=base_dir)


@mock.patch('tempfile.mkdtemp')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_get_oras_artifact_with_custom_base_dir(mock_run_cmd, mock_mkdtemp):
    """Test artifact pull with custom base directory."""
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


@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_success(mock_run_cmd, mock_exists):
    """Test successful artifact push. Updated local_path to be relative."""
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
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_auth(mock_run_cmd, mock_exists, mock_auth, registry_auths):
    """Test artifact push with authentication. Updated local_path to be relative."""
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = './test.db'
    artifact_type = 'application/vnd.sqlite'
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type, registry_auths)

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


@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_annotations(mock_run_cmd, mock_exists):
    """Test artifact push with annotations. Updated local_path to be relative."""
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


@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_failure(mock_run_cmd, mock_exists):
    """Test artifact push failure."""
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
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_various_types(
    mock_run_cmd, mock_exists, artifact_ref, local_path, artifact_type, expected_cmd
):
    """Test artifact push with various artifact types. Updated local_path to be relative."""
    mock_run_cmd.return_value = 'Success'
    mock_exists.return_value = True

    push_oras_artifact(artifact_ref, local_path, artifact_type)

    mock_run_cmd.assert_called_once_with(
        expected_cmd, exc_msg=f'Failed to push OCI artifact to {artifact_ref}'
    )


@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_relative_path(mock_run_cmd, mock_exists):
    """Test artifact push with relative path (should not add --disable-path-validation)."""
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


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('tempfile.mkdtemp')
@mock.patch("iib.workers.tasks.utils.subprocess")
def test_get_oras_artifact_with_base_dir_wont_leak_credentials(
    mock_subprocess, mock_mkdtemp, mock_auth, registry_auths, caplog
):
    """Ensure the get_oras_artifact with base_dir won't leak credentials in logs."""
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    oras_logger = logging.getLogger('iib.workers.tasks.utils')
    oras_logger.disabled = False
    oras_logger.setLevel(logging.DEBUG)

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

    get_oras_artifact(artifact_ref, base_dir, registry_auths)

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


@mock.patch('iib.workers.tasks.oras_utils.get_image_stream_digest')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_verify_indexdb_cache_sync_match(mock_get_image_digest, mock_get_is_digest):
    """Test successful verification when digests match."""
    mock_get_image_digest.return_value = 'sha256:abc'
    mock_get_is_digest.return_value = 'sha256:abc'
    tag = 'test-tag'

    result = verify_indexdb_cache_sync(tag)

    assert result is True
    mock_get_image_digest.assert_called_once_with('test-artifact-registry/index-db:test-tag')
    mock_get_is_digest.assert_called_once_with(tag)


@mock.patch('iib.workers.tasks.oras_utils.get_image_stream_digest')
@mock.patch('iib.workers.tasks.oras_utils.get_image_digest')
def test_verify_indexdb_cache_sync_no_match(mock_get_image_digest, mock_get_is_digest):
    """Test successful verification when digests don't match."""
    mock_get_image_digest.return_value = 'sha256:abc'
    mock_get_is_digest.return_value = 'sha256:xyz'
    tag = 'test-tag'

    result = verify_indexdb_cache_sync(tag)

    assert result is False
    mock_get_image_digest.assert_called_once_with('test-artifact-registry/index-db:test-tag')
    mock_get_is_digest.assert_called_once_with(tag)


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_refresh_indexdb_cache_success(mock_run_cmd, mock_auth, registry_auths):
    """Test successful cache refresh."""
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


@mock.patch('iib.workers.tasks.oras_utils.run_cmd', side_effect=IIBError('refresh failed'))
def test_refresh_indexdb_cache_failure(mock_run_cmd):
    """Test cache refresh failure."""
    tag = 'test-tag'

    with pytest.raises(IIBError, match='refresh failed'):
        refresh_indexdb_cache(tag)


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_refresh_indexdb_cache_with_empty_registry_auths(mock_run_cmd, mock_auth):
    """Test that refresh_indexdb_cache works correctly when registry_auths is an empty dict."""
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
    "image_name,tag,expected_tag",
    [
        ("iib-pub-pending", "v4.17", "iib-pub-pending-v4.17"),
        ("my-image", "latest", "my-image-latest"),
        ("test-index", "v1.0.0", "test-index-v1.0.0"),
    ],
)
def test_get_artifact_combined_tag(image_name, tag, expected_tag):
    """Test generating combined artifact tags."""
    from iib.workers.tasks.oras_utils import _get_artifact_combined_tag

    result = _get_artifact_combined_tag(image_name, tag)

    assert result == expected_tag


@pytest.mark.parametrize(
    "from_index,expected_pullspec",
    [
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17",
            "test-artifact-registry/index-db:iib-pub-pending-v4.17",
        ),
        (
            "quay.io/namespace/my-image:latest",
            "test-artifact-registry/index-db:my-image-latest",
        ),
        (
            "registry.io/org/repo/index-image:v1.0.0",
            "test-artifact-registry/index-db:index-image-v1.0.0",
        ),
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17@sha256:abc123",
            "test-artifact-registry/index-db:iib-pub-pending-v4.17",
        ),
    ],
)
@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
def test_get_indexdb_artifact_pullspec(mock_gwc, from_index, expected_pullspec):
    """Test constructing index DB artifact pullspecs."""
    from iib.workers.tasks.oras_utils import get_indexdb_artifact_pullspec

    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_tag_template': '{image_name}-{tag}',
    }

    result = get_indexdb_artifact_pullspec(from_index)

    assert result == expected_pullspec


@mock.patch('iib.workers.tasks.oras_utils.get_worker_config')
def test_get_indexdb_artifact_pullspec_invalid(mock_gwc):
    """Test _get_indexdb_artifact_pullspec with invalid pullspec."""
    from iib.workers.tasks.oras_utils import get_indexdb_artifact_pullspec

    mock_gwc.return_value = {
        'iib_index_db_artifact_registry': 'test-artifact-registry',
        'iib_index_db_artifact_template': '{registry}/index-db:{tag}',
        'iib_index_db_artifact_tag_template': '{image_name}-{tag}',
    }

    with pytest.raises(IIBError, match="Missing tag"):
        get_indexdb_artifact_pullspec("registry.example.com/namespace/image")


@mock.patch('iib.workers.tasks.oras_utils.verify_indexdb_cache_sync')
@pytest.mark.parametrize(
    "pullspec,expected_combined_tag,sync_result",
    [
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17",
            "iib-pub-pending-v4.17",
            True,
        ),
        (
            "quay.io/namespace/my-image:latest",
            "my-image-latest",
            False,
        ),
        (
            "registry.io/org/repo/index-image:v1.0.0@sha256:abc123",
            "index-image-v1.0.0",
            True,
        ),
    ],
)
def test_verify_indexdb_cache_for_image(
    mock_verify_sync, pullspec, expected_combined_tag, sync_result
):
    """Test verify_indexdb_cache_for_image with various pullspecs."""
    from iib.workers.tasks.oras_utils import verify_indexdb_cache_for_image

    mock_verify_sync.return_value = sync_result

    result = verify_indexdb_cache_for_image(pullspec)

    assert result == sync_result
    mock_verify_sync.assert_called_once_with(expected_combined_tag)


@mock.patch('iib.workers.tasks.oras_utils.verify_indexdb_cache_sync')
def test_verify_indexdb_cache_for_image_invalid_pullspec(mock_verify_sync):
    """Test verify_indexdb_cache_for_image with invalid pullspec."""
    from iib.workers.tasks.oras_utils import verify_indexdb_cache_for_image

    with pytest.raises(IIBError, match="Missing tag"):
        verify_indexdb_cache_for_image("registry.example.com/namespace/image")

    mock_verify_sync.assert_not_called()


@mock.patch('iib.workers.tasks.oras_utils.refresh_indexdb_cache')
@pytest.mark.parametrize(
    "pullspec,expected_combined_tag",
    [
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17",
            "iib-pub-pending-v4.17",
        ),
        (
            "quay.io/namespace/my-image:latest",
            "my-image-latest",
        ),
        (
            "registry.io/org/repo/index-image:v1.0.0",
            "index-image-v1.0.0",
        ),
        (
            "registry.example.com/namespace/iib-pub-pending:v4.17@sha256:abc123",
            "iib-pub-pending-v4.17",
        ),
    ],
)
def test_refresh_indexdb_cache_for_image(mock_refresh_cache, pullspec, expected_combined_tag):
    """Test refresh_indexdb_cache_for_image with various pullspecs."""
    from iib.workers.tasks.oras_utils import refresh_indexdb_cache_for_image

    refresh_indexdb_cache_for_image(pullspec)

    mock_refresh_cache.assert_called_once_with(expected_combined_tag)


@mock.patch('iib.workers.tasks.oras_utils.refresh_indexdb_cache')
def test_refresh_indexdb_cache_for_image_invalid_pullspec(mock_refresh_cache):
    """Test refresh_indexdb_cache_for_image with invalid pullspec."""
    from iib.workers.tasks.oras_utils import refresh_indexdb_cache_for_image

    with pytest.raises(IIBError, match="Missing tag"):
        refresh_indexdb_cache_for_image("registry.example.com/namespace/image")

    mock_refresh_cache.assert_not_called()


@mock.patch('iib.workers.tasks.oras_utils.refresh_indexdb_cache')
def test_refresh_indexdb_cache_for_image_propagates_exception(mock_refresh_cache):
    """Test if refresh_indexdb_cache_for_image propagates exceptions from refresh_indexdb_cache."""
    from iib.workers.tasks.oras_utils import refresh_indexdb_cache_for_image

    mock_refresh_cache.side_effect = IIBError('Refresh failed')

    with pytest.raises(IIBError, match='Refresh failed'):
        refresh_indexdb_cache_for_image("registry.example.com/namespace/image:v1.0.0")
