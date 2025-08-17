# SPDX-License-Identifier: GPL-3.0-or-later
"""Basic unit tests for oras_utils."""
import logging
import pytest
from unittest import mock

from iib.exceptions import IIBError
from iib.workers.tasks.oras_utils import (
    get_oras_artifact,
    push_oras_artifact,
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
    """Test successful artifact push."""
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = '/tmp/test.db'
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
            '--disable-path-validation',
        ],
        exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
    )


@mock.patch('iib.workers.tasks.oras_utils.set_registry_auths')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_auth(mock_run_cmd, mock_exists, mock_auth, registry_auths):
    """Test artifact push with authentication."""
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = '/tmp/test.db'
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
            '--disable-path-validation',
        ],
        exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
    )


@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_with_annotations(mock_run_cmd, mock_exists):
    """Test artifact push with annotations."""
    artifact_ref = 'quay.io/test/repo:latest'
    local_path = '/tmp/test.db'
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
        '--disable-path-validation',
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
    local_path = '/tmp/test.db'
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
            "/tmp/test.db",
            "application/vnd.sqlite",
            [
                "oras",
                "push",
                "quay.io/test/repo:latest",
                "/tmp/test.db:application/vnd.sqlite",
                "--disable-path-validation",
            ],
        ),
        (
            "registry.example.com/myapp:v1.0",
            "/data/config.yaml",
            "application/vnd.yaml",
            [
                "oras",
                "push",
                "registry.example.com/myapp:v1.0",
                "/data/config.yaml:application/vnd.yaml",
                "--disable-path-validation",
            ],
        ),
        (
            "docker.io/library/nginx:latest",
            "/etc/nginx.conf",
            "application/vnd.config",
            [
                "oras",
                "push",
                "docker.io/library/nginx:latest",
                "/etc/nginx.conf:application/vnd.config",
                "--disable-path-validation",
            ],
        ),
    ],
)
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.oras_utils.run_cmd')
def test_push_oras_artifact_various_types(
    mock_run_cmd, mock_exists, artifact_ref, local_path, artifact_type, expected_cmd
):
    """Test artifact push with various artifact types."""
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
