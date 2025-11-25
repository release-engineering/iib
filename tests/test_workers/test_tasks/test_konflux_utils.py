# SPDX-License-Identifier: GPL-3.0-or-later
import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from kubernetes.client.rest import ApiException
from kubernetes import client

from iib.exceptions import IIBError
from iib.workers.tasks.konflux_utils import (
    find_pipelinerun,
    wait_for_pipeline_completion,
    get_pipelinerun_image_url,
    _get_kubernetes_client,
    _create_kubernetes_client,
    _create_kubernetes_configuration,
)


def setup_function():
    """Reset the global client before each test."""
    import iib.workers.tasks.konflux_utils

    iib.workers.tasks.konflux_utils._v1_client = None


@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_find_pipelinerun_success(mock_get_worker_config, mock_get_client):
    """Test successful pipelinerun search."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    expected_runs = {
        "items": [{"metadata": {"name": "pipelinerun-1"}}, {"metadata": {"name": "pipelinerun-2"}}]
    }
    mock_client.list_namespaced_custom_object.return_value = expected_runs

    # Test
    result = find_pipelinerun("abc123")

    # Verify
    assert result == expected_runs["items"]
    mock_client.list_namespaced_custom_object.assert_called_once_with(
        group="tekton.dev",
        version="v1",
        namespace="iib-tenant",
        plural="pipelineruns",
        label_selector="pipelinesascode.tekton.dev/sha=abc123",
    )


@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_find_pipelinerun_empty_result(mock_get_worker_config, mock_get_client):
    """Test pipelinerun search with empty results raises IIBError for retry."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_config.iib_total_attempts = 3  # Reduced to make test faster
    mock_config.iib_retry_multiplier = 1  # Reduced to make test faster
    mock_get_worker_config.return_value = mock_config

    mock_client.list_namespaced_custom_object.return_value = {"items": []}

    # Test & Verify - should raise IIBError to trigger retry decorator
    with pytest.raises(IIBError, match="No pipelineruns found for commit abc123"):
        find_pipelinerun("abc123")


@pytest.mark.parametrize(
    "exception,expected_error",
    [
        (
            ApiException(status=401, reason="Unauthorized"),
            "Failed to fetch pipelineruns for commit abc123: API error 401",
        ),
        (
            Exception("Network error"),
            "Unexpected error while fetching pipelineruns for commit abc123: Exception",
        ),
    ],
)
@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_find_pipelinerun_exceptions(
    mock_get_worker_config, mock_get_client, exception, expected_error
):
    """Test pipelinerun search with various exceptions."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    mock_client.list_namespaced_custom_object.side_effect = exception

    # Test & Verify
    with pytest.raises(IIBError, match=expected_error):
        find_pipelinerun("abc123")


@pytest.mark.parametrize(
    "reason,condition_type,status,should_succeed",
    [
        ("Succeeded", "Succeeded", "True", True),
        ("Completed", "Succeeded", "True", True),
    ],
)
@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_wait_for_pipeline_completion_success_cases(
    mock_get_worker_config, mock_get_client, reason, condition_type, status, should_succeed
):
    """Test waiting for pipelinerun completion with success scenarios."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    run_status = {
        "status": {
            "conditions": [
                {
                    "reason": reason,
                    "type": condition_type,
                    "status": status,
                    "message": f"Tasks completed with {reason}",
                }
            ]
        }
    }
    mock_client.get_namespaced_custom_object.return_value = run_status

    # Test
    result = wait_for_pipeline_completion("test-pipelinerun")

    # Verify
    assert result == run_status
    mock_client.get_namespaced_custom_object.assert_called_once_with(
        group="tekton.dev",
        version="v1",
        namespace="iib-tenant",
        plural="pipelineruns",
        name="test-pipelinerun",
    )


@pytest.mark.parametrize(
    "reason,expected_error_msg",
    [
        ("Failed", "Pipelinerun test-pipelinerun failed"),
        ("PipelineRunTimeout", "Pipelinerun test-pipelinerun failed due to timeout"),
        ("CreateRunFailed", "Pipelinerun test-pipelinerun failed due to resource creation failure"),
        ("Cancelled", "Pipelinerun test-pipelinerun was cancelled"),
    ],
)
@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_wait_for_pipeline_completion_failure_cases(
    mock_get_worker_config, mock_get_client, reason, expected_error_msg
):
    """Test waiting for pipelinerun completion with failure scenarios."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    run_status = {
        "status": {
            "conditions": [
                {
                    "reason": reason,
                    "type": "Succeeded",
                    "status": "False" if reason != "Cancelled" else "False",
                    "message": f"PipelineRun {reason.lower()}",
                }
            ]
        }
    }
    mock_client.get_namespaced_custom_object.return_value = run_status

    # Test & Verify
    with pytest.raises(IIBError, match=expected_error_msg):
        wait_for_pipeline_completion("test-pipelinerun")


@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_wait_for_pipeline_completion_status_false(mock_get_worker_config, mock_get_client):
    """Test waiting for pipelinerun completion with status=False."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    run_status = {
        "status": {
            "conditions": [
                {
                    "reason": "SomeError",
                    "type": "Succeeded",
                    "status": "False",
                    "message": "Some error occurred",
                }
            ]
        }
    }
    mock_client.get_namespaced_custom_object.return_value = run_status

    # Test & Verify
    with pytest.raises(IIBError, match="Pipelinerun test-pipelinerun failed: Some error occurred"):
        wait_for_pipeline_completion("test-pipelinerun")


@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
@patch('iib.workers.tasks.konflux_utils.time.sleep')
def test_wait_for_pipeline_completion_still_running(
    mock_sleep, mock_get_worker_config, mock_get_client
):
    """Test waiting for pipelinerun completion when still running."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    # First call: still running, second call: succeeded
    run_status_running = {
        "status": {
            "conditions": [
                {
                    "reason": "Running",
                    "type": "Succeeded",
                    "status": "Unknown",
                    "message": "PipelineRun is running",
                }
            ]
        }
    }
    run_status_succeeded = {
        "status": {
            "conditions": [
                {
                    "reason": "Succeeded",
                    "type": "Succeeded",
                    "status": "True",
                    "message": "Tasks completed successfully",
                }
            ]
        }
    }
    mock_client.get_namespaced_custom_object.side_effect = [
        run_status_running,
        run_status_succeeded,
    ]

    # Test
    result = wait_for_pipeline_completion("test-pipelinerun")

    # Verify
    assert result == run_status_succeeded
    assert mock_client.get_namespaced_custom_object.call_count == 2
    mock_sleep.assert_called_once_with(30)


@pytest.mark.parametrize(
    "timeout_scenario,time_values,expected_error",
    [
        (
            "timeout_exceeded",
            [0, 1801],
            "Timeout waiting for pipelinerun test-pipelinerun to complete after 1800 seconds",
        ),
        (
            "no_conditions",
            [0, 30, 1801],
            "Timeout waiting for pipelinerun test-pipelinerun to complete after 1800 seconds",
        ),
    ],
)
@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
@patch('iib.workers.tasks.konflux_utils.time.sleep')
@patch('iib.workers.tasks.konflux_utils.time.time')
@patch('iib.workers.tasks.konflux_utils.log')
def test_wait_for_pipeline_completion_timeout_scenarios(
    mock_log,
    mock_time,
    mock_sleep,
    mock_get_worker_config,
    mock_get_client,
    timeout_scenario,
    time_values,
    expected_error,
):
    """Test waiting for pipelinerun completion with timeout scenarios."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    # Mock time to simulate timeout
    mock_time.side_effect = time_values

    if timeout_scenario == "timeout_exceeded":
        run_status = {
            "status": {
                "conditions": [
                    {
                        "reason": "Running",
                        "type": "Succeeded",
                        "status": "Unknown",
                        "message": "PipelineRun is running",
                    }
                ]
            }
        }
    else:  # no_conditions
        run_status = {"status": {"conditions": []}}

    mock_client.get_namespaced_custom_object.return_value = run_status

    # Test & Verify
    with pytest.raises(IIBError, match=expected_error):
        wait_for_pipeline_completion("test-pipelinerun", timeout=1800)


@pytest.mark.parametrize(
    "exception,expected_error",
    [
        (
            ApiException(status=404, reason="Not Found"),
            "Failed to monitor pipelinerun test-pipelinerun: API error 404",
        ),
        (
            Exception("Network error"),
            "Unexpected error while monitoring pipelinerun test-pipelinerun: Exception",
        ),
    ],
)
@patch('iib.workers.tasks.konflux_utils._get_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_wait_for_pipeline_completion_exceptions(
    mock_get_worker_config, mock_get_client, exception, expected_error
):
    """Test waiting for pipelinerun completion with various exceptions."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_config.iib_konflux_pipeline_timeout = 1800
    mock_get_worker_config.return_value = mock_config

    mock_client.get_namespaced_custom_object.side_effect = exception

    # Test & Verify
    with pytest.raises(IIBError, match=expected_error):
        wait_for_pipeline_completion("test-pipelinerun")


@patch('iib.workers.tasks.konflux_utils._create_kubernetes_client')
def test_get_kubernetes_client_caching(mock_create_client):
    """Test that _get_kubernetes_client caches the client."""
    # Setup
    mock_client = Mock()
    mock_create_client.return_value = mock_client

    # Reset global client
    import iib.workers.tasks.konflux_utils

    iib.workers.tasks.konflux_utils._v1_client = None

    # Test - first call should create client
    result1 = _get_kubernetes_client()
    assert result1 == mock_client
    assert mock_create_client.call_count == 1

    # Test - second call should return cached client
    result2 = _get_kubernetes_client()
    assert result2 == mock_client
    assert mock_create_client.call_count == 1  # Should not be called again


@pytest.mark.parametrize(
    "exception,expected_error,should_log",
    [
        (IIBError("Original IIBError message"), "Original IIBError message", False),
        (
            ValueError("Some unexpected error"),
            "Failed to initialize Kubernetes client: ValueError",
            True,
        ),
    ],
)
@patch('iib.workers.tasks.konflux_utils._create_kubernetes_client')
@patch('iib.workers.tasks.konflux_utils.log')
def test_get_kubernetes_client_exception_handling(
    mock_log, mock_create_client, exception, expected_error, should_log
):
    """Test that _get_kubernetes_client handles different types of exceptions."""
    # Setup
    mock_create_client.side_effect = exception

    # Reset global client
    import iib.workers.tasks.konflux_utils

    iib.workers.tasks.konflux_utils._v1_client = None

    # Test & Verify
    with pytest.raises(IIBError, match=expected_error) as exc_info:
        _get_kubernetes_client()

    # For IIBError, ensure it's the same exception object (re-raised, not wrapped)
    if isinstance(exception, IIBError):
        assert exc_info.value is exception

    # Verify logging only for general exceptions
    if should_log:
        mock_log.error.assert_called_once_with("Failed to initialize Kubernetes client: ValueError")
    else:
        mock_log.error.assert_not_called()


@pytest.mark.parametrize(
    "url,token,ca_cert,description",
    [
        (None, 'test-token', '/path/to/ca.crt', 'missing URL'),
        ('https://api.example.com:6443', None, '/path/to/ca.crt', 'missing token'),
        ('https://api.example.com:6443', 'test-token', None, 'missing CA cert'),
    ],
)
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
def test_create_kubernetes_client_missing_config(
    mock_get_worker_config, url, token, ca_cert, description
):
    """Test _create_kubernetes_client with missing configuration."""
    # Setup
    mock_config = Mock()
    mock_config.iib_konflux_cluster_url = url
    mock_config.iib_konflux_cluster_token = token
    mock_config.iib_konflux_cluster_ca_cert = ca_cert
    mock_get_worker_config.return_value = mock_config

    # Test & Verify
    with pytest.raises(IIBError, match="Konflux configuration is required"):
        _create_kubernetes_client()


@patch('iib.workers.tasks.konflux_utils.client.CustomObjectsApi')
@patch('iib.workers.tasks.konflux_utils._create_kubernetes_configuration')
@patch('iib.workers.tasks.konflux_utils.get_worker_config')
@patch('iib.workers.tasks.konflux_utils.log')
def test_create_kubernetes_client_success(
    mock_log, mock_get_worker_config, mock_create_config, mock_custom_objects_api
):
    """Test successful _create_kubernetes_client."""
    # Setup
    mock_config = Mock()
    mock_config.iib_konflux_cluster_url = 'https://api.example.com:6443'
    mock_config.iib_konflux_cluster_token = 'test-token'
    mock_config.iib_konflux_cluster_ca_cert = '/path/to/ca.crt'
    mock_get_worker_config.return_value = mock_config

    # Create a real configuration object to avoid mock issues
    from kubernetes import client

    real_config = client.Configuration()
    real_config.host = 'https://api.example.com:6443'
    real_config.api_key_prefix['authorization'] = 'Bearer'
    real_config.api_key['authorization'] = 'test-token'
    real_config.ssl_ca_cert = '/path/to/ca.crt'
    mock_create_config.return_value = real_config

    mock_client = Mock()
    mock_custom_objects_api.return_value = mock_client

    # Test
    result = _create_kubernetes_client()

    # Verify
    assert result == mock_client
    mock_create_config.assert_called_once_with(
        'https://api.example.com:6443', 'test-token', '/path/to/ca.crt'
    )
    mock_custom_objects_api.assert_called_once()
    mock_log.info.assert_called_once_with(
        "Configuring Kubernetes client for cross-cluster access to %s",
        'https://api.example.com:6443',
    )


def test_create_kubernetes_configuration_with_file_path():
    """Test _create_kubernetes_configuration with existing file path."""
    # Setup
    with tempfile.NamedTemporaryFile(mode='w', suffix='.crt', delete=False) as f:
        f.write('test-cert-content')
        ca_cert_path = f.name

    try:
        # Test
        config = _create_kubernetes_configuration(
            'https://api.example.com:6443', 'test-token', ca_cert_path
        )

        # Verify
        assert isinstance(config, client.Configuration)
        assert config.host == 'https://api.example.com:6443'
        assert config.api_key_prefix['authorization'] == 'Bearer'
        assert config.api_key['authorization'] == 'test-token'
        assert config.ssl_ca_cert == ca_cert_path

    finally:
        # Cleanup
        os.unlink(ca_cert_path)


@pytest.mark.parametrize(
    "ca_cert_input,expected_ssl_ca_cert,should_create_temp_file,description",
    [
        ('test-cert-content-as-string', '/tmp/temp_cert_123.crt', True, 'string content'),
        ('/existing/path/ca.crt', '/existing/path/ca.crt', False, 'existing file path'),
    ],
)
@patch('iib.workers.tasks.konflux_utils.os.path.isfile')
@patch('tempfile.NamedTemporaryFile')
def test_create_kubernetes_configuration_ca_cert_handling(
    mock_tempfile,
    mock_isfile,
    ca_cert_input,
    expected_ssl_ca_cert,
    should_create_temp_file,
    description,
):
    """Test _create_kubernetes_configuration with different CA cert scenarios."""
    # Setup
    mock_isfile.return_value = not should_create_temp_file

    if should_create_temp_file:
        mock_temp_file = Mock()
        mock_temp_file.name = '/tmp/temp_cert_123.crt'
        mock_tempfile.return_value.__enter__.return_value = mock_temp_file

    # Test
    config = _create_kubernetes_configuration(
        'https://api.example.com:6443', 'test-token', ca_cert_input
    )

    # Verify
    assert isinstance(config, client.Configuration)
    assert config.host == 'https://api.example.com:6443'
    assert config.api_key_prefix['authorization'] == 'Bearer'
    assert config.api_key['authorization'] == 'test-token'
    assert config.ssl_ca_cert == expected_ssl_ca_cert

    # Verify file existence check
    mock_isfile.assert_called_once_with(ca_cert_input)

    # Verify temp file creation if needed
    if should_create_temp_file:
        mock_tempfile.assert_called_once_with(mode='w', delete=False, suffix='.crt')
        mock_temp_file.write.assert_called_once_with(ca_cert_input)
    else:
        mock_tempfile.assert_not_called()


@pytest.mark.parametrize(
    "results_key,image_url,description",
    [
        ('results', 'quay.io/namespace/image:tag', 'Konflux format with results key'),
        (
            'pipelineResults',
            'quay.io/namespace/image:tag',
            'Older Tekton format with pipelineResults',
        ),
    ],
)
def test_get_pipelinerun_image_url_success(results_key, image_url, description):
    """Test successful extraction of IMAGE_URL from pipelinerun."""
    # Setup
    run = {
        'status': {
            results_key: [
                {'name': 'IMAGE_DIGEST', 'value': 'sha256:abc123'},
                {'name': 'IMAGE_URL', 'value': image_url},
                {'name': 'CHAINS-GIT_COMMIT', 'value': 'def456'},
            ]
        }
    }

    # Test
    result = get_pipelinerun_image_url('test-pipelinerun', run)

    # Verify
    assert result == image_url


def test_get_pipelinerun_image_url_with_whitespace():
    """Test IMAGE_URL extraction strips whitespace."""
    # Setup
    run = {
        'status': {
            'results': [
                {'name': 'IMAGE_URL', 'value': '  quay.io/namespace/image:tag\n  '},
            ]
        }
    }

    # Test
    result = get_pipelinerun_image_url('test-pipelinerun', run)

    # Verify
    assert result == 'quay.io/namespace/image:tag'


def test_get_pipelinerun_image_url_fallback_to_pipelineresults():
    """Test fallback from results to pipelineResults."""
    # Setup - 'results' is empty but 'pipelineResults' has data
    run = {
        'status': {
            'results': [],
            'pipelineResults': [
                {'name': 'IMAGE_URL', 'value': 'quay.io/namespace/image:tag'},
            ],
        }
    }

    # Test
    result = get_pipelinerun_image_url('test-pipelinerun', run)

    # Verify
    assert result == 'quay.io/namespace/image:tag'


@pytest.mark.parametrize(
    "run,description",
    [
        (
            {
                'status': {
                    'results': [
                        {'name': 'IMAGE_DIGEST', 'value': 'sha256:abc123'},
                        {'name': 'CHAINS-GIT_COMMIT', 'value': 'def456'},
                    ]
                }
            },
            'IMAGE_URL not in results',
        ),
        (
            {
                'status': {
                    'results': [
                        {'name': 'IMAGE_URL', 'value': ''},
                    ]
                }
            },
            'IMAGE_URL has empty value',
        ),
        ({'status': {}}, 'no results key present'),
        (
            {'status': {'results': [], 'pipelineResults': []}},
            'both results and pipelineResults empty',
        ),
    ],
)
def test_get_pipelinerun_image_url_error_cases(run, description):
    """Test error cases when IMAGE_URL is not found or invalid."""
    # Test & Verify
    with pytest.raises(
        IIBError, match='IMAGE_URL not found in pipelinerun test-pipelinerun results'
    ):
        get_pipelinerun_image_url('test-pipelinerun', run)
