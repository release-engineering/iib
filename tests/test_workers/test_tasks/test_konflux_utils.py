# SPDX-License-Identifier: GPL-3.0-or-later
import pytest
from unittest.mock import Mock, patch
from kubernetes.client.rest import ApiException

from iib.exceptions import IIBError
from iib.workers.tasks.konflux_utils import (
    find_pipelinerun,
    wait_for_pipeline_completion,
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
    """Test pipelinerun search with empty results."""
    # Setup
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_config = Mock()
    mock_config.iib_konflux_namespace = 'iib-tenant'
    mock_get_worker_config.return_value = mock_config

    mock_client.list_namespaced_custom_object.return_value = {"items": []}

    # Test
    result = find_pipelinerun("abc123")

    # Verify
    assert result == []


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
    wait_for_pipeline_completion("test-pipelinerun")

    # Verify
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
    wait_for_pipeline_completion("test-pipelinerun")

    # Verify
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
    mock_get_worker_config.return_value = mock_config

    mock_client.get_namespaced_custom_object.side_effect = exception

    # Test & Verify
    with pytest.raises(IIBError, match=expected_error):
        wait_for_pipeline_completion("test-pipelinerun")
