# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import time
from typing import List, Dict, Any, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_chain,
)

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config

__all__ = ['find_pipelinerun', 'wait_for_pipeline_completion', 'get_pipelinerun_image_url']

log = logging.getLogger(__name__)

# Global variables for Kubernetes client and configuration
_v1_client: Optional[client.CustomObjectsApi] = None


def _get_kubernetes_client() -> client.CustomObjectsApi:
    """
    Get or create a Kubernetes CustomObjectsApi client for cross-cluster access.

    :return: Configured Kubernetes CustomObjectsApi client
    :rtype: client.CustomObjectsApi
    :raises IIBError: If unable to create Kubernetes client or configuration is missing
    """
    global _v1_client

    if _v1_client is not None:
        return _v1_client

    try:
        _v1_client = _create_kubernetes_client()
        return _v1_client
    except IIBError:
        # Re-raise IIBError as-is (like CA certificate requirement)
        raise
    except Exception as e:
        # Log error without exposing sensitive information
        error_msg = f"Failed to initialize Kubernetes client: {type(e).__name__}"
        log.error(error_msg)
        raise IIBError(error_msg)


def _create_kubernetes_client() -> client.CustomObjectsApi:
    """
    Create a new Kubernetes client with cross-cluster configuration.

    :return: Configured Kubernetes CustomObjectsApi client
    :rtype: client.CustomObjectsApi
    :raises IIBError: If Konflux configuration is missing or invalid
    """
    worker_config = get_worker_config()

    # Get cross-cluster configuration (validation is done in config.py)
    target_cluster_url = getattr(worker_config, 'iib_konflux_cluster_url', None)
    target_cluster_token = getattr(worker_config, 'iib_konflux_cluster_token', None)
    target_cluster_ca_cert = getattr(worker_config, 'iib_konflux_cluster_ca_cert', None)

    # If no Konflux configuration is provided, raise an error
    if not target_cluster_url or not target_cluster_token or not target_cluster_ca_cert:
        raise IIBError(
            "Konflux configuration is required. Please set "
            "iib_konflux_cluster_url, iib_konflux_cluster_token, and "
            "iib_konflux_cluster_ca_cert in IIB worker configuration."
        )

    log.info("Configuring Kubernetes client for cross-cluster access to %s", target_cluster_url)

    configuration = _create_kubernetes_configuration(
        target_cluster_url, target_cluster_token, target_cluster_ca_cert
    )

    return client.CustomObjectsApi(client.ApiClient(configuration))


def _create_kubernetes_configuration(url: str, token: str, ca_cert: str) -> client.Configuration:
    """
    Create Kubernetes configuration with authentication and SSL settings.

    :param str url: The Kubernetes cluster API URL
    :param str token: The authentication token for the cluster
    :param str ca_cert: The CA certificate for SSL verification (file path or content)
    :return: Configured Kubernetes Configuration object
    :rtype: client.Configuration
    """
    configuration = client.Configuration()
    configuration.host = url
    configuration.api_key_prefix['authorization'] = 'Bearer'
    configuration.api_key['authorization'] = token

    # If CA cert is provided as a string, write it to a temp file
    if not os.path.isfile(ca_cert):
        import tempfile

        # TODO: Clean up the temp file once the request is completed
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crt') as f:
            f.write(ca_cert)
            ca_cert = f.name

    configuration.ssl_ca_cert = ca_cert
    return configuration


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(get_worker_config().iib_total_attempts),
    wait=wait_chain(wait_exponential(multiplier=get_worker_config().iib_retry_multiplier)),
)
def find_pipelinerun(commit_sha: str) -> List[Dict[str, Any]]:
    """
    Find the Konflux pipelinerun triggered by the git commit.

    This function will retry if no pipelineruns are found (empty list), as it may take
    a few seconds for the pipelinerun to start after a commit is pushed.

    :param str commit_sha: The git commit SHA to search for
    :return: List of pipelinerun objects matching the commit SHA
    :rtype: List[Dict[str, Any]]
    :raises IIBError: If there's an error fetching pipelineruns
        or no pipelineruns found after retries
    """
    try:
        log.info("Searching for pipelineruns with commit SHA: %s", commit_sha)

        v1_client = _get_kubernetes_client()
        worker_config = get_worker_config()
        namespace = worker_config.iib_konflux_namespace

        runs = v1_client.list_namespaced_custom_object(
            group="tekton.dev",
            version="v1",
            namespace=namespace,
            plural="pipelineruns",
            label_selector=f"pipelinesascode.tekton.dev/sha={commit_sha}",
        )

        items = runs.get("items", [])
        log.info("Found %s pipelinerun(s) for commit %s", len(items), commit_sha)

        if not items:
            raise IIBError(f"No pipelineruns found for commit {commit_sha}")

        return items

    except IIBError:
        # Re-raise IIBError without wrapping it (needed for retry decorator)
        raise
    except ApiException as e:
        error_msg = f"Failed to fetch pipelineruns for commit {commit_sha}: API error {e.status}"
        log.error("Kubernetes API error while fetching pipelineruns: %s - %s", e.status, e.reason)
        raise IIBError(error_msg)
    except Exception as e:
        error_msg = (
            f"Unexpected error while fetching pipelineruns for commit {commit_sha}: "
            f"{type(e).__name__}"
        )
        log.error("Unexpected error while fetching pipelineruns: %s", type(e).__name__)
        raise IIBError(error_msg)


def wait_for_pipeline_completion(
    pipelinerun_name: str, timeout: Optional[int] = None
) -> dict[str, Any]:
    """
    Poll the status of a tekton Pipelinerun and wait for completion.

    Handles all Tekton PipelineRun status reasons:
    - Success: Succeeded, Completed
    - Failure: Failed, PipelineRunTimeout, CreateRunFailed, status=False
    - Cancellation: Cancelled

    :param str pipelinerun_name: Name of the pipelinerun to monitor
    :param int timeout: Maximum time to wait in seconds (default: from config)
    :return: Dictionary containing the pipelinerun status information
    :rtype: Dict[str, Any]
    :raises IIBError: If the pipelinerun fails, is cancelled, or times out
    """
    if timeout is None:
        worker_config = get_worker_config()
        timeout = getattr(worker_config, 'iib_konflux_pipeline_timeout', 1800)

    log.info("Starting to monitor pipelinerun: %s", pipelinerun_name)
    start_time = time.time()

    while True:
        try:
            _check_timeout(pipelinerun_name, start_time, timeout)
            run = _fetch_pipelinerun_status(pipelinerun_name)

            if _handle_pipelinerun_completion(pipelinerun_name, run):
                return run

            time.sleep(30)

        except ApiException as e:
            error_msg = f"Failed to monitor pipelinerun {pipelinerun_name}: API error {e.status}"
            log.error(
                "Kubernetes API error while monitoring pipelinerun %s: %s - %s",
                pipelinerun_name,
                e.status,
                e.reason,
            )
            raise IIBError(error_msg)
        except IIBError as e:
            log.error("IIBError while monitoring pipelinerun %s: %s", pipelinerun_name, e)
            # Re-raise IIBError as-is
            raise
        except Exception as e:
            error_msg = (
                f"Unexpected error while monitoring pipelinerun {pipelinerun_name}: "
                f"{type(e).__name__}"
            )
            log.error(
                "Unexpected error while monitoring pipelinerun %s: %s",
                pipelinerun_name,
                type(e).__name__,
            )
            raise IIBError(error_msg)


def _check_timeout(pipelinerun_name: str, start_time: float, timeout: int) -> None:
    """
    Check if the timeout has been exceeded for pipelinerun monitoring.

    :param str pipelinerun_name: Name of the pipelinerun being monitored
    :param float start_time: The start time of monitoring (from time.time())
    :param int timeout: Maximum time to wait in seconds
    :raises IIBError: If the timeout has been exceeded
    """
    elapsed_time = time.time() - start_time
    if elapsed_time > timeout:
        raise IIBError(
            f"Timeout waiting for pipelinerun {pipelinerun_name} to complete "
            f"after {timeout} seconds"
        )


def get_pipelinerun_image_url(pipelinerun_name: str, run: Dict[str, Any]) -> str:
    """
    Extract IMAGE_URL from a completed pipelinerun's results.

    :param str pipelinerun_name: Name of the pipelinerun
    :param Dict[str, Any] run: The pipelinerun object
    :return: The IMAGE_URL value from the pipelinerun results
    :rtype: str
    :raises IIBError: If IMAGE_URL is not found in the pipelinerun results
    """
    status = run.get('status', {})

    # Check for 'results' (Konflux format) first, then fall back to 'pipelineResults' (older Tekton)
    pipeline_results = status.get('results', []) or status.get('pipelineResults', [])

    log.info("Found %d pipeline results for %s", len(pipeline_results), pipelinerun_name)

    for result in pipeline_results:
        if result.get('name') == 'IMAGE_URL':
            if image_url := result.get('value'):
                # Strip whitespace (including newlines) from the URL
                image_url = image_url.strip()
                log.info("Extracted IMAGE_URL from pipelinerun %s: %s", pipelinerun_name, image_url)
                return image_url

    # If not found, log for debugging
    log.error(
        "IMAGE_URL not found in pipelinerun %s. Available results: %s",
        pipelinerun_name,
        [r.get('name') for r in pipeline_results],
    )
    raise IIBError(f"IMAGE_URL not found in pipelinerun {pipelinerun_name} results")


def _fetch_pipelinerun_status(pipelinerun_name: str) -> Dict[str, Any]:
    """
    Fetch the current status of the pipelinerun from Kubernetes.

    :param str pipelinerun_name: Name of the pipelinerun to fetch
    :return: Dictionary containing the pipelinerun status information
    :rtype: Dict[str, Any]
    :raises ApiException: If there's an error accessing the Kubernetes API
    """
    v1_client = _get_kubernetes_client()
    worker_config = get_worker_config()
    namespace = worker_config.iib_konflux_namespace

    return v1_client.get_namespaced_custom_object(
        group="tekton.dev",
        version="v1",
        namespace=namespace,
        plural="pipelineruns",
        name=pipelinerun_name,
    )


def _handle_pipelinerun_completion(pipelinerun_name: str, run: Dict[str, Any]) -> bool:
    """
    Handle pipelinerun completion status and return True if completed.

    :return: True if pipelinerun completed (success or failure), False if still running
    :rtype: bool
    :raises IIBError: If the pipelinerun failed or was cancelled
    """
    status = run.get("status", {})
    conditions = status.get("conditions", [])

    if not conditions:
        log.info("Pipelinerun %s is still initializing...", pipelinerun_name)
        return False

    condition = conditions[0] if conditions else {}
    reason = condition.get("reason", "Unknown")
    condition_type = condition.get("type", "Unknown")
    status_value = condition.get("status", "Unknown")
    message = condition.get("message", "")

    log.info(
        "Pipelinerun %s status: reason=%s, type=%s, status=%s",
        pipelinerun_name,
        reason,
        condition_type,
        status_value,
    )
    if message:
        log.info("Pipelinerun %s message: %s", pipelinerun_name, message)

    if _is_pipelinerun_successful(reason):
        log.info("Pipelinerun %s completed successfully", pipelinerun_name)
        return True

    _is_pipelinerun_cancelled(reason, pipelinerun_name)
    _is_pipelinerun_failed(reason, status_value, message, pipelinerun_name)

    # Still running
    log.info("Pipelinerun %s is still running... (reason: %s)", pipelinerun_name, reason)
    return False


def _is_pipelinerun_successful(reason: str) -> bool:
    """
    Check if pipelinerun completed successfully.

    :param str reason: The reason from the pipelinerun condition
    :return: True if the pipelinerun completed successfully, False otherwise
    :rtype: bool
    """
    return reason in ("Succeeded", "Completed")


def _is_pipelinerun_failed(
    reason: str, status_value: str, message: str, pipelinerun_name: str
) -> None:
    """
    Check if pipelinerun failed and raise appropriate error.

    :param str reason: The reason from the pipelinerun condition
    :param str status_value: The status value from the pipelinerun condition
    :param str message: The message from the pipelinerun condition
    :param str pipelinerun_name: Name of the pipelinerun
    :raises IIBError: If the pipelinerun failed with appropriate error message
    """
    if reason in ("Failed", "PipelineRunTimeout", "CreateRunFailed"):
        error_msg = f"Pipelinerun {pipelinerun_name} failed"
        if reason == "PipelineRunTimeout":
            error_msg += " due to timeout"
        elif reason == "CreateRunFailed":
            error_msg += " due to resource creation failure"
        elif message:
            error_msg += f": {message}"
        raise IIBError(error_msg)

    if status_value == "False":
        error_msg = f"Pipelinerun {pipelinerun_name} failed"
        if message:
            error_msg += f": {message}"
        raise IIBError(error_msg)


def _is_pipelinerun_cancelled(reason: str, pipelinerun_name: str) -> None:
    """
    Check if pipelinerun was cancelled and raise appropriate error.

    :param str reason: The reason from the pipelinerun condition
    :param str pipelinerun_name: Name of the pipelinerun
    :raises IIBError: If the pipelinerun was cancelled
    """
    if reason == "Cancelled":
        raise IIBError(f"Pipelinerun {pipelinerun_name} was cancelled")
