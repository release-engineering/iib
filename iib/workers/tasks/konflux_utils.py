# SPDX-License-Identifier: GPL-3.0-or-later
"""
Konflux utilities for interacting with Tekton pipelineruns in OpenShift clusters.

This module provides functions to find and monitor Konflux pipelineruns triggered by git commits.
It supports cross-cluster access to OpenShift/Kubernetes clusters via IIB Worker configuration.
"""
import logging
import os
import time
from typing import List, Dict, Any, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config

__all__ = ['find_pipelinerun', 'wait_for_pipeline_completion']

log = logging.getLogger(__name__)

# Global variables for Kubernetes client and configuration
_v1_client: Optional[client.CustomObjectsApi] = None


def _get_kubernetes_client() -> client.CustomObjectsApi:
    """
    Get or create a Kubernetes CustomObjectsApi client for cross-cluster access.

    :return: Configured Kubernetes CustomObjectsApi client
    :raises IIBError: If unable to create Kubernetes client or configuration is missing
    """
    global _v1_client

    if _v1_client is not None:
        return _v1_client

    try:
        # Get configuration from IIB worker config
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

            log.info(
                f"Configuring Kubernetes client for cross-cluster access to {target_cluster_url}"
            )

        configuration = client.Configuration()
        configuration.host = target_cluster_url
        configuration.api_key_prefix['authorization'] = 'Bearer'
        configuration.api_key['authorization'] = target_cluster_token

        # If CA cert is provided as a string, write it to a temp file
        if not os.path.isfile(target_cluster_ca_cert):
            import tempfile

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crt') as f:
                f.write(target_cluster_ca_cert)
                target_cluster_ca_cert = f.name

        configuration.ssl_ca_cert = target_cluster_ca_cert

        _v1_client = client.CustomObjectsApi(client.ApiClient(configuration))
        return _v1_client

    except IIBError:
        # Re-raise IIBError as-is (like CA certificate requirement)
        raise
    except Exception as e:
        # Log error without exposing sensitive information
        log.error(f"Failed to initialize Kubernetes client: {type(e).__name__}")
        raise IIBError(f"Failed to initialize Kubernetes client: {type(e).__name__}")


def find_pipelinerun(commit_sha: str) -> List[Dict[str, Any]]:
    """
    Find the Konflux pipelinerun triggered by the git commit.

    :param str commit_sha: The git commit SHA to search for
    :return: List of pipelinerun objects matching the commit SHA
    :rtype: List[Dict[str, Any]]
    :raises IIBError: If there's an error fetching pipelineruns
    """
    try:
        log.info(f"Searching for pipelineruns with commit SHA: {commit_sha}")

        v1_client = _get_kubernetes_client()
        worker_config = get_worker_config()
        namespace = getattr(worker_config, 'iib_konflux_namespace', 'iib-tenant')

        runs = v1_client.list_namespaced_custom_object(
            group="tekton.dev",
            version="v1",
            namespace=namespace,
            plural="pipelineruns",
            label_selector=f"pipelinesascode.tekton.dev/sha={commit_sha}",
        )

        items = runs.get("items", [])
        log.info(f"Found {len(items)} pipelinerun(s) for commit {commit_sha}")

        return items

    except ApiException as e:
        log.error(f"Kubernetes API error while fetching pipelineruns: {e.status} - {e.reason}")
        raise IIBError(
            f"Failed to fetch pipelineruns for commit {commit_sha}: API error {e.status}"
        )
    except Exception as e:
        log.error(f"Unexpected error while fetching pipelineruns: {type(e).__name__}")
        raise IIBError(
            f"Unexpected error while fetching pipelineruns for commit {commit_sha}: "
            f"{type(e).__name__}"
        )


def wait_for_pipeline_completion(pipelinerun_name: str, timeout: int = 1800) -> None:
    """
    Poll the status of a tekton Pipelinerun and wait for completion.

    Handles all Tekton PipelineRun status reasons:
    - Success: Succeeded, Completed
    - Failure: Failed, PipelineRunTimeout, CreateRunFailed, status=False
    - Cancellation: Cancelled

    :param str pipelinerun_name: Name of the pipelinerun to monitor
    :param int timeout: Maximum time to wait in seconds (default: 1800 = 30 mins)
    :raises IIBError: If the pipelinerun fails, is cancelled, or times out
    """
    log.info(f"Starting to monitor pipelinerun: {pipelinerun_name}")
    start_time = time.time()

    while True:
        try:
            # Check if we've exceeded the timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise IIBError(
                    f"Timeout waiting for pipelinerun {pipelinerun_name} to complete "
                    f"after {timeout} seconds"
                )

            # Fetch the current status of the pipelinerun
            v1_client = _get_kubernetes_client()
            worker_config = get_worker_config()
            namespace = getattr(worker_config, 'iib_konflux_namespace', 'iib-tenant')

            run = v1_client.get_namespaced_custom_object(
                group="tekton.dev",
                version="v1",
                namespace=namespace,
                plural="pipelineruns",
                name=pipelinerun_name,
            )

            # Extract status information
            status = run.get("status", {})
            conditions = status.get("conditions", [])

            if not conditions:
                log.info(f"Pipelinerun {pipelinerun_name} is still initializing...")
                time.sleep(30)
                continue

            # Get the condition (there's typically only one condition object for PipelineRuns)
            condition = conditions[0] if conditions else {}
            reason = condition.get("reason", "Unknown")
            condition_type = condition.get("type", "Unknown")
            status = condition.get("status", "Unknown")
            message = condition.get("message", "")

            log.info(
                f"Pipelinerun {pipelinerun_name} status: reason={reason}, "
                f"type={condition_type}, status={status}"
            )
            if message:
                log.info(f"Pipelinerun {pipelinerun_name} message: {message}")

            # Check if the pipelinerun has completed based on Tekton status documentation
            # https://tekton.dev/docs/pipelines/pipelineruns/#monitoring-execution-status
            # Success cases
            if reason in ("Succeeded", "Completed"):
                log.info(f"Pipelinerun {pipelinerun_name} completed successfully")
                return

            # Failure cases
            elif reason in ("Failed", "PipelineRunTimeout", "CreateRunFailed"):
                error_msg = f"Pipelinerun {pipelinerun_name} failed"
                if reason == "PipelineRunTimeout":
                    error_msg += " due to timeout"
                elif reason == "CreateRunFailed":
                    error_msg += " due to resource creation failure"
                elif message:
                    error_msg += f": {message}"
                raise IIBError(error_msg)

            # Cancellation cases
            elif reason == "Cancelled":
                raise IIBError(f"Pipelinerun {pipelinerun_name} was cancelled")

            # Check for error status (False status indicates failure)
            elif status == "False":
                error_msg = f"Pipelinerun {pipelinerun_name} failed"
                if message:
                    error_msg += f": {message}"
                raise IIBError(error_msg)

            # Still running, wait before next check
            log.info(f"Pipelinerun {pipelinerun_name} is still running... (reason: {reason})")
            time.sleep(30)

        except ApiException as e:
            log.error(
                f"Kubernetes API error while monitoring pipelinerun {pipelinerun_name}: "
                f"{e.status} - {e.reason}"
            )
            raise IIBError(
                f"Failed to monitor pipelinerun {pipelinerun_name}: API error {e.status}"
            )
        except IIBError:
            # Re-raise IIBError as-is
            raise
        except Exception as e:
            log.error(
                f"Unexpected error while monitoring pipelinerun {pipelinerun_name}: "
                f"{type(e).__name__}"
            )
            raise IIBError(
                f"Unexpected error while monitoring pipelinerun {pipelinerun_name}: "
                f"{type(e).__name__}"
            )
