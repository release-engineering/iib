# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains functions for saving changes to Git."""
import logging
import os
import tempfile
import shutil
from typing import Dict, Optional, Tuple

from operator_manifest.operator import ImageName
import requests
from urllib.parse import urlparse, quote_plus

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import requests_session
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import run_cmd


log = logging.getLogger(__name__)


@instrument_tracing(span_name="workers.tasks.git_utils.push_configs_to_git")
def push_configs_to_git(
    request_id: int,
    from_index: str,
    src_configs_path: str,
    index_repo_map: Dict[str, str],
    commit_message: Optional[str] = None,
) -> None:
    """
    Pushes /configs subfolders to a Git repository.

    :param int request_id: The ID of the IIB request.
    :param str from_index: The from_index pullspec. Note: This should have a tag that
                           corresponds to the OCP version, like 'v4.19'. The code
                           assumes that the branch already exists in remote repo.
    :param str src_configs_path: Path to /configs folder where <pkg>/catalog.json
                           files reside.
    :param dict(str) index_repo_map: The repo mapping to resolve the git URL.
    :param str commit_message: Custom commit message. If None, a default message is used.
    :raises IIBError: If src_configs_path is not found, remote branch does not exist,
                      or a Git operation fails.
    """
    # Determine Git repo from pullspec
    index_image = ImageName.parse(from_index)
    branch = index_image.tag
    repo_url = resolve_git_url(from_index, index_repo_map)

    # Do not proceed to store with git if the image is not present in the mappings
    if not repo_url:
        log.info(f"Aborting git storage: no repository set for {from_index}.")
        return

    # Retrieve the repo auth token
    git_token_name, git_token = get_git_token(repo_url)

    # Validate branch
    remote_branch_status = run_cmd(["git", "ls-remote", "--heads", repo_url, branch], strict=False)
    if not remote_branch_status.strip():
        raise IIBError(f"Remote branch '{branch}' not found for repo {repo_url}")

    # Verify the path to existing /configs is correct
    if not os.path.exists(src_configs_path):
        raise IIBError(f"Catalog configs directory does not exist: {src_configs_path}")

    # Make sure there are subdirs under /configs for the operator package dirs
    operator_packages = os.listdir(src_configs_path)
    if not operator_packages:
        raise IIBError(f"No packages found in configs directory {src_configs_path}")

    # Clone/checkout remote repo in temp dir
    with tempfile.TemporaryDirectory(prefix=f"git-repo-{request_id}-") as local_repo_dir:
        log.info("Cloning repo to %s", local_repo_dir)
        try:
            clone_git_repo(repo_url, branch, git_token_name, git_token, local_repo_dir)

            # Configure Git user for commits
            configure_git_user(local_repo_dir)

            # Copy configs/ subdirs to local Git repo
            repo_configs_dir = os.path.join(local_repo_dir, 'configs')
            log.info(
                "Copying content of %s to local Git repository %s",
                src_configs_path,
                repo_configs_dir,
            )
            for operator_package in operator_packages:
                src_pkg_dir = os.path.join(src_configs_path, operator_package)
                dest_pkg_dir = os.path.join(repo_configs_dir, operator_package)
                os.makedirs(dest_pkg_dir, exist_ok=True)
                shutil.copytree(src_pkg_dir, dest_pkg_dir, dirs_exist_ok=True)

            # Print git status to the logs
            git_status = run_cmd(
                ["git", "-C", local_repo_dir, "status"], exc_msg="Error getting git status"
            )
            log.info(git_status)

            # Add updates
            log.info("Commiting changes to local Git repository.")
            run_cmd(
                ["git", "-C", local_repo_dir, "add", "."], exc_msg="Error staging changes to git"
            )
            git_status = run_cmd(
                ["git", "-C", local_repo_dir, "status"], exc_msg="Error getting git status"
            )
            log.info(git_status)
            commit_and_push(
                request_id,
                local_repo_dir,
                repo_url,
                branch,
                commit_message,
            )
        finally:
            # tempfile should have done this already, but just in case
            if os.path.exists(local_repo_dir):
                shutil.rmtree(local_repo_dir)
                log.debug("Cleaned up local Git repository %s", local_repo_dir)


def commit_and_push(
    request_id: int,
    local_repo_path: str,
    repo_url: str,
    branch: str,
    commit_message: Optional[str] = None,
) -> None:
    """
    Commit and push locally staged changes to remote Git repo.

    :param int request_id: The ID of the IIB request.
    :param str local_repo_path: Path to local git repository where changes have been staged.
    :param str repo_url: Git repository URL.
    :param str branch: Branch name to push changes to.
    :param str commit_message: Custom commit message. If None, a default message is used.
    :raises IIBError: If a Git operation fails.
    """
    # Commit updates
    final_commit_message = commit_message or (
        f"IIB: Update for request id {request_id} (overwrite_from_index)"
    )
    commit_output = run_cmd(
        ["git", "-C", local_repo_path, "commit", "-m", final_commit_message],
        exc_msg="Error committing changes",
    )
    log.info(commit_output)

    # Push updates
    log.info("Pushing changes to %s branch of %s", branch, repo_url)
    push_output = run_cmd(
        ["git", "-C", local_repo_path, "push", "origin", branch],
        exc_msg=f"Error pushing changes to git repo {repo_url}",
    )
    log.info(push_output)


def resolve_git_url(from_index, index_repo_map: Dict[str, str]) -> Optional[str]:
    """
    Get Git repository URL from iib_web_index_to_gitlab_push_map.

    :param str from_index: from_index image pull spec.
    :param dict(str) index_repo_map: The repo mapping to resolve the git URL.
    :return: Git URL.
    :rtype: str
    """
    index_image = ImageName.parse(from_index)
    index_no_tag = f"{index_image.registry}/{index_image.namespace}/{index_image.repo}"
    git_url = index_repo_map.get(index_no_tag, None)
    if not git_url:
        log.warning(f"Missing key '{index_no_tag}' in 'iib_web_index_to_gitlab_push_map'")
    return git_url


def get_git_token(git_repo) -> Tuple[str, str]:
    """
    Get Git repository token from iib_index_configs_gitlab_tokens_map.

    :param str git_repo: Git repository URL.
    :return: token name, token value.
    :rtype: Tuple[str, str]
    :raises IIBError: If no token found for Git repository.
    """
    git_token_map = get_worker_config()['iib_index_configs_gitlab_tokens_map']
    if git_repo not in git_token_map:
        raise IIBError(f"Missing key '{git_repo}' in 'iib_index_configs_gitlab_tokens_map'")
    str_token_name_value = git_token_map[git_repo]
    splitted_token = str_token_name_value.split(":")
    if ":" not in str_token_name_value or '' in splitted_token[:2]:
        raise IIBError(
            f"Invalid token format for '{git_repo}' in 'iib_index_configs_gitlab_tokens_map'. "
            "Expected 'token_name:token_value'."
        )
    token_name, token_value = splitted_token[:2]
    return token_name, token_value


def clone_git_repo(
    repo_url: str, branch: str, token_name: str, token: str, local_repo_path: str
) -> None:
    """
    Clone Git repository and perform checkout.

    In order to make effective use of space:
    - The origin is set to track only a single branch corresponding to the OCP version.
    - Shallow clone with depth of 1 used to retrieve only the most recent commit.

    :param str repo_url: Git repo URL.
    :param str branch: Branch name corresponding to OCP version, like "v4.19".
    :param str token_name: Name of Git repository token.
    :param str token: Value of Git repository token.
    :param str local_repo_path: The local path where the Git repo will be cloned.
    """
    base_url = repo_url.replace('https://', '')
    remote_url = f"https://{token_name}:{token}@{base_url}"

    clone_output = run_cmd(
        ["git", "clone", "--depth", "1", "--branch", branch, remote_url, local_repo_path],
        exc_msg=f"Error cloning remote repository for {repo_url}",
    )
    log.info(clone_output)

    # Show most recent commit
    last_commit = run_cmd(
        ["git", "-C", local_repo_path, "log", "-n1"],
        exc_msg=f"Error displaying last commit for {repo_url}",
    )
    log.info("Most recent commit: %s", last_commit)


def configure_git_user(
    local_repo_path: str,
    user_name: Optional[str] = "IIB Worker",
    email_address: Optional[str] = "iib-worker@redhat.com",
):
    """
    Configure git user name and email displayed in commit message.

    :param str local_repo_path: Path to local Git repo.
    :param str user_name: User name for local Git repo.
    :param str email_address: Email address for local Git repo.
    """
    run_cmd(
        ["git", "-C", local_repo_path, "config", "--local", "user.name", str(user_name)],
        exc_msg="Error configuring git user.email",
    )
    run_cmd(
        ["git", "-C", local_repo_path, "config", "--local", "user.email", str(email_address)],
        exc_msg="Error configuring git user.email",
    )


def revert_last_commit(
    request_id: int,
    from_index: str,
    index_repo_map: Dict[str, str],
) -> None:
    """
    Revert the last commit and push to remote Git repo.

    :param int request_id: The ID of the IIB request.
    :param str from_index: The from_index pullspec.
    :param dict(str) index_repo_map: The repo mapping to resolve the git URL.
    """
    index_image = ImageName.parse(from_index)
    branch = index_image.tag
    repo_url = resolve_git_url(from_index, index_repo_map)

    # Do not proceed to store with git if the image is not present in the mappings
    if not repo_url:
        log.info(f"Aborting git revert: no repository set for {from_index}.")
        return

    # Get repo auth token
    git_token_name, git_token = get_git_token(repo_url)

    with tempfile.TemporaryDirectory(prefix=f"git-repo-{request_id}-") as local_repo_dir:
        log.info("Cloning repo to %s", local_repo_dir)
        try:
            clone_git_repo(repo_url, branch, git_token_name, git_token, local_repo_dir)

            # Configure Git user for commits
            configure_git_user(local_repo_dir)

            log.info("Reverting last commit to %s branch of %s", branch, repo_url)
            revert_output = run_cmd(
                ["git", "-C", local_repo_dir, "reset", "--hard", "HEAD~1"],
                exc_msg="Error resetting last commit",
            )
            log.info(revert_output)

            log.info("Pushing 1-commit reverted %s branch of %s", branch, repo_url)
            force_push_output = run_cmd(
                ["git", "-C", local_repo_dir, "push", "--force", "origin", branch],
                exc_msg=f"Error pushing changes to git repo {repo_url}",
            )
            log.info(force_push_output)
        finally:
            # tempfile should have done this already, but just in case
            if os.path.exists(local_repo_dir):
                shutil.rmtree(local_repo_dir)
                log.debug("Cleaned up local Git repository %s", local_repo_dir)


@instrument_tracing(span_name="workers.tasks.git_utils.create_mr")
def create_mr(
    request_id: int,
    local_repo_path: str,
    repo_url: str,
    branch: str,
    commit_message: Optional[str] = None,
) -> Dict[str, str]:
    """
    Create a merge request on GitLab repository.

    :param int request_id: The ID of the IIB request.
    :param str local_repo_path: Path to local git repository where changes have been staged.
    :param str repo_url: Git repository URL.
    :param str branch: Branch name corresponding to OCP version, like "v4.19".
    :param str commit_message: Custom commit message. If None, a default message is used.
    :return: Dictionary containing MR details (mr_id, mr_url, source_branch).
    :rtype: Dict[str, str]
    :raises IIBError: If a Git operation or GitLab API call fails.
    """
    # Get GitLab token for API access
    _, git_token = get_git_token(repo_url)

    # Create a feature branch for the MR
    feature_branch = f"iib-request-{request_id}-{branch}"

    # Create and switch to feature branch
    log.info("Creating feature branch %s", feature_branch)
    run_cmd(
        ["git", "-C", local_repo_path, "checkout", "-b", feature_branch],
        exc_msg="Error creating feature branch",
    )

    # Use commit_and_push to handle commit and push operations
    commit_and_push(
        request_id=request_id,
        local_repo_path=local_repo_path,
        repo_url=repo_url,
        branch=feature_branch,
        commit_message=commit_message,
    )

    return _create_gitlab_mr(repo_url, git_token, feature_branch, branch, request_id)


def _extract_gitlab_info(repo_url: str) -> Tuple[str, str]:
    """
    Extract GitLab API URL and project path from repository URL.

    :param str repo_url: Git repository URL.
    :return: Tuple of (api_url, project_path).
    :rtype: Tuple[str, str]
    :raises IIBError: If the repository URL is not a valid GitLab URL.
    """
    # Parse the URL using urllib.parse for robust handling
    parsed_url = urlparse(repo_url)

    if parsed_url.scheme != 'https':
        raise IIBError(f"Unsupported repository URL format: {repo_url}")

    if not parsed_url.netloc:
        raise IIBError(f"Invalid GitLab repository URL format: {repo_url}")

    # Extract the path and remove .git suffix if present
    path = parsed_url.path
    if path.endswith('.git'):
        path = path[:-4]  # Remove '.git'

    if not path:
        raise IIBError(f"Invalid GitLab repository URL format: {repo_url}")

    # Remove leading slash
    if path.startswith('/'):
        path = path[1:]

    # Construct API URL from the base URL (scheme + netloc)
    api_url = f"https://{parsed_url.netloc}/api/v4"

    # The project path is everything in the path
    project_path = path

    return api_url, project_path


def _create_gitlab_mr(
    repo_url: str, git_token: str, source_branch: str, target_branch: str, request_id: int
) -> Dict[str, str]:
    """
    Create a merge request using GitLab API.

    :param str repo_url: Git repository URL.
    :param str git_token: GitLab access token.
    :param str source_branch: Source branch for the MR.
    :param str target_branch: Target branch for the MR.
    :param int request_id: The ID of the IIB request.
    :return: Dictionary containing MR details (mr_id, mr_url, source_branch).
    :rtype: Dict[str, str]
    :raises IIBError: If GitLab API call fails.
    """
    # Extract GitLab information from repository URL
    api_url, project_path = _extract_gitlab_info(repo_url)

    # GitLab API endpoint for creating merge requests
    api_url = f"{api_url}/projects/{quote_plus(project_path)}/merge_requests"

    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {git_token}'}

    payload = {
        'source_branch': source_branch,
        'target_branch': target_branch,
        'title': f'IIB: Update for request id {request_id}',
        'description': f'Automated merge request created by IIB for request {request_id}',
        'remove_source_branch': True,
        'squash': True,
    }

    try:
        log.info("Creating merge request via GitLab API for project %s", project_path)
        response = requests_session.post(api_url, headers=headers, json=payload, timeout=30)

        if not response.ok:
            log.error(
                'Failed to create merge request. Status: %d, Response: %s',
                response.status_code,
                response.text,
            )
            raise IIBError(f'Failed to create merge request: {response.status_code}')

        mr_data = response.json()
        mr_id = str(mr_data['iid'])
        mr_url = mr_data['web_url']

        log.info("Successfully created merge request %s: %s", mr_id, mr_url)

        return {'mr_id': mr_id, 'mr_url': mr_url, 'source_branch': source_branch}

    except requests.RequestException as e:
        log.exception("Error creating merge request via GitLab API")
        raise IIBError(f'GitLab API request failed: {str(e)}')


@instrument_tracing(span_name="workers.tasks.git_utils.close_mr")
def close_mr(mr_details: Dict[str, str], repo_url: str) -> None:
    """
    Close a merge request on GitLab repository.

    :param dict mr_details: Dictionary containing MR details (mr_id, mr_url, source_branch).
    :param str repo_url: Git repository URL.
    :raises IIBError: If GitLab API call fails.
    """
    mr_id = mr_details.get('mr_id')
    if not mr_id:
        raise IIBError("Missing mr_id in mr_details")

    # Get GitLab token for API access
    _, git_token = get_git_token(repo_url)

    # Close merge request via GitLab API
    _close_gitlab_mr(repo_url, git_token, mr_id)


def _close_gitlab_mr(repo_url: str, git_token: str, mr_id: str) -> None:
    """
    Close a merge request using GitLab API.

    :param str repo_url: Git repository URL.
    :param str git_token: GitLab access token.
    :param str mr_id: Merge request ID.
    :raises IIBError: If GitLab API call fails.
    """
    # Extract GitLab information from repository URL
    api_url, project_path = _extract_gitlab_info(repo_url)

    # GitLab API endpoint for updating merge requests
    api_url = f"{api_url}/projects/{quote_plus(project_path)}/merge_requests/{mr_id}"

    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {git_token}'}

    payload = {'state_event': 'close'}

    try:
        log.info("Closing merge request %s via GitLab API", mr_id)
        response = requests_session.put(api_url, headers=headers, json=payload, timeout=30)

        if not response.ok:
            log.error(
                'Failed to close merge request. Status: %d, Response: %s',
                response.status_code,
                response.text,
            )
            raise IIBError(f'Failed to close merge request: {response.status_code}')

        log.info("Successfully closed merge request %s", mr_id)

    except requests.RequestException as e:
        log.exception("Error closing merge request via GitLab API")
        raise IIBError(f'GitLab API request failed: {str(e)}')
