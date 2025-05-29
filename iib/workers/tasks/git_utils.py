
# SPDX-License-Identifier: GPL-3.0-or-later
"""This file contains functions for saving changes to GitLab"""
import logging
import os
import tempfile
import shutil
from typing import Optional, Tuple

from operator_manifest.operator import ImageName

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import run_cmd


log = logging.getLogger(__name__)


@instrument_tracing(span_name="workers.tasks.git_utils.push_configs_to_git")
def push_configs_to_git(
    request_id: int,
    from_index: str,
    src_configs_path: str,
    commit_message: Optional[str] = None
) -> None:

    """
    Pushes /configs subfolders to GitLab repo.

    :param int request_id: The ID of the IIB request.
    :param str from_index: The from_index pullspec. Note: This should have a tag that
                           corresponds to the OCP version, like 'v4.19'. The code
                           assumes that the branch already exists in remote repo.
    :param str src_configs_path: Path to /configs folder where <pkg>/catalog.json
                           files reside.
    :param str commit_message: Custom commit message. If None, a default message is used.
    :raises IIBError: If src_configs_path is not found, remote branch does not exist,
                      or a Git operation fails.
    """
    # Determine Git repo from pullspec
    index_image = ImageName.parse(from_index)
    branch = index_image.tag
    repo_url = get_gitlab_url(from_index)
    gitlab_token_name, gitlab_token = get_gitlab_token(repo_url)

    # Validate branch
    remote_branch_status = run_cmd(["git",  "ls-remote", "--exit-code", "--heads",
                                    repo_url, branch], strict=False)
    if not remote_branch_status:
        raise IIBError(f"Remote branch '{branch}' does not exist in {repo_url}")

    # Verify the path to existing /conifgs is correct
    if not os.path.exists(src_configs_path):
        raise IIBError(f"Catalog configs directory does not exist: {src_configs_path}")

    # Make sure there are subdirs under /configs for the operator package dirs
    operator_packages = os.listdir(src_configs_path)
    if not operator_packages:
        raise IIBError(f"No packages found in configs directory {src_configs_path}")

    # Clone/checkout remote repo in temp dir
    with tempfile.TemporaryDirectory(
        prefix=f"git-repo-{request_id}-", delete=True) as local_repo_dir:
        log.info("Cloning repo to %s", local_repo_dir)
        try:
            clone_gitlab_repo(
                repo_url,
                branch,
                gitlab_token_name,
                gitlab_token,
                local_repo_dir
            )

            # Configure Git user for commits
            configure_git_user(local_repo_dir)

            # Copy configs/ subdirs to local Git repo
            repo_configs_dir = os.path.join(local_repo_dir, 'configs')
            log.info("Copying content of %s to local Git repository %s",
                src_configs_path, repo_configs_dir)
            for operator_package in operator_packages:
                src_pkg_dir = os.path.join(src_configs_path, operator_package)
                dest_pkg_dir = os.path.join(repo_configs_dir, operator_package)
                os.makedirs(dest_pkg_dir, exist_ok=True)
                shutil.copytree(src_pkg_dir, dest_pkg_dir, dirs_exist_ok=True)

            # Print git status to the logs
            git_status = run_cmd(["git", "-C", local_repo_dir, "status"],
                exc_msg="Error getting git status")
            log.info(git_status)

            # Add updates
            log.info("Commiting changes to local Git repository.")
            run_cmd(["git", "-C", local_repo_dir, "add", "."],
                exc_msg="Error staging changes to git")
            git_status = run_cmd(["git", "-C", local_repo_dir, "status"],
                exc_msg="Error getting git status")
            log.info(git_status)

            # Commit updates
            final_commit_message = commit_message or (
                f"IIB: Update for request id {request_id} (overwrite_from_index)"
            )
            commit_output = run_cmd(
                ["git", "-C", local_repo_dir, "commit", "-m", final_commit_message],
                exc_msg="Error committing changes"
            )
            log.info(commit_output)

            # Push updates
            log.info("Pushing changes to %s branch of %s", branch, repo_url)
            push_output = run_cmd(
                ["git", "-C", local_repo_dir, "push", "--force", "origin", branch],
                exc_msg=f"Error pushing changes to git repo {repo_url}"
            )
            log.info(push_output)

        finally:
            # tempfile should have done this already, but just in case
            if os.path.exists(local_repo_dir):
                shutil.rmtree(local_repo_dir)
                log.debug("Cleaned up local Git repository %s", local_repo_dir)

def get_gitlab_url(from_index) -> str:
    """
    Get GitLab URL from iib_web_index_to_gitlab_push_map.

    :param str from_index: from_index image pull spec.
    :return: GitLab URL.
    :rtype: str
    :raises IIBError: If no mapping found.
    """
    index_image = ImageName.parse(from_index)
    index_no_tag = f"{index_image.registry}/{index_image.namespace}/{index_image.repo}"
    index_repo_map = get_worker_config()['iib_web_index_to_gitlab_push_map']
    map_for_index = index_repo_map.get(index_no_tag, None)
    if not map_for_index:
        raise IIBError(f"No mapping found for image {from_index}")
    return map_for_index

def get_gitlab_token(git_repo) -> Tuple[str, str]:
    """
    Get GitLab token from iib_gitlab_token_map.

    :param str git_repo: Git repository URL.
    :return: token name, token value.
    :rtype: Tuple[str, str]
    :raises IIBError: If no token found for Git repository.
    """
    git_token_map = get_worker_config()['iib_gitlab_token_map']
    if git_repo not in git_token_map:
        raise IIBError(f"No token found for Git repo {git_repo}")
    return git_token_map[git_repo]

def clone_gitlab_repo(
    repo_url: str,
    branch: str,
    token_name: str,
    token: str,
    local_repo_path: str
    ) -> None:
    """
    Clone GitLab repository and perform checkout.

    In order to make effective use of space:
    - The origin is set to track only a single branch corresponding to the OCP version.
    - Shallow clone with depth of 1 used to retrieve only the most recent commit.

    :param str repo_url: GitLab repo URL.
    :param str branch: Branch name corresponding to OCP version, like "v4.19".
    :param str token_name: Name of GitLab token.
    :param str token: Value of GitLab token.
    :param str local_repo_path: The local path where the Git repo will be cloned.
    """
    base_url = repo_url.replace('https://', '')
    remote_url = f"https://{token_name}:{token}@{base_url}"

    clone_output = run_cmd(
        [
        "git",
        "clone",
        "--depth", "1",
        "--branch", branch,
        remote_url, local_repo_path
        ], exc_msg=f"Error cloning remote repository for {repo_url}")
    log.info(clone_output)

    # Show most recent commit
    last_commit = run_cmd(
        ["git", "-C", local_repo_path, "log", "-n1"],
        exc_msg=f"Error displaying last commit for {repo_url}")
    log.info("Most recent commit: %s", last_commit)

def configure_git_user(
    local_repo_path: str,
    user_name: Optional[str] = "IIB Worker",
    email_address: Optional[str] = "iib-worker@redhat.com"
    ):
    """
    Configure git user name and email displayed in commit message.

    :param str local_repo_path: Path to local Git repo.
    :param str user_name: User name for local Git repo.
    :param str email_address: Email address for local Git repo.
    """
    set_username = ["git", "-C", local_repo_path, "config",  "--local", "user.name", user_name]
    run_cmd(set_username, exc_msg="Error configuring git user.email")

    set_email = ["git", "-C", local_repo_path, "config", "--local", "user.email", email_address]
    run_cmd(set_email, exc_msg="Error configuring git user.email")
