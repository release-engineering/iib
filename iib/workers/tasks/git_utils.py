
# SPDX-License-Identifier: GPL-3.0-or-later
# This file contains functions for saving changes to GitLab
import logging
import os
import tempfile
import shutil
from typing import List, Optional, Tuple

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
    operator_packages: List[str],
    src_configs_path: str, # /configs folder where updates have been applied
    commit_message: Optional[str] = None
) -> None:

    """
    Pushes /configs subfolders to GitLab repo.

    :param int request_id: The ID of the IIB request.
    :param str from_index: The from_index pullspec. This should have a tag corresponding
                           to THE OCP version.
    :param List[str] operator_packages: List of operator package names that will be saved.
    :param str src_configs_path: Path to /configs folder where updates have been applied.
    :param str commit_message: Custom commit message. If None, a default message is used.
    :raises IIBError: If any Git operation fails.
    """
    # Derive git repo data
    index_image = ImageName.parse(from_index)
    branch = index_image.tag
    repo_url = _get_gitlab_url(from_index)
    if not repo_url:
        raise IIBError("No GitLab URL configured for from_index '{from_index}'")
    git_token_name, git_token = _get_gitlab_token(repo_url)

    # Verify paths
    for pkg in operator_packages:
        src_catalog_json = os.path.join(src_configs_path, pkg, 'catalog.json')
        if not os.path.exists(src_catalog_json):
            raise IIBError(f"Catalog JSON file does not exist: {src_catalog_json}")

    with tempfile.TemporaryDirectory(
        prefix=f"git-repo-{request_id}-",
        delete=True) as local_repo_dir:
        os.chdir(local_repo_dir)
        log.info("Cloning repo to %s", local_repo_dir)

        try:
            _clone_repo(
                repo_url,
                branch,
                git_token_name,
                git_token,
                operator_packages,
                local_repo_dir
            )

            # Copy catalog.json files from updated operator packages
            for pkg in operator_packages:
                src_catalog_json = os.path.join(src_configs_path, pkg, 'catalog.json')
                dest_catalog_dir = os.path.join(local_repo_dir, pkg)
                dest_catalog_json = os.path.join(dest_catalog_dir, 'catalog.json')

                # Verify if package already exists in Git
                if not os.path.exists(dest_catalog_dir):
                    os.mkdir(dest_catalog_dir)

                # Copy catalog.json
                shutil.copyfile(src_catalog_json, dest_catalog_json)

            git_status = run_cmd(["git", "-C", local_repo_dir, "status", "-vv"],
                exc_msg="Error getting git status")
            log.info(git_status)

            run_cmd(["git", "-C", local_repo_dir, "add", "."],
                exc_msg="Error staging changes to git")
            git_status = run_cmd(["git", "-C", local_repo_dir, "status"],
                exc_msg="Error getting git status")
            log.info(git_status)

            final_commit_message = commit_message or (
                f"IIB: Update for request id {request_id} (overwrite_from_index)"
            )

            # Commit
            res = run_cmd(
                ["git", "-C", local_repo_dir, "commit", "-m", final_commit_message],
                exc_msg="Error committing changes"
            )
            log.info(res)

            # Push
            log.info("Pushing changes to %s branch of %s", branch, repo_url)
            res = run_cmd(
                ["git", "-C", local_repo_dir, "push", "origin", branch],
                exc_msg=f"Error pushing changes to git repo {repo_url}"
            )
            log.info(res)

        finally:
            # tempfile should have done this already, but just in case
            if os.path.exists(local_repo_dir):
                shutil.rmtree(local_repo_dir)
                log.debug("Cleaned up local Git repository at %s", local_repo_dir)

def _get_gitlab_url(from_index) -> str:
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
    return index_repo_map.get(index_no_tag, None)

def _get_gitlab_token(git_repo) -> Tuple[str, str]:
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

def _clone_repo(
    repo_url: str,
    branch: str,
    token_name: str,
    token: str,
    operator_packages: str,
    local_repo_path: str
    ) -> None:
    """
    Clone GitLab repository and checkout paths for specified operator packages.

    This wraps git commands. GitPython does not support sparse-checkout yet.
    https://github.com/gitpython-developers/GitPython/discussions/1250

    In order to make effective use of space:
    - The origin is set to track only a single branch corresponding to the OCP version.
    - The filter is set to blob:none to not pull blobs from the remote.
    - Shallow clone with depth of 1 used to retrieve only the most recent commit.
    - sparseCheckout is used to only retrieve objects for the specified operator packages.

    :param str repo_url: GitLab URL.
    :param str branch: Branch name corresponding to OCP version, like "v4.19".
    :param List[str] operator_packages: Names of operator packages that will be updated.
    :param str token_name: Name of GitLab token.
    :param str token: Value of GitLab token.
    :param str local_repo_path: The local path where the Git repo will be cloned.
    """
    base_url = repo_url.replace('https://', '')
    remote_url = f"https://{token_name}:{token}@{base_url}"

    # First clone the repo
    res = run_cmd(
        [
        "git",
        "clone",
        "--sparse",
        "--depth", "1",
        "--filter", "blob:none",
        "--branch", branch,
        remote_url, local_repo_path
        ], exc_msg=f"Error cloning remote repository for {repo_url}")
    log.info(res)

    # Checkout folders for specific operator packages
    log.info("Configuring sparse checkout")
    cmd = ["git", "-C", local_repo_path, "sparse-checkout", "set"]
    for pkg in operator_packages:
        cmd.append(f"configs/{pkg}")
    res = run_cmd(cmd, exc_msg="Error configuring sparse checkout")
    log.info(res)

    # Configure Git user email for commit message
    set_email = ["git", "-C", local_repo_path, "config", "user.email", "iib-worker@redhat.com"]
    run_cmd(set_email, exc_msg="Error configuring git user.email")
    # Configure Git user name for commit message
    set_username = ["git", "-C", local_repo_path, "config", "user.name", "IIB Worker"]
    run_cmd(set_username, exc_msg="Error configuring git user.email")
