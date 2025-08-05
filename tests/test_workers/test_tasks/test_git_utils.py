# SPDX-License-Identifier: GPL-3.0-or-later
"""Basic unit tests for git_utils."""
import logging
import pytest
import tempfile
from unittest import mock

from operator_manifest.operator import ImageName


from iib.exceptions import IIBError
from iib.workers.tasks.utils import run_cmd
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    configure_git_user,
    get_git_token,
    push_configs_to_git,
    resolve_git_url,
    revert_last_commit,
)

GIT_BASE_URL = 'https://gitlab.cee.redhat.com/exd-guild-hello-operator-gitlab'
PUB_INDEX_IMAGE = 'registry-proxy.engineering.redhat.com/rh-osbs/iib-pub'
PUB_GIT_REPO = f"{GIT_BASE_URL}/iib-pub-index-configs.git"
PUB_TOKEN_NAME = "iibpubtoken"
PUB_TOKEN_VALUE = "iibpubabc123"
PUB_PENDING_INDEX_IMAGE = 'registry-proxy.engineering.redhat.com/rh-osbs/iib-pub-pending'
PUB_PENDING_GIT_REPO = f"{GIT_BASE_URL}/iib-pub-pending-index-configs.git"
PUB_PENDING_TOKEN_NAME = "iibpubpendingtoken"
PUB_PENDING_TOKEN_VALUE = "iibpubpendingabc123"


@pytest.fixture()
def mock_gwc():
    with mock.patch('iib.workers.tasks.git_utils.get_worker_config') as mc:
        mc.return_value = {
            "iib_index_configs_gitlab_tokens_map": {
                PUB_GIT_REPO: (PUB_TOKEN_NAME, PUB_TOKEN_VALUE),
                PUB_PENDING_GIT_REPO: (PUB_PENDING_TOKEN_NAME, PUB_PENDING_TOKEN_VALUE),
            },
        }
        yield mc


@pytest.fixture()
def gitlab_url_mapping():
    return {
        PUB_INDEX_IMAGE: PUB_GIT_REPO,
        PUB_PENDING_INDEX_IMAGE: PUB_PENDING_GIT_REPO,
    }


def test_configure_git_user():
    test_user = "IIB Test Person"
    test_email = "iib-test-person@redhat.com"
    with tempfile.TemporaryDirectory(prefix=f"test-git-repo") as test_repo:
        run_cmd(f"git -C {test_repo} init".split(), strict=False)
        configure_git_user(test_repo, test_user, test_email)
        git_user = run_cmd(f"git -C {test_repo} config get user.name".split())
        git_email = run_cmd(f"git -C {test_repo} config get user.email".split())
        assert git_user.strip() == test_user
        assert git_email.strip() == test_email


def test_unmapped_git_token(mock_gwc):
    repo_url = f"{GIT_BASE_URL}/some-unknown-repo.git"
    expected_error = f"Missing key '{repo_url}' in 'iib_index_configs_gitlab_tokens_map'"
    with pytest.raises(IIBError, match=expected_error):
        git_token_name, git_token = get_git_token(repo_url)


@pytest.mark.parametrize(
    "repo_url,expected_token_name,expected_token_value",
    [
        (PUB_GIT_REPO, PUB_TOKEN_NAME, PUB_TOKEN_VALUE),
        (PUB_PENDING_GIT_REPO, PUB_PENDING_TOKEN_NAME, PUB_PENDING_TOKEN_VALUE),
    ],
)
def test_get_git_token(repo_url, expected_token_name, expected_token_value, mock_gwc):
    git_token_name, git_token_value = get_git_token(repo_url)
    assert git_token_name == expected_token_name
    assert git_token_value == expected_token_value


def test_unmapped_git_url(mock_gwc, gitlab_url_mapping, caplog):
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    git_logger = logging.getLogger('iib.workers.tasks.git_utils')
    git_logger.disabled = False
    git_logger.setLevel(logging.DEBUG)

    index_image = ImageName.parse("some-registry.com/test/image:latest")
    index_no_tag = f"{index_image.registry}/{index_image.namespace}/{index_image.repo}"
    expected_warning = f"Missing key '{index_no_tag}' in 'iib_web_index_to_gitlab_push_map'"
    res = resolve_git_url(index_image, gitlab_url_mapping)
    assert not res
    assert expected_warning in caplog.messages


@pytest.mark.parametrize(
    "from_index,expected_git_repo",
    [
        (
            "registry-proxy.engineering.redhat.com/rh-osbs/iib-pub-pending:v4.19",
            PUB_PENDING_GIT_REPO,
        ),
        (
            "registry-proxy.engineering.redhat.com/rh-osbs/iib-pub:v4.20",
            PUB_GIT_REPO,
        ),
    ],
)
def test_resolve_git_url(from_index, expected_git_repo, mock_gwc, gitlab_url_mapping):
    mapped_git_repo = resolve_git_url(from_index, gitlab_url_mapping)
    assert mapped_git_repo == expected_git_repo


@mock.patch("iib.workers.tasks.git_utils.run_cmd")
@mock.patch("iib.workers.tasks.git_utils.get_git_token")
def test_push_configs_to_git_aborts_without_repo_map(mock_ggt, mock_cmd) -> None:
    """Ensure the ``push_configs_to_git`` will not store the catalog to git without the repo map."""
    res = push_configs_to_git(
        request_id=1,
        from_index="some-registry.com/foobar:latest",
        src_configs_path="/configs",
        index_repo_map={},
    )

    assert not res
    mock_ggt.assert_not_called()
    mock_cmd.assert_not_called()


@mock.patch("iib.workers.tasks.git_utils.run_cmd")
@mock.patch("iib.workers.tasks.git_utils.get_git_token")
def test_revert_last_commit_aborts_without_repo_map(mock_ggt, mock_cmd) -> None:
    """Ensure the ``revert_last_commit`` will not store the catalog to git without the repo map."""
    res = revert_last_commit(
        request_id=1,
        from_index="some-registry.com/foobar:latest",
        index_repo_map={},
    )

    assert not res
    mock_ggt.assert_not_called()
    mock_cmd.assert_not_called()


@pytest.mark.parametrize(
    "token_name,token_secret",
    [
        ("token", "token_super_secret"),
        ("secret-token", "Sup3r_S3cr31-T0ken"),
        ("secret-token", "Sup3r_S3cr31-T0ken"),
        ("fake_GH-secret", "ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789"),
        ("F4k3-G1tL4b-T0k3n", "glpat-ABcdeF5g6HIjkl1Mnop11"),
    ],
)
@mock.patch("iib.workers.tasks.utils.subprocess")
def test_clone_git_repo_wont_leak_credentials(
    mock_subprocess, token_name, token_secret, caplog
) -> None:
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    git_logger = logging.getLogger('iib.workers.tasks.utils')
    git_logger.disabled = False
    git_logger.setLevel(logging.DEBUG)

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

    # Git clone params
    branch = "main"
    local_repo_path = "https://local_repo"
    remote_url = f"https://{token_name}:{token_secret}@fake_repo"

    # Test
    clone_git_repo(
        repo_url="https://fake_repo",
        branch=branch,
        token_name=token_name,
        token=token_secret,
        local_repo_path=local_repo_path,
    )

    mock_subprocess.run.assert_has_calls(
        [
            mock.call(
                ["git", "clone", "--depth", "1", "--branch", branch, remote_url, local_repo_path],
                **default_run_cmd_args,
            ),
            mock.call(["git", "-C", local_repo_path, "log", "-n1"], **default_run_cmd_args),
        ],
        any_order=True,
    )

    # Ensure the `super_secret_token` isn't leaked
    for msg in caplog.messages:
        assert msg.find(token_secret) == -1

    # Ensure the sanitized log is present
    expected_log = (
        f'Running the command "git clone --depth 1 --branch {branch} '
        f'https://*****:*******@fake_repo {local_repo_path}"'
    )
    assert expected_log in caplog.messages


@mock.patch("iib.workers.tasks.utils.subprocess")
def test_clone_git_repo_no_credentials(mock_subprocess, caplog) -> None:
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    git_logger = logging.getLogger('iib.workers.tasks.utils')
    git_logger.disabled = False
    git_logger.setLevel(logging.DEBUG)

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

    # Git clone params
    branch = "main"
    local_repo_path = "https://local_repo"
    remote_url = "https://fake_repo"
    expected_url = "https://:@fake_repo"

    # Test
    clone_git_repo(
        repo_url=remote_url,
        branch=branch,
        token_name="",
        token="",
        local_repo_path=local_repo_path,
    )

    mock_subprocess.run.assert_has_calls(
        [
            mock.call(
                ["git", "clone", "--depth", "1", "--branch", branch, expected_url, local_repo_path],
                **default_run_cmd_args,
            ),
            mock.call(["git", "-C", local_repo_path, "log", "-n1"], **default_run_cmd_args),
        ],
        any_order=True,
    )

    # Ensure the sanitized log is present
    expected_log = (
        'Running the command "git clone --depth 1 --branch '
        f'{branch} {expected_url} {local_repo_path}"'
    )
    assert expected_log in caplog.messages
