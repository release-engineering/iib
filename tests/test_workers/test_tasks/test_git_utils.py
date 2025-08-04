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
