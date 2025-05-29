# SPDX-License-Identifier: GPL-3.0-or-later
"""Basic unit tests for git_utils."""
import pytest
import tempfile
from unittest import mock

from iib.exceptions import IIBError
from iib.workers.tasks.utils import run_cmd
from iib.workers.tasks.git_utils import (
    configure_git_user,
    get_gitlab_token,
    get_gitlab_url,
)

GITLAB_BASE_URL = 'https://gitlab.cee.redhat.com/exd-guild-hello-operator-gitlab'
PUB_INDEX_IMAGE = 'registry-proxy.engineering.redhat.com/rh-osbs/iib-pub'
PUB_GIT_REPO = f"{GITLAB_BASE_URL}/iib-pub-index-configs.git"
PUB_TOKEN_NAME = "iibpubtoken"
PUB_TOKEN_VALUE = "iibpubabc123"
PUB_PENDING_INDEX_IMAGE = 'registry-proxy.engineering.redhat.com/rh-osbs/iib-pub-pending'
PUB_PENDING_GIT_REPO = f"{GITLAB_BASE_URL}/iib-pub-pending-index-configs.git"
PUB_PENDING_TOKEN_NAME = "iibpubpendingtoken"
PUB_PENDING_TOKEN_VALUE = "iibpubpendingabc123"


@pytest.fixture()
def mock_gwc():
    with mock.patch('iib.workers.tasks.git_utils.get_worker_config') as mc:
        mc.return_value = {
            "iib_web_index_to_gitlab_push_map": {
                PUB_INDEX_IMAGE: PUB_GIT_REPO,
                PUB_PENDING_INDEX_IMAGE: PUB_PENDING_GIT_REPO,
            },
            "iib_gitlab_token_map": {
                PUB_GIT_REPO: (PUB_TOKEN_NAME, PUB_TOKEN_VALUE),
                PUB_PENDING_GIT_REPO: (PUB_PENDING_TOKEN_NAME, PUB_PENDING_TOKEN_VALUE),
            },
        }
        yield mc


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


def test_unmapped_gitlab_token(mock_gwc):
    repo_url = f"{GITLAB_BASE_URL}/some-unknown-repo.git"
    expected_error = f"No token found for Git repo {repo_url}"
    with pytest.raises(IIBError, match=expected_error):
        gitlab_token_name, gitlab_token = get_gitlab_token(repo_url)


@pytest.mark.parametrize(
    "repo_url,expected_token_name,expected_token_value",
    [
        (PUB_GIT_REPO, PUB_TOKEN_NAME, PUB_TOKEN_VALUE),
        (PUB_PENDING_GIT_REPO, PUB_PENDING_TOKEN_NAME, PUB_PENDING_TOKEN_VALUE),
    ],
)
def test_get_gitlab_token(repo_url, expected_token_name, expected_token_value, mock_gwc):
    gitlab_token_name, gitlab_token_value = get_gitlab_token(repo_url)
    assert gitlab_token_name == expected_token_name
    assert gitlab_token_value == expected_token_value


def test_unmapped_gitlab_url(mock_gwc):
    image = "some-image:latest"
    expected_error = f"No mapping found for image {image}"
    with pytest.raises(IIBError, match=expected_error):
        get_gitlab_url(image)


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
def test_get_gitlab_url(from_index, expected_git_repo, mock_gwc):
    mapped_git_repo = get_gitlab_url(from_index)
    assert mapped_git_repo == expected_git_repo
