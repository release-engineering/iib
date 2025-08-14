# SPDX-License-Identifier: GPL-3.0-or-later
"""Basic unit tests for git_utils."""
import logging
import pytest
import tempfile
from unittest import mock

import requests
from operator_manifest.operator import ImageName

from iib.exceptions import IIBError
from iib.workers.tasks import git_utils
from iib.workers.tasks.utils import run_cmd
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    push_configs_to_git,
    resolve_git_url,
    revert_last_commit,
)

GIT_BASE_URL = 'https://my-gitlab-instance.com/exd-guild-hello-operator-gitlab'
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
    with tempfile.TemporaryDirectory(prefix="test-git-repo") as test_repo:
        run_cmd(f"git -C {test_repo} init".split(), strict=False)
        git_utils.configure_git_user(test_repo, test_user, test_email)
        git_user = run_cmd(f"git -C {test_repo} config --get user.name".split())
        git_email = run_cmd(f"git -C {test_repo} config --get user.email".split())
        assert git_user.strip() == test_user
        assert git_email.strip() == test_email


def test_unmapped_git_token(mock_gwc):
    repo_url = f"{GIT_BASE_URL}/some-unknown-repo.git"
    expected_error = f"Missing key '{repo_url}' in 'iib_index_configs_gitlab_tokens_map'"
    with pytest.raises(IIBError, match=expected_error):
        git_token_name, git_token = git_utils.get_git_token(repo_url)


@pytest.mark.parametrize(
    "repo_url,expected_token_name,expected_token_value",
    [
        (PUB_GIT_REPO, PUB_TOKEN_NAME, PUB_TOKEN_VALUE),
        (PUB_PENDING_GIT_REPO, PUB_PENDING_TOKEN_NAME, PUB_PENDING_TOKEN_VALUE),
    ],
)
def test_get_git_token(repo_url, expected_token_name, expected_token_value, mock_gwc):
    git_token_name, git_token_value = git_utils.get_git_token(repo_url)
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
    mapped_git_repo = git_utils.resolve_git_url(from_index, gitlab_url_mapping)
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


@mock.patch('iib.workers.tasks.git_utils._create_gitlab_mr')
@mock.patch('iib.workers.tasks.git_utils.get_git_token')
@mock.patch('iib.workers.tasks.git_utils.commit_and_push')
@mock.patch('iib.workers.tasks.git_utils.run_cmd')
def test_create_mr_success(
    mock_run_cmd, mock_commit_and_push, mock_get_git_token, mock_create_gitlab_mr
):
    """Test successful creation of merge request."""
    mock_get_git_token.return_value = (PUB_TOKEN_NAME, PUB_TOKEN_VALUE)
    mock_run_cmd.return_value = "Success"
    mock_commit_and_push.return_value = None
    mock_create_gitlab_mr.return_value = {
        'mr_id': '123',
        'mr_url': 'https://my-gitlab-instance.com/project/merge_requests/123',
        'source_branch': 'iib-request-456-v4.19',
    }

    with tempfile.TemporaryDirectory(prefix="test-git-repo") as test_repo:
        # Initialize git repo
        run_cmd(f"git -C {test_repo} init".split(), strict=False)
        run_cmd(f"git -C {test_repo} config user.name 'Test'".split(), strict=False)
        run_cmd(f"git -C {test_repo} config user.email 'test@example.com'".split(), strict=False)

        # Create a test file to commit
        with open(f"{test_repo}/test.txt", "w") as f:
            f.write("test content")

        run_cmd(f"git -C {test_repo} add test.txt".split(), strict=False)

        # Test create_mr
        result = git_utils.create_mr(
            request_id=456,
            local_repo_path=test_repo,
            repo_url=PUB_GIT_REPO,
            branch="v4.19",
            commit_message="Test commit",
        )

        # Verify results
        assert result['mr_id'] == '123'
        assert result['mr_url'] == 'https://my-gitlab-instance.com/project/merge_requests/123'
        assert result['source_branch'] == 'iib-request-456-v4.19'

        # Verify git commands were called
        mock_run_cmd.assert_any_call(
            ["git", "-C", test_repo, "checkout", "-b", "iib-request-456-v4.19"],
            exc_msg="Error creating feature branch",
        )

        # Verify commit_and_push was called with correct parameters
        mock_commit_and_push.assert_called_once_with(
            request_id=456,
            local_repo_path=test_repo,
            repo_url=PUB_GIT_REPO,
            branch="iib-request-456-v4.19",
            commit_message="Test commit",
        )

        # Verify GitLab API was called
        mock_create_gitlab_mr.assert_called_once_with(
            PUB_GIT_REPO, PUB_TOKEN_VALUE, "iib-request-456-v4.19", "v4.19", 456
        )


@mock.patch('iib.workers.tasks.git_utils._close_gitlab_mr')
@mock.patch('iib.workers.tasks.git_utils.get_git_token')
def test_close_mr_success(mock_get_git_token, mock_close_gitlab_mr):
    """Test successful closing of merge request."""
    mock_get_git_token.return_value = (PUB_TOKEN_NAME, PUB_TOKEN_VALUE)
    mock_close_gitlab_mr.return_value = None

    mr_details = {
        'mr_id': '123',
        'mr_url': 'https://my-gitlab-instance.com/project/merge_requests/123',
        'source_branch': 'iib-request-456-v4.19',
    }

    # Test close_mr
    git_utils.close_mr(mr_details, PUB_GIT_REPO)

    # Verify GitLab API was called
    mock_close_gitlab_mr.assert_called_once_with(PUB_GIT_REPO, PUB_TOKEN_VALUE, '123')


def test_close_mr_missing_mr_id():
    """Test close_mr with missing mr_id."""
    mr_details = {
        'mr_url': 'https://my-gitlab-instance.com/project/merge_requests/123',
        'source_branch': 'iib-request-456-v4.19',
    }

    with pytest.raises(IIBError, match="Missing mr_id in mr_details"):
        git_utils.close_mr(mr_details, PUB_GIT_REPO)


@pytest.mark.parametrize(
    "repo_url,expected_api_url,expected_project_path",
    [
        (
            PUB_GIT_REPO,
            'https://my-gitlab-instance.com/api/v4',
            'exd-guild-hello-operator-gitlab/iib-pub-index-configs',
        ),
        (
            PUB_GIT_REPO.replace('.git', ''),
            'https://my-gitlab-instance.com/api/v4',
            'exd-guild-hello-operator-gitlab/iib-pub-index-configs',
        ),
        (
            'https://gitlab.com/mygroup/myproject.git',
            'https://gitlab.com/api/v4',
            'mygroup/myproject',
        ),
        (
            'https://gitlab.com/mygroup/subgroup/myproject.git',
            'https://gitlab.com/api/v4',
            'mygroup/subgroup/myproject',
        ),
        ('https://gitlab.com/myproject.git', 'https://gitlab.com/api/v4', 'myproject'),
    ],
)
def test_extract_gitlab_info_success(repo_url, expected_api_url, expected_project_path):
    """Test successful extraction of GitLab info from valid repository URLs."""
    api_url, project_path = git_utils._extract_gitlab_info(repo_url)
    assert api_url == expected_api_url
    assert project_path == expected_project_path


@pytest.mark.parametrize(
    "invalid_url",
    [
        'https://',  # Missing domain
        'https://gitlab.com',  # Missing path
    ],
)
def test_extract_gitlab_info_invalid_format(invalid_url):
    """Test _extract_gitlab_info with invalid URL formats."""
    with pytest.raises(IIBError, match="Invalid GitLab repository URL format"):
        git_utils._extract_gitlab_info(invalid_url)


@pytest.mark.parametrize(
    "unsupported_url",
    [
        'git://example.com/repo.git',  # git protocol
        'http://gitlab.com/repo.git',  # http protocol
        'ssh://git@gitlab.com/repo.git',  # ssh protocol
    ],
)
def test_extract_gitlab_info_unsupported_scheme(unsupported_url):
    """Test _extract_gitlab_info with unsupported URL schemes."""
    with pytest.raises(IIBError, match="Unsupported repository URL format"):
        git_utils._extract_gitlab_info(unsupported_url)


@mock.patch('iib.workers.api_utils.requests_session.post')
def test_create_gitlab_mr_success(mock_requests_post):
    """Test successful GitLab API call for creating MR."""
    # Mock successful response
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_response.json.return_value = {
        'iid': 123,
        'web_url': 'https://my-gitlab-instance.com/project/merge_requests/123',
    }
    mock_requests_post.return_value = mock_response

    result = git_utils._create_gitlab_mr(
        repo_url=PUB_GIT_REPO,
        git_token=PUB_TOKEN_VALUE,
        source_branch='feature-branch',
        target_branch='main',
        request_id=456,
    )

    assert result['mr_id'] == '123'
    assert result['mr_url'] == 'https://my-gitlab-instance.com/project/merge_requests/123'
    assert result['source_branch'] == 'feature-branch'

    # Verify API call
    mock_requests_post.assert_called_once()
    call_args = mock_requests_post.call_args
    assert 'https://my-gitlab-instance.com/api/v4/projects/' in call_args[0][0]
    assert call_args[1]['headers']['Authorization'] == f'Bearer {PUB_TOKEN_VALUE}'


@mock.patch('iib.workers.api_utils.requests_session.put')
def test_close_gitlab_mr_success(mock_requests_put):
    """Test successful GitLab API call for closing MR."""
    # Mock successful response
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_requests_put.return_value = mock_response

    git_utils._close_gitlab_mr(repo_url=PUB_GIT_REPO, git_token=PUB_TOKEN_VALUE, mr_id='123')

    # Verify API call
    mock_requests_put.assert_called_once()
    call_args = mock_requests_put.call_args
    assert 'https://my-gitlab-instance.com/api/v4/projects/' in call_args[0][0]
    assert call_args[1]['headers']['Authorization'] == f'Bearer {PUB_TOKEN_VALUE}'
    assert call_args[1]['json']['state_event'] == 'close'


@pytest.mark.parametrize(
    "status_code,error_message",
    [
        (400, "Bad Request"),
        (500, "Internal Server Error"),
    ],
)
@mock.patch('iib.workers.api_utils.requests_session.post')
def test_create_gitlab_mr_http_errors(mock_requests_post, status_code, error_message):
    """Test GitLab API call for creating MR with various HTTP error responses."""
    # Mock error response
    mock_response = mock.Mock()
    mock_response.ok = False
    mock_response.status_code = status_code
    mock_response.text = error_message
    mock_requests_post.return_value = mock_response

    with pytest.raises(IIBError, match=f"Failed to create merge request: {status_code}"):
        git_utils._create_gitlab_mr(
            repo_url=PUB_GIT_REPO,
            git_token=PUB_TOKEN_VALUE,
            source_branch='feature-branch',
            target_branch='main',
            request_id=456,
        )

    # Verify API call was made
    mock_requests_post.assert_called_once()


@mock.patch('iib.workers.api_utils.requests_session.post')
def test_create_gitlab_mr_network_errors(mock_requests_post):
    """Test GitLab API call for creating MR with network/connection errors."""
    # Mock network error - RequestException is the base class that catches all requests exceptions
    mock_requests_post.side_effect = requests.RequestException("Network error occurred")

    with pytest.raises(IIBError, match="GitLab API request failed: Network error occurred"):
        git_utils._create_gitlab_mr(
            repo_url=PUB_GIT_REPO,
            git_token=PUB_TOKEN_VALUE,
            source_branch='feature-branch',
            target_branch='main',
            request_id=456,
        )

    # Verify API call was attempted
    mock_requests_post.assert_called_once()


@pytest.mark.parametrize(
    "status_code,error_message",
    [
        (400, "Bad Request"),
        (500, "Internal Server Error"),
    ],
)
@mock.patch('iib.workers.api_utils.requests_session.put')
def test_close_gitlab_mr_http_errors(mock_requests_put, status_code, error_message):
    """Test GitLab API call for closing MR with various HTTP error responses."""
    # Mock error response
    mock_response = mock.Mock()
    mock_response.ok = False
    mock_response.status_code = status_code
    mock_response.text = error_message
    mock_requests_put.return_value = mock_response

    with pytest.raises(IIBError, match=f"Failed to close merge request: {status_code}"):
        git_utils._close_gitlab_mr(
            repo_url=PUB_GIT_REPO,
            git_token=PUB_TOKEN_VALUE,
            mr_id='123',
        )

    # Verify API call was made
    mock_requests_put.assert_called_once()


@mock.patch('iib.workers.api_utils.requests_session.put')
def test_close_gitlab_mr_network_errors(mock_requests_put):
    """Test GitLab API call for closing MR with network/connection errors."""
    # Mock network error - RequestException is the base class that catches all requests exceptions
    mock_requests_put.side_effect = requests.RequestException("Network error occurred")

    with pytest.raises(IIBError, match="GitLab API request failed: Network error occurred"):
        git_utils._close_gitlab_mr(
            repo_url=PUB_GIT_REPO,
            git_token=PUB_TOKEN_VALUE,
            mr_id='123',
        )

    # Verify API call was attempted
    mock_requests_put.assert_called_once()
