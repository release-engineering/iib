# Multi-Cluster Konflux Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route Konflux builds to environment-specific clusters by unifying all git interactions into the MR path and using source branch naming for PaC CEL filtering.

**Architecture:** Each IIB deployment gets an `iib_environment_name` config (e.g., `"qe"`). All builds — both throw-away and overwrite — go through MRs with env-prefixed source branches (`iib-{env}-request-{id}-{branch}`). Overwrite requests merge the MR after a successful build instead of pushing directly. Push-event pipelines are removed from Konflux.

**Tech Stack:** Python 3.12, Flask, Celery, SQLAlchemy, GitLab API, Kubernetes API, pytest, tox

## Global Constraints

- Run all tests via `tox -e py312`, never `pytest` directly
- Never edit existing Alembic migrations
- Workers never touch Postgres directly — state updates go via `iib/workers/api_utils.py`
- `regenerate_bundle` is always throw-away (close MR, no merge, no index.db push)
- `create_empty_index` always has `overwrite_from_index=False`
- `merge` handler uses `overwrite_target_index` instead of `overwrite_from_index`

---

### Task 1: Add `iib_environment_name` config and validation

**Files:**
- Modify: `iib/workers/config.py:171-176` (add config field)
- Modify: `iib/workers/config.py:539-593` (add validation)
- Modify: `docker/containerized/worker_config.py:22-28` (add env var)
- Test: `tests/test_workers/test_config.py`

**Interfaces:**
- Consumes: nothing
- Produces: `iib_environment_name: Optional[str]` config field, accessible via `get_worker_config().iib_environment_name`

- [ ] **Step 1: Write failing tests for environment name validation**

Add these tests to `tests/test_workers/test_config.py`:

```python
@pytest.mark.parametrize(
    'env_name,expected_error',
    [
        (
            None,
            'iib_environment_name must be set when using Konflux configuration',
        ),
        (
            '',
            'iib_environment_name must be set when using Konflux configuration',
        ),
        (
            123,
            'iib_environment_name must be a non-empty string containing only',
        ),
        (
            'invalid name!',
            'iib_environment_name must be a non-empty string containing only',
        ),
        (
            'has spaces',
            'iib_environment_name must be a non-empty string containing only',
        ),
    ],
)
def test_validate_konflux_config_invalid_environment_name(env_name, expected_error):
    """Test Konflux config validation rejects invalid environment names."""
    conf = mock.Mock()
    conf.get.side_effect = lambda key: {
        'iib_konflux_cluster_url': 'https://api.example.com:6443',
        'iib_konflux_cluster_token': 'test-token',
        'iib_konflux_cluster_ca_cert': '/path/to/ca.crt',
        'iib_konflux_namespace': 'iib-tenant',
        'iib_environment_name': env_name,
    }.get(key)

    with pytest.raises(ConfigError, match=expected_error):
        _validate_konflux_config(conf)


def test_validate_konflux_config_valid_environment_name():
    """Test Konflux config validation accepts valid environment names."""
    conf = mock.Mock()
    conf.get.side_effect = lambda key: {
        'iib_konflux_cluster_url': 'https://api.example.com:6443',
        'iib_konflux_cluster_token': 'test-token',
        'iib_konflux_cluster_ca_cert': '/path/to/ca.crt',
        'iib_konflux_namespace': 'iib-tenant',
        'iib_environment_name': 'qe',
    }.get(key)

    _validate_konflux_config(conf)


@pytest.mark.parametrize('env_name', ['qe', 'stage', 'prod', 'my-env-1'])
def test_validate_konflux_config_valid_environment_name_variants(env_name):
    """Test that alphanumeric and hyphenated environment names are accepted."""
    conf = mock.Mock()
    conf.get.side_effect = lambda key: {
        'iib_konflux_cluster_url': 'https://api.example.com:6443',
        'iib_konflux_cluster_token': 'test-token',
        'iib_konflux_cluster_ca_cert': '/path/to/ca.crt',
        'iib_konflux_namespace': 'iib-tenant',
        'iib_environment_name': env_name,
    }.get(key)

    _validate_konflux_config(conf)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `tox -e py312 -- tests/test_workers/test_config.py::test_validate_konflux_config_invalid_environment_name -v`
Expected: FAIL — validation doesn't check `iib_environment_name` yet

- [ ] **Step 3: Add config field and validation**

In `iib/workers/config.py`, add the field after line 176 (after `iib_konflux_pipeline_timeout`):

```python
iib_environment_name: Optional[str] = None
```

In `_validate_konflux_config()`, add after the existing `_validate_konflux_fields()` call (around line 552):

```python
env_name = conf.get('iib_environment_name')
if not env_name or not isinstance(env_name, str):
    raise ConfigError(
        'iib_environment_name must be set when using Konflux configuration'
    )
import re
if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$', env_name):
    raise ConfigError(
        'iib_environment_name must be a non-empty string containing only '
        'alphanumeric characters and hyphens when using Konflux configuration'
    )
```

In `docker/containerized/worker_config.py`, add after the existing Konflux config block (around line 28):

```python
iib_environment_name: Optional[str] = os.getenv('IIB_ENVIRONMENT_NAME')
```

Also add `'iib_environment_name': cls.iib_environment_name,` to the `required_configs` dict in the `validate()` method (around line 99).

- [ ] **Step 4: Update existing valid config test**

The existing `test_validate_konflux_config_valid_config` test must also include `iib_environment_name` or it will now fail. Update the `conf.get.side_effect` dict to include `'iib_environment_name': 'qe'`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `tox -e py312 -- tests/test_workers/test_config.py -k "konflux" -v`
Expected: All Konflux config tests PASS

- [ ] **Step 6: Commit**

```bash
git add iib/workers/config.py docker/containerized/worker_config.py tests/test_workers/test_config.py
git commit -m "feat: add iib_environment_name config for multi-cluster Konflux routing"
```

---

### Task 2: Add `merge_mr()` to git_utils and update `create_mr()` branch naming

**Files:**
- Modify: `iib/workers/tasks/git_utils.py:347-389` (modify `create_mr`)
- Modify: `iib/workers/tasks/git_utils.py` (add `merge_mr`, add `_merge_gitlab_mr`)
- Test: `tests/test_workers/test_tasks/test_git_utils.py`

**Interfaces:**
- Consumes: `get_worker_config().iib_environment_name` from Task 1
- Produces:
  - `create_mr(request_id: int, local_repo_path: str, repo_url: str, branch: str, commit_message: Optional[str] = None, environment_name: Optional[str] = None) -> Dict[str, str]` — updated signature with `environment_name` kwarg
  - `merge_mr(mr_details: Dict[str, str], repo_url: str) -> str` — new function, returns merge commit SHA
  - `_merge_gitlab_mr(repo_url: str, git_token: str, mr_id: str) -> str` — internal, returns merge commit SHA

- [ ] **Step 1: Write failing test for `create_mr` with environment_name**

Add to `tests/test_workers/test_tasks/test_git_utils.py`:

```python
@mock.patch('iib.workers.tasks.git_utils._create_gitlab_mr')
@mock.patch('iib.workers.tasks.git_utils.get_git_token')
@mock.patch('iib.workers.tasks.git_utils.commit_and_push')
@mock.patch('iib.workers.tasks.git_utils.run_cmd')
def test_create_mr_with_environment_name(
    mock_run_cmd, mock_commit_and_push, mock_get_git_token, mock_create_gitlab_mr
):
    """Test that create_mr uses environment-prefixed branch names."""
    mock_get_git_token.return_value = (PUB_TOKEN_NAME, PUB_TOKEN_VALUE)
    mock_run_cmd.return_value = "Success"
    mock_commit_and_push.return_value = None
    mock_create_gitlab_mr.return_value = {
        'mr_id': '123',
        'mr_url': 'https://my-gitlab-instance.com/project/merge_requests/123',
        'source_branch': 'iib-qe-request-456-v4.19',
    }

    with tempfile.TemporaryDirectory(prefix="test-git-repo") as test_repo:
        run_cmd(f"git -C {test_repo} init".split(), strict=False)
        run_cmd(f"git -C {test_repo} config user.name 'Test'".split(), strict=False)
        run_cmd(f"git -C {test_repo} config user.email 'test@example.com'".split(), strict=False)

        with open(f"{test_repo}/test.txt", "w") as f:
            f.write("test content")

        run_cmd(f"git -C {test_repo} add test.txt".split(), strict=False)

        result = git_utils.create_mr(
            request_id=456,
            local_repo_path=test_repo,
            repo_url=PUB_GIT_REPO,
            branch="v4.19",
            commit_message="Test commit",
            environment_name="qe",
        )

        mock_run_cmd.assert_any_call(
            ["git", "-C", test_repo, "checkout", "-b", "iib-qe-request-456-v4.19"],
            exc_msg="Error creating feature branch",
        )

        mock_commit_and_push.assert_called_once_with(
            request_id=456,
            local_repo_path=test_repo,
            repo_url=PUB_GIT_REPO,
            branch="iib-qe-request-456-v4.19",
            commit_message="Test commit",
        )

        mock_create_gitlab_mr.assert_called_once_with(
            PUB_GIT_REPO, PUB_TOKEN_VALUE, "iib-qe-request-456-v4.19", "v4.19", 456
        )
```

- [ ] **Step 2: Write failing tests for `merge_mr`**

```python
@mock.patch('iib.workers.tasks.git_utils._merge_gitlab_mr')
@mock.patch('iib.workers.tasks.git_utils.get_git_token')
def test_merge_mr_success(mock_get_git_token, mock_merge_gitlab_mr):
    """Test successful merging of merge request."""
    mock_get_git_token.return_value = (PUB_TOKEN_NAME, PUB_TOKEN_VALUE)
    mock_merge_gitlab_mr.return_value = 'abc123def456'

    mr_details = {
        'mr_id': '123',
        'mr_url': 'https://my-gitlab-instance.com/project/merge_requests/123',
        'source_branch': 'iib-qe-request-456-v4.19',
    }

    result = git_utils.merge_mr(mr_details, PUB_GIT_REPO)

    assert result == 'abc123def456'
    mock_merge_gitlab_mr.assert_called_once_with(PUB_GIT_REPO, PUB_TOKEN_VALUE, '123')


@mock.patch('iib.workers.tasks.git_utils._merge_gitlab_mr')
@mock.patch('iib.workers.tasks.git_utils.get_git_token')
def test_merge_mr_missing_mr_id(mock_get_git_token, mock_merge_gitlab_mr):
    """Test merge_mr raises when mr_id is missing."""
    mr_details = {'mr_url': 'https://example.com/merge_requests/123'}

    with pytest.raises(IIBError, match="Missing mr_id"):
        git_utils.merge_mr(mr_details, PUB_GIT_REPO)

    mock_merge_gitlab_mr.assert_not_called()


@mock.patch('iib.workers.tasks.git_utils.requests_session')
@mock.patch('iib.workers.tasks.git_utils._extract_gitlab_info')
def test_merge_gitlab_mr_success(mock_extract, mock_session):
    """Test successful GitLab merge API call."""
    mock_extract.return_value = ('https://gitlab.example.com/api/v4', 'group/project')
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_response.json.return_value = {
        'merge_commit_sha': 'abc123def456',
    }
    mock_session.put.return_value = mock_response

    result = git_utils._merge_gitlab_mr(PUB_GIT_REPO, PUB_TOKEN_VALUE, '123')

    assert result == 'abc123def456'
    mock_session.put.assert_called_once()
    call_args = mock_session.put.call_args
    assert 'merge_requests/123/merge' in call_args[0][0]
    assert call_args[1]['json']['squash'] is True


@mock.patch('iib.workers.tasks.git_utils.requests_session')
@mock.patch('iib.workers.tasks.git_utils._extract_gitlab_info')
def test_merge_gitlab_mr_conflict(mock_extract, mock_session):
    """Test GitLab merge API returns 405 (cannot be merged)."""
    mock_extract.return_value = ('https://gitlab.example.com/api/v4', 'group/project')
    mock_response = mock.Mock()
    mock_response.ok = False
    mock_response.status_code = 405
    mock_response.text = 'Method Not Allowed'
    mock_session.put.return_value = mock_response

    with pytest.raises(IIBError, match="Failed to merge"):
        git_utils._merge_gitlab_mr(PUB_GIT_REPO, PUB_TOKEN_VALUE, '123')


@mock.patch('iib.workers.tasks.git_utils.time.sleep')
@mock.patch('iib.workers.tasks.git_utils.requests_session')
@mock.patch('iib.workers.tasks.git_utils._extract_gitlab_info')
def test_merge_gitlab_mr_retries_on_transient_failure(mock_extract, mock_session, mock_sleep):
    """Test GitLab merge API retries on 5xx and 409 errors."""
    mock_extract.return_value = ('https://gitlab.example.com/api/v4', 'group/project')

    transient_response = mock.Mock()
    transient_response.ok = False
    transient_response.status_code = 500
    transient_response.text = 'Internal Server Error'

    success_response = mock.Mock()
    success_response.ok = True
    success_response.json.return_value = {'merge_commit_sha': 'abc123'}

    mock_session.put.side_effect = [transient_response, success_response]

    result = git_utils._merge_gitlab_mr(PUB_GIT_REPO, PUB_TOKEN_VALUE, '123')

    assert result == 'abc123'
    assert mock_session.put.call_count == 2
    mock_sleep.assert_called_once()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `tox -e py312 -- tests/test_workers/test_tasks/test_git_utils.py -k "environment_name or merge_mr" -v`
Expected: FAIL — `environment_name` kwarg doesn't exist, `merge_mr` doesn't exist

- [ ] **Step 4: Implement `create_mr` environment_name support**

In `iib/workers/tasks/git_utils.py`, modify `create_mr()` at line 347:

```python
@instrument_tracing(span_name="workers.tasks.git_utils.create_mr")
def create_mr(
    request_id: int,
    local_repo_path: str,
    repo_url: str,
    branch: str,
    commit_message: Optional[str] = None,
    environment_name: Optional[str] = None,
) -> Dict[str, str]:
```

Change the feature branch name at line 371 from:

```python
feature_branch = f"iib-request-{request_id}-{branch}"
```

To:

```python
if environment_name:
    feature_branch = f"iib-{environment_name}-request-{request_id}-{branch}"
else:
    feature_branch = f"iib-request-{request_id}-{branch}"
```

- [ ] **Step 5: Implement `merge_mr` and `_merge_gitlab_mr`**

Add `import time` to the imports at the top of `git_utils.py` (if not already present).

Add after the `close_mr` function (after line 544):

```python
@instrument_tracing(span_name="workers.tasks.git_utils.merge_mr")
def merge_mr(mr_details: Dict[str, str], repo_url: str) -> str:
    """
    Merge a merge request on GitLab repository.

    :param dict mr_details: Dictionary containing MR details (mr_id, mr_url, source_branch).
    :param str repo_url: Git repository URL.
    :return: The merge commit SHA.
    :rtype: str
    :raises IIBError: If GitLab API call fails.
    """
    mr_id = mr_details.get('mr_id')
    if not mr_id:
        raise IIBError("Missing mr_id in mr_details")

    _, git_token = get_git_token(repo_url)

    return _merge_gitlab_mr(repo_url, git_token, mr_id)


def _merge_gitlab_mr(repo_url: str, git_token: str, mr_id: str, max_retries: int = 3) -> str:
    """
    Merge a merge request using GitLab API with retry for transient failures.

    Retryable status codes: 409 (merge in progress), 500, 502, 503, 504.
    Non-retryable: 401, 403, 405 (cannot be merged, e.g., conflict).

    :param str repo_url: Git repository URL.
    :param str git_token: GitLab access token.
    :param str mr_id: Merge request ID.
    :param int max_retries: Maximum number of retry attempts for transient failures.
    :return: The merge commit SHA.
    :rtype: str
    :raises IIBError: If GitLab API call fails permanently.
    """
    api_url, project_path = _extract_gitlab_info(repo_url)

    merge_url = f"{api_url}/projects/{quote_plus(project_path)}/merge_requests/{mr_id}/merge"

    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {git_token}'}

    payload = {'squash': True}

    retryable_codes = {409, 500, 502, 503, 504}

    for attempt in range(max_retries + 1):
        try:
            log.info("Merging merge request %s via GitLab API (attempt %d)", mr_id, attempt + 1)
            response = requests_session.put(merge_url, headers=headers, json=payload, timeout=30)

            if response.ok:
                mr_data = response.json()
                merge_commit_sha = mr_data.get('merge_commit_sha', '')
                log.info(
                    "Successfully merged merge request %s, commit: %s", mr_id, merge_commit_sha
                )
                return merge_commit_sha

            if response.status_code in retryable_codes and attempt < max_retries:
                wait_time = 5 * (attempt + 1)
                log.warning(
                    'Transient error merging MR %s (HTTP %d). Retrying in %ds...',
                    mr_id,
                    response.status_code,
                    wait_time,
                )
                time.sleep(wait_time)
                continue

            log.error(
                'Failed to merge merge request. Status: %d, Response: %s',
                response.status_code,
                response.text,
            )
            raise IIBError(
                f'Failed to merge merge request {mr_id}: HTTP {response.status_code}'
            )

        except requests.RequestException as e:
            if attempt < max_retries:
                wait_time = 5 * (attempt + 1)
                log.warning(
                    'Network error merging MR %s: %s. Retrying in %ds...',
                    mr_id,
                    e,
                    wait_time,
                )
                time.sleep(wait_time)
                continue
            log.exception("Error merging merge request via GitLab API")
            raise IIBError(f'GitLab API request failed: {str(e)}')

    raise IIBError(f'Failed to merge merge request {mr_id} after {max_retries + 1} attempts')
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `tox -e py312 -- tests/test_workers/test_tasks/test_git_utils.py -k "environment_name or merge_mr" -v`
Expected: PASS

- [ ] **Step 7: Run full git_utils test suite for regressions**

Run: `tox -e py312 -- tests/test_workers/test_tasks/test_git_utils.py -v`
Expected: All tests PASS (existing `test_create_mr_success` still passes since `environment_name` defaults to `None`)

- [ ] **Step 8: Commit**

```bash
git add iib/workers/tasks/git_utils.py tests/test_workers/test_tasks/test_git_utils.py
git commit -m "feat: add merge_mr and environment-prefixed branch naming to git_utils"
```

---

### Task 3: Unify `git_commit_and_create_mr_or_push()` to always use MRs and add `merge_mr_after_build()`

**Files:**
- Modify: `iib/workers/tasks/containerized_utils.py:18-27` (update imports)
- Modify: `iib/workers/tasks/containerized_utils.py:570-622` (modify `git_commit_and_create_mr_or_push`)
- Modify: `iib/workers/tasks/containerized_utils.py:419-494` (simplify `cleanup_on_failure`)
- Add function: `merge_mr_after_build()` in `containerized_utils.py`
- Test: `tests/test_workers/test_tasks/test_containerized_utils.py`

**Interfaces:**
- Consumes:
  - `create_mr(request_id, local_repo_path, repo_url, branch, commit_message, environment_name)` from Task 2
  - `merge_mr(mr_details, repo_url) -> str` from Task 2
  - `get_worker_config().iib_environment_name` from Task 1
- Produces:
  - `git_commit_and_create_mr_or_push(request_id, local_git_repo_path, index_git_repo, branch, commit_message, overwrite_from_index=False) -> Tuple[Dict[str, str], str]` — signature unchanged, but now always returns non-None `mr_details` dict
  - `merge_mr_after_build(mr_details, index_git_repo) -> str` — new function, returns merge commit SHA, closes MR and raises on failure

- [ ] **Step 1: Write failing test for unified MR flow**

Add to `tests/test_workers/test_tasks/test_containerized_utils.py`:

```python
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.create_mr')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
def test_git_commit_always_creates_mr_for_overwrite(
    mock_config, mock_set_state, mock_create_mr, mock_get_sha
):
    """Test that git_commit_and_create_mr_or_push always creates MR even with overwrite=True."""
    mock_config.return_value = mock.Mock()
    mock_config.return_value.get.return_value = 'qe'
    mock_create_mr.return_value = {
        'mr_id': '123',
        'mr_url': 'https://gitlab.example.com/merge_requests/123',
        'source_branch': 'iib-qe-request-1-v4.14',
    }
    mock_get_sha.return_value = 'abc123'

    mr_details, sha = git_commit_and_create_mr_or_push(
        request_id=1,
        local_git_repo_path='/tmp/repo',
        index_git_repo='https://gitlab.example.com/project',
        branch='v4.14',
        commit_message='test',
        overwrite_from_index=True,
    )

    assert mr_details is not None
    assert mr_details['mr_id'] == '123'
    mock_create_mr.assert_called_once_with(
        request_id=1,
        local_repo_path='/tmp/repo',
        repo_url='https://gitlab.example.com/project',
        branch='v4.14',
        commit_message='test',
        environment_name='qe',
    )
```

- [ ] **Step 2: Write failing test for `merge_mr_after_build`**

```python
@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.merge_mr')
def test_merge_mr_after_build_success(mock_merge_mr, mock_close_mr):
    """Test successful MR merge after build."""
    mock_merge_mr.return_value = 'merge_sha_123'
    mr_details = {'mr_id': '123', 'mr_url': 'https://example.com/mr/123'}

    result = merge_mr_after_build(mr_details, 'https://gitlab.example.com/project')

    assert result == 'merge_sha_123'
    mock_merge_mr.assert_called_once_with(mr_details, 'https://gitlab.example.com/project')
    mock_close_mr.assert_not_called()


@mock.patch('iib.workers.tasks.containerized_utils.close_mr')
@mock.patch('iib.workers.tasks.containerized_utils.merge_mr')
def test_merge_mr_after_build_failure_closes_mr(mock_merge_mr, mock_close_mr):
    """Test that merge failure closes the MR and raises IIBError."""
    mock_merge_mr.side_effect = IIBError("Failed to merge")
    mr_details = {'mr_id': '123', 'mr_url': 'https://example.com/mr/123'}

    with pytest.raises(IIBError, match="Failed to merge MR after successful build"):
        merge_mr_after_build(mr_details, 'https://gitlab.example.com/project')

    mock_close_mr.assert_called_once_with(mr_details, 'https://gitlab.example.com/project')
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `tox -e py312 -- tests/test_workers/test_tasks/test_containerized_utils.py -k "always_creates_mr or merge_mr_after_build" -v`
Expected: FAIL

- [ ] **Step 4: Update imports in containerized_utils.py**

In `iib/workers/tasks/containerized_utils.py`, add `merge_mr` to the `git_utils` imports (line 18-27):

```python
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    close_mr,
    commit_and_push,
    create_mr,
    get_git_token,
    get_last_commit_sha,
    merge_mr,
    resolve_git_url,
    revert_last_commit,
)
```

- [ ] **Step 5: Implement unified `git_commit_and_create_mr_or_push`**

Replace the function body at line 570-622 with:

```python
def git_commit_and_create_mr_or_push(
    request_id: int,
    local_git_repo_path: str,
    index_git_repo: str,
    branch: str,
    commit_message: str,
    overwrite_from_index: bool = False,
) -> Tuple[Dict[str, str], str]:
    """
    Commit changes and trigger Konflux pipeline by creating an MR.

    All requests (both overwrite and throw-away) go through the MR path.
    The environment name from worker config is used to prefix the source branch,
    enabling PaC CEL expression routing to the correct Konflux tenant.

    :param int request_id: The IIB request ID
    :param str local_git_repo_path: Path to local Git repository
    :param str index_git_repo: URL of the Git repository
    :param str branch: Git branch name
    :param str commit_message: Commit message to use
    :param bool overwrite_from_index: Whether to overwrite from_index (kept for API compat)
    :return: Tuple of (mr_details, last_commit_sha)
    :rtype: Tuple[Dict[str, str], str]
    """
    set_request_state(request_id, 'in_progress', 'Committing changes to Git repository')
    log.info("Committing changes to Git repository. Triggering KONFLUX pipeline.")

    conf = get_worker_config()
    environment_name = conf.get('iib_environment_name')

    mr_details = create_mr(
        request_id=request_id,
        local_repo_path=local_git_repo_path,
        repo_url=index_git_repo,
        branch=branch,
        commit_message=commit_message,
        environment_name=environment_name,
    )
    log.info("Created merge request: %s", mr_details.get('mr_url'))

    last_commit_sha = get_last_commit_sha(local_repo_path=local_git_repo_path)

    return mr_details, last_commit_sha
```

- [ ] **Step 6: Implement `merge_mr_after_build`**

Add after the `cleanup_merge_request_if_exists` function:

```python
def merge_mr_after_build(
    mr_details: Dict[str, str],
    index_git_repo: str,
) -> str:
    """
    Merge the MR after a successful Konflux build (overwrite flow).

    On merge failure, closes the MR and raises IIBError so the request
    is marked as failed.

    :param Dict[str, str] mr_details: Details of the merge request
    :param str index_git_repo: URL of the Git repository
    :return: The merge commit SHA
    :rtype: str
    :raises IIBError: If the merge fails
    """
    try:
        merge_commit_sha = merge_mr(mr_details, index_git_repo)
        log.info("Successfully merged MR %s, commit: %s", mr_details.get('mr_id'), merge_commit_sha)
        return merge_commit_sha
    except IIBError as e:
        log.error("Failed to merge MR %s after build: %s", mr_details.get('mr_id'), e)
        try:
            close_mr(mr_details, index_git_repo)
            log.info("Closed MR %s after merge failure", mr_details.get('mr_id'))
        except Exception as close_error:
            log.warning("Failed to close MR after merge failure: %s", close_error)
        raise IIBError(f"Failed to merge MR after successful build: {e}")
```

- [ ] **Step 7: Simplify `cleanup_on_failure`**

In `cleanup_on_failure()` (line 419), the `elif overwrite_from_index and last_commit_sha` branch is now dead code since `mr_details` is always set. Keep it for backward safety but add a log noting it's unexpected:

Replace the `elif` block (approximately lines 456-466):

```python
elif overwrite_from_index and last_commit_sha:
    log.warning(
        "Unexpected: overwrite_from_index cleanup without mr_details. "
        "This code path should not be reached with the unified MR flow."
    )
    log.error("Reverting commit due to %s", reason)
    try:
        revert_last_commit(
            request_id=request_id,
            from_index=from_index,
            index_repo_map=index_repo_map,
        )
    except Exception as revert_error:
        log.error("Failed to revert commit: %s", revert_error)
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `tox -e py312 -- tests/test_workers/test_tasks/test_containerized_utils.py -v`
Expected: All tests PASS

- [ ] **Step 9: Commit**

```bash
git add iib/workers/tasks/containerized_utils.py tests/test_workers/test_tasks/test_containerized_utils.py
git commit -m "feat: unify git flow to always use MRs, add merge_mr_after_build"
```

---

### Task 4: Update build handlers to insert merge step for overwrite flow

**Files:**
- Modify: `iib/workers/tasks/build_containerized_add.py:286-372`
- Modify: `iib/workers/tasks/build_containerized_rm.py:230-322`
- Modify: `iib/workers/tasks/build_containerized_fbc_operations.py:180-267`
- Modify: `iib/workers/tasks/build_containerized_create_empty_index.py:294-378`
- Modify: `iib/workers/tasks/build_containerized_merge.py:307-397`
- Modify: `iib/workers/tasks/build_containerized_regenerate_bundle.py:205-281` (env prefix only, no merge)
- Test: `tests/test_workers/test_tasks/test_build_containerized_add.py`
- Test: `tests/test_workers/test_tasks/test_build_containerized_rm.py`
- Test: `tests/test_workers/test_tasks/test_build_containerized_fbc_operations.py`
- Test: `tests/test_workers/test_tasks/test_build_containerized_create_empty_index.py`
- Test: `tests/test_workers/test_tasks/test_build_containerized_merge.py`
- Test: `tests/test_workers/test_tasks/test_build_containerized_regenerate_bundle.py`

**Interfaces:**
- Consumes:
  - `merge_mr_after_build(mr_details, index_git_repo) -> str` from Task 3
  - `git_commit_and_create_mr_or_push()` now always returns non-None `mr_details` from Task 3
- Produces: Updated handler functions with correct post-build ordering

**Note:** The pattern is the same across add, rm, fbc_operations, merge, and create_empty_index. Only regenerate_bundle differs (no merge, no index.db push). The `merge` handler uses `overwrite_target_index` instead of `overwrite_from_index`.

- [ ] **Step 1: Add `merge_mr_after_build` import to all handlers**

In each of the five handlers that may need merging (add, rm, fbc_operations, create_empty_index, merge), add `merge_mr_after_build` to the import from `containerized_utils`:

```python
from iib.workers.tasks.containerized_utils import (
    ...
    merge_mr_after_build,
    ...
)
```

`build_containerized_regenerate_bundle.py` does NOT need this import.

- [ ] **Step 2: Update `build_containerized_add.py` post-build flow**

In `build_containerized_add.py`, after `monitor_pipeline_and_extract_image()` (line 301), insert the merge step before `replicate_image_to_tagged_destinations`. The new ordering between the `try` and `except` blocks:

```python
        try:
            mr_details, last_commit_sha = git_commit_and_create_mr_or_push(
                request_id=request_id,
                local_git_repo_path=local_git_repo_path,
                index_git_repo=index_git_repo,
                branch=branch,
                commit_message=(
                    f"IIB: Add bundles for request {request_id}\n\n"
                    f"Bundles: {', '.join(bundles)}"
                ),
                overwrite_from_index=overwrite_from_index,
            )

            image_url = monitor_pipeline_and_extract_image(
                request_id=request_id,
                last_commit_sha=last_commit_sha,
            )

            # Merge MR if this is an overwrite request (source of truth update)
            if overwrite_from_index:
                merge_mr_after_build(mr_details, index_git_repo)

            output_pull_specs = replicate_image_to_tagged_destinations(
                request_id=request_id,
                image_url=image_url,
                build_tags=build_tags,
            )

            output_pull_spec = output_pull_specs[0]
            if not output_pull_spec:
                raise IIBError(
                    "output_pull_spec was not set. "
                    "This should not happen if the pipeline completed successfully."
                )

            _update_index_image_pull_spec(
                output_pull_spec=output_pull_spec,
                request_id=request_id,
                arches=arches,
                from_index=from_index,
                overwrite_from_index=overwrite_from_index,
                overwrite_from_index_token=overwrite_from_index_token,
                resolved_prebuild_from_index=from_index_resolved,
                add_or_rm=True,
                is_image_fbc=True,
                index_repo_map={},
            )

            original_index_db_digest = push_index_db_artifact(
                request_id=request_id,
                from_index=str(from_index),
                index_db_path=artifact_index_db_file,
                operators=operators,
                overwrite_from_index=overwrite_from_index,
                request_type='add',
            )

            # Close MR for throw-away requests (overwrite MRs were already merged)
            if not overwrite_from_index:
                cleanup_merge_request_if_exists(mr_details, index_git_repo)

            set_request_state(
                request_id,
                'complete',
                'The operator bundle(s) were successfully added to the index image',
            )
        except Exception as e:
            cleanup_on_failure(
                mr_details=mr_details,
                last_commit_sha=last_commit_sha,
                index_git_repo=index_git_repo,
                overwrite_from_index=overwrite_from_index,
                request_id=request_id,
                from_index=str(from_index),
                index_repo_map=index_to_gitlab_push_map or {},
                original_index_db_digest=original_index_db_digest,
                reason=f"error: {e}",
            )
            raise IIBError(f"Failed to add bundles: {e}")
```

- [ ] **Step 3: Apply the same pattern to `build_containerized_rm.py`**

Same change: insert `if overwrite_from_index: merge_mr_after_build(mr_details, index_git_repo)` after `monitor_pipeline_and_extract_image()`, and change `cleanup_merge_request_if_exists(mr_details, index_git_repo)` to `if not overwrite_from_index: cleanup_merge_request_if_exists(mr_details, index_git_repo)`.

- [ ] **Step 4: Apply the same pattern to `build_containerized_fbc_operations.py`**

Same change as Steps 2-3.

- [ ] **Step 5: Apply the pattern to `build_containerized_create_empty_index.py`**

This handler always has `overwrite_from_index=False`, so the merge step will never execute. Still add it for consistency with the conditional guard:

```python
if overwrite_from_index:
    merge_mr_after_build(mr_details, index_git_repo)
```

And change the cleanup to:

```python
if not overwrite_from_index:
    cleanup_merge_request_if_exists(mr_details, index_git_repo)
```

Since `overwrite_from_index` is always `False` here, the behavior is identical to before.

- [ ] **Step 6: Apply the pattern to `build_containerized_merge.py`**

This handler uses `overwrite_target_index` instead of `overwrite_from_index`. The merge step uses that variable:

```python
if overwrite_target_index:
    merge_mr_after_build(mr_details, index_git_repo)
```

And the cleanup:

```python
if not overwrite_target_index:
    cleanup_merge_request_if_exists(mr_details, index_git_repo)
```

- [ ] **Step 7: Update `build_containerized_regenerate_bundle.py` (env prefix only)**

No merge step — this handler is always throw-away. The only change is that `git_commit_and_create_mr_or_push()` now always creates MRs (which it already did for regenerate_bundle since `overwrite_from_index=False`). No code changes are needed in this file beyond what Task 3 already provides. Verify the import of `merge_mr_after_build` is NOT added.

- [ ] **Step 8: Write tests for overwrite merge flow in `test_build_containerized_add.py`**

Add a test that verifies `merge_mr_after_build` is called when `overwrite_from_index=True` and NOT called when `False`. The test should mock the entire post-build chain:

```python
@mock.patch('iib.workers.tasks.build_containerized_add.cleanup_merge_request_if_exists')
@mock.patch('iib.workers.tasks.build_containerized_add.merge_mr_after_build')
@mock.patch('iib.workers.tasks.build_containerized_add.push_index_db_artifact')
@mock.patch('iib.workers.tasks.build_containerized_add._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_containerized_add.replicate_image_to_tagged_destinations')
@mock.patch('iib.workers.tasks.build_containerized_add.monitor_pipeline_and_extract_image')
@mock.patch('iib.workers.tasks.build_containerized_add.git_commit_and_create_mr_or_push')
def test_add_overwrite_merges_mr_after_build(
    mock_git_commit, mock_monitor, mock_replicate, mock_update, mock_push_db,
    mock_merge_mr, mock_cleanup_mr,
    # ... other fixtures for the full handler setup
):
    """Test that overwrite=True triggers merge_mr_after_build."""
    mr_details = {'mr_id': '1', 'mr_url': 'https://example.com/mr/1', 'source_branch': 'iib-qe-request-1-v4.14'}
    mock_git_commit.return_value = (mr_details, 'abc123')
    mock_monitor.return_value = 'quay.io/built-image:latest'
    mock_replicate.return_value = ['registry:8443/iib-build:1']
    mock_push_db.return_value = None

    # ... call handler with overwrite_from_index=True ...

    mock_merge_mr.assert_called_once_with(mr_details, index_git_repo)
    mock_cleanup_mr.assert_not_called()
```

Write a corresponding test with `overwrite_from_index=False` that asserts `merge_mr_after_build` is NOT called and `cleanup_merge_request_if_exists` IS called.

Follow the existing test patterns in the test file for setting up the full handler mock chain. Each handler test file will need a similar pair of tests.

- [ ] **Step 9: Run all handler tests**

Run: `tox -e py312 -- tests/test_workers/test_tasks/test_build_containerized_add.py tests/test_workers/test_tasks/test_build_containerized_rm.py tests/test_workers/test_tasks/test_build_containerized_fbc_operations.py tests/test_workers/test_tasks/test_build_containerized_create_empty_index.py tests/test_workers/test_tasks/test_build_containerized_merge.py tests/test_workers/test_tasks/test_build_containerized_regenerate_bundle.py -v`
Expected: All tests PASS

- [ ] **Step 10: Commit**

```bash
git add iib/workers/tasks/build_containerized_*.py tests/test_workers/test_tasks/test_build_containerized_*.py
git commit -m "feat: insert merge step in overwrite flow for multi-cluster Konflux routing"
```

---

### Task 5: Full integration test and static analysis pass

**Files:**
- Test: all test files from Tasks 1-4
- Lint: all modified source files

**Interfaces:**
- Consumes: all changes from Tasks 1-4
- Produces: clean test and lint results

- [ ] **Step 1: Run full test suite**

Run: `tox -e py312 -v`
Expected: All tests PASS

- [ ] **Step 2: Run static analysis**

Run: `tox -m static`
Expected: black, flake8, yamllint, mypy all PASS

- [ ] **Step 3: Fix any failures**

If any test or linter fails, fix the issue and re-run.

- [ ] **Step 4: Final commit if fixes were needed**

```bash
git add -u
git commit -m "fix: address lint and test issues from multi-cluster Konflux changes"
```
