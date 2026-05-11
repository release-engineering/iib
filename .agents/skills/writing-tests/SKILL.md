---
name: writing-tests
description: Use when writing or modifying unit tests for IIB web endpoints, models, or worker tasks. Triggers on "add tests", "write tests", "test coverage", or when creating new features that need test coverage.
---

## Overview

IIB tests live in `tests/test_web/` (API, models) and `tests/test_workers/` (tasks, utilities). Tests use pytest with SQLite (not PostgreSQL) and rely on shared fixtures from `tests/conftest.py`.

## Test File Placement

| What you're testing | File |
|---|---|
| API endpoints | `tests/test_web/test_api_v1.py` |
| SQLAlchemy models | `tests/test_web/test_models.py` |
| Worker task handler | `tests/test_workers/test_tasks/test_build_containerized_<type>.py` |
| Worker utilities | `tests/test_workers/test_tasks/test_utils.py` |
| Worker config | `tests/test_workers/test_config.py` |

## Key Fixtures (from `tests/conftest.py`)

```python
app          # Flask app with TestingConfig (SQLite, auth enabled)
app_no_auth  # Flask app with LOGIN_DISABLED=True
db           # Fresh database — migrations applied, no records
client       # Flask test client
auth_env     # {'REMOTE_USER': 'tbrady@DOMAIN.LOCAL'}
worker_auth_env    # {'REMOTE_USER': 'worker@DOMAIN.LOCAL'}
worker_forbidden_env  # {'REMOTE_USER': 'vkohli@DOMAIN.LOCAL'}
minimal_request_add   # RequestAdd instance committed to db
minimal_request_rm    # RequestRm instance committed to db
minimal_request_fbc_operations  # RequestFbcOperations instance committed to db
```

## API Endpoint Test Pattern

```python
def test_your_endpoint(app, auth_env, client, db):
    with app.test_request_context(environ_base=auth_env):
        # Setup: create request via model
        data = {
            'binary_image': 'quay.io/namespace/binary_image:latest',
            'from_index': 'quay.io/namespace/index:latest',
            # ...required fields
        }
        request = RequestYourType.from_json(data)
        db.session.add(request)
        db.session.commit()

    # Test: call the endpoint
    rv = client.get('/api/v1/builds/1').json
    assert rv['state'] == 'in_progress'
```

For POST endpoints, mock the celery task:

```python
@mock.patch('iib.web.api_v1.handle_containerized_your_type_request')
def test_your_post_endpoint(mock_handler, app, auth_env, client, db):
    with app.test_request_context(environ_base=auth_env):
        rv = client.post(
            '/api/v1/builds/your-type',
            json={'from_index': 'quay.io/ns/index:latest', ...},
            environ_base=auth_env,
        )
    assert rv.status_code == 201
    mock_handler.apply_async.assert_called_once()
```

## Containerized Worker Handler Test Pattern

Worker handler tests mock two categories of dependencies:
- **Handler-local functions** — mock on `iib.workers.tasks.build_containerized_<type>.<function>`
- **Shared containerized utilities** — mock on `iib.workers.tasks.containerized_utils.<function>`

```python
# Shared utilities (containerized_utils.py) — mock on containerized_utils
@mock.patch('iib.workers.tasks.containerized_utils.Path.mkdir')
@mock.patch('iib.workers.tasks.containerized_utils.Path.exists')
@mock.patch('iib.workers.tasks.containerized_utils.pull_index_db_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.clone_git_repo')
@mock.patch('iib.workers.tasks.containerized_utils.get_git_token')
@mock.patch('iib.workers.tasks.containerized_utils.commit_and_push')
@mock.patch('iib.workers.tasks.containerized_utils.get_last_commit_sha')
@mock.patch('iib.workers.tasks.containerized_utils.find_pipelinerun')
@mock.patch('iib.workers.tasks.containerized_utils.wait_for_pipeline_completion')
@mock.patch('iib.workers.tasks.containerized_utils.get_pipelinerun_image_url')
@mock.patch('iib.workers.tasks.containerized_utils.push_oras_artifact')
@mock.patch('iib.workers.tasks.containerized_utils.get_worker_config')
@mock.patch('iib.workers.tasks.containerized_utils.set_request_state')
@mock.patch('iib.workers.tasks.containerized_utils._skopeo_copy')
# Handler-local functions — mock on build_containerized_<type>
@mock.patch('iib.workers.tasks.build_containerized_your_type.cleanup_on_failure')
@mock.patch('iib.workers.tasks.build_containerized_your_type.set_request_state')
@mock.patch('iib.workers.tasks.build_containerized_your_type.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_containerized_your_type.tempfile.TemporaryDirectory')
def test_handle_containerized_your_type_request_success(
    mock_tempdir,
    mock_prfb,
    mock_srs,
    mock_cof,
    # ...remaining mock args in reverse decorator order
):
    mock_tempdir.return_value.__enter__.return_value = '/tmp/iib-1-test'
    mock_prfb.return_value = {
        'arches': {'amd64'},
        'binary_image_resolved': 'registry.io/binary@sha256:abc',
        'from_index_resolved': 'quay.io/ns/index@sha256:def',
        'ocp_version': 'v4.14',
        'distribution_scope': 'prod',
    }

    build_containerized_your_type.handle_containerized_your_type_request(
        request_id=1, from_index='quay.io/ns/index:v4.14', ...
    )

    mock_srs.assert_any_call(1, 'in_progress', mock.ANY)
    final_call = mock_srs.call_args_list[-1]
    assert final_call[0][1] == 'complete'
```

### Testing Failure Paths

Failure tests must verify `cleanup_on_failure` is called (or not, depending on where the error occurs):

```python
def test_handle_containerized_your_type_request_pipeline_failure(
    mock_cof, mock_srs, ...
):
    mock_wfpc.side_effect = IIBError('Pipeline failed')

    with pytest.raises(IIBError, match='Pipeline failed'):
        build_containerized_your_type.handle_containerized_your_type_request(...)

    # Verify cleanup WAS called (error after git push)
    mock_cof.assert_called_once()

def test_handle_containerized_your_type_request_prebuild_failure(
    mock_cof, mock_srs, ...
):
    mock_prfb.side_effect = IIBError('Image not found')

    with pytest.raises(IIBError, match='Image not found'):
        build_containerized_your_type.handle_containerized_your_type_request(...)

    # Verify cleanup was NOT called (error before try block)
    mock_cof.assert_not_called()
```

## Running Tests

```bash
tox -e py312                           # all unit tests
tox -e py312 -- tests/test_web/test_api_v1.py::test_name  # single test
tox -e py312 -- tests/test_workers/test_tasks/test_build_containerized_rm.py  # one handler's tests
tox -e py312 -- tests/test_web/ -k "add"  # keyword filter
tox -e py312 -- --no-cov tests/path    # skip coverage for speed
```

## Gotchas

- **Always use `app.test_request_context(environ_base=auth_env)`** for tests that create model instances — the app context is required for database access and `current_user`.
- **Mock celery tasks in API tests** — never let `apply_async` actually run.
- **The `db` fixture gives you a clean database** — each test starts fresh (migrations applied, no records).
- **Use `mock.patch` on the module where the function is imported**, not where it's defined (standard Python mock rules). For containerized handlers, this means shared utilities are mocked on `iib.workers.tasks.containerized_utils.<fn>`, while handler-local imports are mocked on `iib.workers.tasks.build_containerized_<type>.<fn>`.
- **`conftest.py` patches `tenacity.nap.time.sleep`** globally so retries don't actually sleep.
- **Mock args are received in reverse decorator order** — the bottom `@mock.patch` maps to the first arg after `self`/test name.
- **Always test cleanup_on_failure** — verify it's called for errors after git push, and NOT called for errors before the try block (e.g., prebuild failures).
