# CLAUDE.md — IIB (IIB Image Builder)

## Project overview

IIB is a REST API service for managing operator index container images and bundle images
in the Red Hat container ecosystem. It is a Python 3.12+ application with two main layers
and a shared utility package:

- **`iib/web/`** — Flask REST API. Handles HTTP requests, authentication (Kerberos via
  `flask-login`), input validation, SQLAlchemy models, and database migrations (Alembic).
  Entry point: `iib/web/app.py`.
- **`iib/workers/`** — Celery worker layer. Executes long-running build tasks
  asynchronously. Orchestrates container tools (`podman`, `buildah`, `opm`, `skopeo`),
  interacts with container registries, and manages Git-based FBC (file-based catalog)
  workflows. Task entry points are in `iib/workers/tasks/`.
- **`iib/common/`** — Shared utilities (tracing, binary version helpers).

The web layer validates and enqueues requests; the worker layer executes them. They
communicate through Celery (RabbitMQ) and share the database via the REST API
(`iib/workers/api_utils.py` calls back to the web API to update request state).

## Code style

### Formatting and linting

- **`black`** enforces formatting with `line-length = 100` and
  `skip-string-normalization = true` (single quotes preferred over double quotes).
- **`flake8`** enforces PEP 8 with `max-line-length = 100`. The `flake8-docstrings`
  plugin is enabled with exemptions for D100, D104, D105.
- **`mypy`** runs with `--ignore-missing-imports` on the `iib/` package.
- **`yamllint`** checks all YAML files (max line length 100, warning level).
- **`hadolint`** lints Dockerfiles with `--failure-threshold warning`.
- **`bandit`** runs static application security testing.

Run all static checks: `tox -m static` (runs `black`, `flake8`, `yamllint`, `mypy`,
`hadolint`).

### Docstrings

Use reStructuredText format:

```python
"""
Get the IIB build request from the REST API.

:param int request_id: the ID of the IIB request
:return: the request
:rtype: dict
:raises IIBError: if the HTTP request fails
"""
```

### License headers

Every Python source file starts with `# SPDX-License-Identifier: GPL-3.0-or-later`.

### Naming conventions

- Functions that sanitize sensitive data from values use prefixes like `_sanitize_`
  (e.g., `_sanitize_cmd_log`) or `_get_safe_` (e.g., `_get_safe_args`). These naming
  patterns indicate credential-redaction logic — treat them as security-sensitive.
- Validation helpers use `_is_safe_` prefix (e.g., `_is_safe_operator_package_name`).

### Import organization

Standard library imports first, then third-party, then `iib.*` internal imports.
Each group is separated by a blank line. Import ordering is a manual convention
(no automated import sorter is configured).

## Security patterns

### Credential redaction

All subprocess commands that may contain credentials (Git URLs with tokens, registry
auth) must be logged through `_sanitize_cmd_log` (in `iib/workers/tasks/utils.py`),
which strips embedded credentials from URLs before logging.

API request arguments containing tokens (`cnr_token`, `overwrite_from_index_token`,
`overwrite_target_index_token`, `registry_auths`) are redacted via `_get_safe_args`
(in `iib/web/api_v1.py`) before logging.

When reviewing or writing code that logs commands or request parameters:
- Never log raw token values or registry auth credentials.
- Always use the existing sanitization helpers rather than ad-hoc redaction.
- Test that log output does not contain credential material.

### Authentication

The web API uses Kerberos authentication via `flask-login`. Worker identity is
validated separately. See `iib/web/auth.py` for the authentication setup.

## Test conventions

### Structure

- **`tests/test_web/`** — tests for the Flask web layer.
- **`tests/test_workers/`** — tests for the Celery worker layer.
  - **`tests/test_workers/test_tasks/`** — tests for individual build task modules.

Test files mirror the source structure: `iib/web/api_v1.py` is tested in
`tests/test_web/test_api_v1.py`, `iib/workers/tasks/utils.py` is tested in
`tests/test_workers/test_tasks/test_utils.py`.

### Patterns

- Tests use `pytest` with `unittest.mock` for mocking (`@mock.patch` decorators).
- Fixtures are defined in `tests/conftest.py` (app, db, client, auth environments).
- `@pytest.mark.parametrize` is used extensively for testing multiple input
  combinations.
- The `app` fixture creates a Flask application context; `db` provides a clean
  database per test using SQLite and Alembic migrations.
- D103 (missing docstring) is exempted for all test files.
- Coverage threshold is 90% (`fail_under = 90` in `.coveragerc`).

### Running tests

```bash
# Run all tests and static checks
tox

# Run only unit tests
tox -m test

# Run only static linters
tox -m static

# Run a specific test
tox -e py312 tests/test_web/test_api_v1.py::test_add_bundle_invalid_param

# Run security checks (bandit)
tox -m security
```

## Review preferences

### Findings that are useful

- Correctness bugs (wrong logic, missing error handling, race conditions).
- Security issues (credential leakage in logs, missing input validation,
  authentication bypasses).
- Missing or incorrect test coverage for changed behavior.
- API contract violations (changed response shapes, missing required fields).

### Findings that are noise for this project

- **String quoting style**: `black` with `skip-string-normalization = true` means
  single quotes are the project convention. Do not flag single-vs-double quote
  choices — `black` enforces this automatically.
- **Docstring format preferences**: The project uses reStructuredText style. Do not
  suggest switching to Google or NumPy style.
- **Import ordering**: Import order follows a manual convention (stdlib → third-party
  → `iib.*`). No automated sorter is configured. Do not flag import order.
- **Linked issues in PRs**: Not every PR requires a linked issue. Do not flag
  missing issue references as a review finding.
- **Scope bundling**: Cross-cutting changes (e.g., security hardening across
  multiple files) are acceptable in a single PR. Do not flag these as scope
  bundling unless the changes are genuinely unrelated.
- **Generic naming suggestions** (e.g., rename `x` to `y` for clarity) unless
  the name is actively misleading.
- **Pattern inconsistency** across files that predate the change under review.
  Only flag inconsistency introduced by the current PR.

## Build and CI

### Tox environments

| Label       | Environments                                    | Purpose                 |
|-------------|-------------------------------------------------|-------------------------|
| `static`    | `black`, `flake8`, `yamllint`, `mypy`, `hadolint` | Code style and types  |
| `test`      | `unit_tests`                                    | Unit tests with coverage |
| `security`  | `bandit`                                        | SAST                    |
| `docs`      | `docs`                                          | Sphinx documentation    |

CI runs on every pull request and push to `master` via `.github/workflows/run_tox.yml`.
It tests on Python 3.12 and 3.13.

### Dependency management

Dependencies are pinned with hashes via `pip-tools`. Runtime deps are in `setup.py`,
pinned to `requirements.txt`. Test deps are in `requirements-test.in`, pinned to
`requirements-test.txt`. Update with `tox -e pip-compile`.

### Database migrations

Use Alembic via Flask-Migrate. Migrations live in `iib/web/migrations/versions/`.
Generate with `tox -e migrate-db <message>`.

### Docker

Dockerfiles are in `docker/` — `Dockerfile-api` (web), `Dockerfile-workers` (workers).
Linted by `hadolint` in CI.
