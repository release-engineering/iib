# IIB (Index Image Builder)

## Purpose
REST API service for managing Operator index images in container registries.
Two components connected by RabbitMQ:
- **API** (Flask + PostgreSQL): `iib/web/` — receives requests, stores state
- **Workers** (Celery): `iib/workers/tasks/` — execute builds on OpenShift

Request types (each a `POST /builds/<type>` endpoint + celery task in `iib/workers/tasks/build_containerized*.py`): add, rm, merge-index-image, regenerate-bundle, fbc-operations, create-empty-index.

## Build
Production: API and Worker images are built from Dockerfiles in `docker/containerized/` and deployed in OpenShift pods.
Local dev: (containerized workflow): See `docker/containerized/README.md` for full setup. Requires Konflux cluster credentials and GitLab tokens.
Legacy local dev: `make up` / `make down` (docker-compose) spins up api, worker, db, rabbitmq, memcached, registry (TLS), jaeger.
Entry points: WSGI `iib/web/wsgi.py`, CLI `iib` (e.g. `iib db upgrade`), worker `celery -A iib.workers.tasks worker`.
Stack: Python 3.12+, Flask, SQLAlchemy, Celery, PostgreSQL, RabbitMQ, Konflux, OPM, ORAS, OpenTelemetry, boto3, Kerberos auth.
Flask config: `IIB_DEV` / `IIB_TESTING` boolean env vars set to `True` for Development and Testing. If not set, ProductionConfig will be used and config file is expected at `/etc/iib/settings.py`.

## Test
```bash
tox                      # all tests + linters
tox -e py312             # unit tests only (Python 3.12)
tox -m static            # black, flake8, yamllint, mypy
tox -e py312 -- tests/test_web/test_api_v1.py::test_name  # single test
```
- `tests/test_web/` (API, models) and `tests/test_workers/` (tasks). Uses SQLite in tests.
- Add new unit tests for new changes or bug fixes
- Always ensure all tests and linters are successful before marking the task as Done
- Verify the change works by spinning up the local dev env and submitting a request on it

## Design Choices
- **Each request is atomic and independent** - API receives the request, determines the queue it should be sent to and calls the appropriate handler. Workers are monitoring the queues and manage handler execution.
- **Workers don't build images directly** — each `build_containerized_*.py` handler fetches `index.db` from Quay via ORAS, mutates FBC config, pushes to git (or opens a PR for throw-away requests when overwrite_from_index_token is not provided), then calls `wait_for_pipeline_completion()` to let Konflux build the final image. On failure, the git commit is reverted. If a PR was opened, it is always closed and never merged.
- **Request logs are stored in files locally and S3 if configured** - One file per request stored locally on disk if `iib_request_logs_dir` is defined. Also uploaded to S3 buckets if `iib_aws_s3_bucket_name` is defined. These are defined in worker config.
- **index.db cached via OpenShift ImageStream** at `/var/index_db`. Workers compare digests before pulling; `oras_utils.py` manages the local copy.
- **Workers never touch Postgres directly** — state updates go via `iib/workers/api_utils.py`.
- **Static types** in `iib/web/iib_static_types.py` and `iib/workers/tasks/iib_static_types.py`. New request types need a `RequestTypeMapping` entry + `Request` subclass + Alembic migration.
- **API and Workers always run in unprivileged containers**

Key modules:
- `iib/web/api_v1.py` — all REST endpoints
- `iib/web/models.py` — SQLAlchemy models (Request types, Batch, User, Image)
- `iib/web/config.py` — Flask config
- `iib/workers/tasks/konflux_utils.py` — Konflux pipeline monitoring
- `iib/workers/tasks/oras_utils.py` — index.db OCI artifact push/pull
- `iib/workers/config.py` — Celery config (~90 options)
- `iib/workers/tasks/build_containerized*.py` - Handlers per request type
- `iib/web/migrations/` — Alembic migrations via Flask-Migrate

## Pitfalls
- **Never use tools that require privileged access** - the production deployment does not have access to privileged containers.
- `overwrite_from_index_token` triggers an index.db push to Quay after build; if that push fails, the git commit must also be reverted.
- **Always revert the git commit on Konflux or push failure** — dangling commits break the next build. This includes when overwrite_from_index_token triggers an index.db push to Quay that fails. Mark the request failed immediately after reverting.
- **`index.db` lives in `/var/index_db`, not `/tmp`** — the task-start cleanup wipes `/tmp`; storing it there deletes the cache between requests.
- **`regenerate_bundle` always opens a PR** — source bundle overwrite is unsupported; don't add a direct-push path for this request type.
- **Tasks must be idempotent** — Celery retries; images may have already been pushed.
- **Never edit existing Alembic migrations** — generate a new revision instead.
- **API ↔ Worker task signatures must match** — renaming args silently breaks in-flight requests.
- **Avoid making changes to the old worker architecture** - Always ask before making changes to the following functions - `handle_add_request`, `handle_rm_request`, `handle_merge_request`, `handle_create_empty_index_request`, `handle_fbc_operation_request`, `handle_regenerate_bundle_request`
