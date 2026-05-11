---
name: debugging-request-failures
description: Use when debugging a failed IIB request, investigating an error from logs, reproducing a bug locally, or when a user reports a request failure. Triggers on "debug", "request failed", "error in logs", "reproduce the bug", "investigate failure", or when logs are provided.
---

## Overview

IIB request failures can originate in the API layer (validation, broker errors) or the worker layer (build failures, git/Konflux issues, ORAS artifact errors). This skill covers the **containerized** request types — the current architecture where workers push to git and delegate image builds to Konflux pipelines.

## Step 1: Identify the Endpoint and Error

**If logs are provided:** Extract the request type and error from the log. Key patterns:

| Log Pattern | Meaning | Layer |
|---|---|---|
| `ValidationError` | Invalid input parameters | API (`iib/web/api_v1.py`) |
| `IIBError` | General build/processing failure | API / Worker |
| `ExternalServiceError` | Registry/external service HTTP error | Worker (retried automatically) |
| `kombu.exceptions.OperationalError` | RabbitMQ broker unreachable | API (`iib/web/errors.py`) |
| `FinalStateOverwriteError` | Attempt to update a completed/failed request | Worker (`iib/workers/api_utils.py`) |
| `cleanup_on_failure` | Worker reverting git/MR/artifact after error | Worker (`containerized_utils.py`) |
| `handle_containerized_*` in traceback | Identifies the specific handler | Worker |

The request type maps to a containerized handler:

| Request Type | Endpoint | Handler File |
|---|---|---|
| `add` | `POST /builds/add` | `build_containerized_add.py` → `handle_containerized_add_request` |
| `rm` | `POST /builds/rm` | `build_containerized_rm.py` → `handle_containerized_rm_request` |
| `regenerate-bundle` | `POST /builds/regenerate-bundle` | `build_containerized_regenerate_bundle.py` → `handle_containerized_regenerate_bundle_request` |
| `merge-index-image` | `POST /builds/merge-index-image` | `build_containerized_merge.py` → `handle_containerized_merge_request` |
| `create-empty-index` | `POST /builds/create-empty-index` | `build_containerized_create_empty_index.py` → `handle_containerized_create_empty_index_request` |
| `fbc-operations` | `POST /builds/fbc-operations` | `build_containerized_fbc_operations.py` → `handle_containerized_fbc_operation_request` |

Shared utilities live in `iib/workers/tasks/containerized_utils.py` — most failures originate from functions there.

**If logs are NOT provided:** Ask the user which endpoint/request type they're investigating, then proceed to Step 2.

## Step 2: Gather Parameters

Ask the user for the **required** parameters for the identified endpoint. Then ask if they want to provide any **optional** parameters.

Read [references/endpoint-parameters.md](references/endpoint-parameters.md) for the full required/optional parameter table for each endpoint.

**Quick reference — required parameters only:**

| Endpoint | Required Parameters |
|---|---|
| `/builds/add` | `bundles`, `from_index` |
| `/builds/rm` | `from_index`, `operators` |
| `/builds/regenerate-bundle` | `from_bundle_image` |
| `/builds/merge-index-image` | `source_from_index` |
| `/builds/create-empty-index` | `from_index` |
| `/builds/fbc-operations` | `from_index`, plus `fbc_fragment` or `fbc_fragments` |

## Step 3: Trace the Error

**For API-layer errors** (ValidationError, 400/403/404):
1. Read the endpoint function in `iib/web/api_v1.py`
2. Read the `from_json()` method of the relevant Request subclass in `iib/web/models.py`
3. Trace the validation logic to find where the error is raised
4. Check `iib/web/errors.py` for error response formatting

**For worker-layer errors** (IIBError, task failures):

The containerized build lifecycle has distinct failure points. Trace in this order:

1. **Handler** — `iib/workers/tasks/build_containerized_<type>.py` — the top-level orchestration
2. **Git operations** — `containerized_utils.py:prepare_git_repository_for_build()`, `git_commit_and_create_mr_or_push()`
3. **ORAS artifacts** — `containerized_utils.py:fetch_and_verify_index_db_artifact()`, `push_index_db_artifact()`
4. **Konflux pipeline** — `containerized_utils.py:monitor_pipeline_and_extract_image()`
5. **Image replication** — `containerized_utils.py:replicate_image_to_tagged_destinations()`
6. **Cleanup/rollback** — `containerized_utils.py:cleanup_on_failure()` — closes MR, reverts git commit, restores index.db digest
7. **Failed callback** — `iib/workers/tasks/general.py:failed_request_callback` — catches unhandled exceptions

## Step 4: Reproduce Locally

**Before starting**, check if `.env.containerized` exists in the repo root. If it does not:

1. Ask the user if they have Konflux and GitLab credentials available.
2. If **yes** — walk them through filling in `.env.containerized` from the template:
   ```bash
   cp .env.containerized.template .env.containerized
   ```
   The key variables they need to provide:
   - `IIB_KONFLUX_CLUSTER_URL` — Konflux cluster API URL
   - `IIB_KONFLUX_SA_TOKEN` — service account token for the Konflux namespace
   - `IIB_KONFLUX_CA_CERT_PATH` — path to Konflux CA cert (also copy the cert to `docker/containerized/konflux-ca.crt`)
   - `IIB_GITLAB_TOKEN` — GitLab token with `read_repository`/`write_repository` permissions
   - `IIB_GITLAB_URL` — GitLab instance URL
   - Registry and namespace variables — see `.env.containerized.template` for the full list

   See `docker/containerized/README.md` for detailed setup instructions.
3. If **no** — they cannot reproduce locally with the containerized workflow. Skip to Step 5 and work from the error trace and tests alone.

If `.env.containerized` already exists, proceed directly:

```bash
# Start all services
podman-compose -f podman-compose-containerized.yml up -d
```

Verify services are running:

```bash
podman-compose -f podman-compose-containerized.yml ps
# Should see: iib-api, iib-worker-containerized, db, rabbitmq, registry, memcached, message-broker
```

Submit the request:

```bash
curl -X POST http://localhost:8080/api/v1/builds/<endpoint> \
  -H 'Content-Type: application/json' \
  -d '<JSON payload with user-provided parameters>'
```

Check the response:
- **201**: Request accepted — check worker logs for the failure
- **400**: Validation error — the error message tells you what's wrong
- **500**: Server error — check API logs

Monitor worker processing:

```bash
podman-compose -f podman-compose-containerized.yml logs -f iib-worker-containerized
```

Check request status:

```bash
curl http://localhost:8080/api/v1/builds/<request_id>
```

If the request reaches `failed` state, the `state_reason` field contains the error message.

## Step 5: Fix and Verify

1. Apply the fix to the relevant source file
2. The dev environment uses watchmedo for auto-reload — the worker will restart automatically after file changes
3. Re-submit the same request
4. Confirm the request reaches `complete` state (not `failed`)
5. Run the targeted test: `tox -e py312 -- tests/<relevant_test_file>::<test_name>`
6. **REQUIRED:** Read the `validating-changes` skill before marking done — run the full `tox` suite

## Step 6: Clean Up

```bash
podman-compose -f podman-compose-containerized.yml down
```

## Gotchas

- **Worker container is `iib-worker-containerized`** — not `iib-worker`. Use `podman-compose -f podman-compose-containerized.yml logs -f iib-worker-containerized` to view logs.
- **Worker logs include request ID** — grep for it to find relevant entries.
- **Request logs endpoint** (`GET /builds/<id>/logs`) only works after request reaches a final state.
- **RabbitMQ management UI at `http://localhost:8081`** (credentials: `iib`/`iib`) — useful for checking if tasks are queued.
- **Database** is PostgreSQL in dev (`iib:iib@db:5432/iib`) — use `psql` inside the container to inspect request state directly if the API isn't responding.
- **Retry logic**: Workers retry on `ExternalServiceError` with exponential backoff (`iib_total_attempts`, default 5).
- **`FinalStateOverwriteError`** is not a bug — it means a retry tried to update an already-finalized request. The callback logs it and exits gracefully.
- **`cleanup_on_failure` handles three recovery paths**: close MR (if throw-away request), revert git commit (if direct push), and restore original index.db digest via ORAS copy.
- **`regenerate_bundle` always creates an MR** — never a direct push. On failure, the MR is closed, never merged.
- **Konflux connectivity issues** are a common failure cause — check worker logs for "Kubernetes client" errors. See `docker/containerized/README.md` Troubleshooting section.
- **GitLab auth errors** — verify token in `.env.containerized` hasn't expired and has `read_repository`/`write_repository` permissions.
