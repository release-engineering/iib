---
name: validating-changes
description: Use before marking work as done, before creating a PR, or before committing IIB changes. Triggers on "ready to submit", "validate", "run checks", "pre-PR checklist", or "before committing".
---

## Overview

IIB requires all tests and linters to pass before changes are considered complete. The `tox` command runs everything, but individual checks can be run for faster iteration.

## Full Validation (Required Before PR)

```bash
tox
```

This runs: `black`, `flake8`, `yamllint`, `mypy`, `bandit`, `safety`, `py312` (pytest), `docs`.

## Individual Checks (For Iteration)

```bash
tox -e black           # code formatting check
tox -e black-format    # auto-fix formatting
tox -e flake8          # PEP8 + docstring checks (max-line-length=100)
tox -e mypy            # type checking
tox -e yamllint        # YAML lint
tox -e bandit          # security static analysis
tox -e py312           # unit tests with coverage
tox -m static          # all static checks (black, flake8, yamllint, mypy)
```

## Pre-PR Checklist

1. **All tox environments pass**: `tox` exits 0
2. **New code has tests**: every new function/endpoint/handler has corresponding test coverage
3. **Migration generated** (if models changed): `tox -e migrate-db "description"`
4. **API args match handler signature**: if you changed a celery task's parameters, verify the args list in `api_v1.py` matches the handler function signature exactly (positional, same order)
5. **Static types updated**: changes to payloads or responses need matching TypedDict updates in `iib/web/iib_static_types.py`
6. **No existing migrations edited**: if a migration needs fixing, create a new one
7. **Local dev verification**: `podman-compose -f podman-compose-containerized.yml up -d`, submit a request, verify it completes (see `docker/containerized/README.md` for setup)

## Common Failures and Fixes

| Check | Common Issue | Fix |
|---|---|---|
| `black` | Line too long or formatting | `tox -e black-format` |
| `flake8` | Missing docstring (D103 exempt in tests) | Add docstring to public function |
| `flake8` | Line > 100 chars | Break line |
| `mypy` | Missing type annotation | Add type hints |
| `bandit` | Security warning on subprocess | Review and suppress if false positive |
| `py312` | Test failure | Fix the test or the code |

## Local Dev Verification (Containerized Workflow)

**First**, ask the user if they want to do local dev verification. If they decline, skip this section — tox and tests are sufficient for code correctness.

If they want to proceed, check if `.env.containerized` exists in the repo root. If it does not:

1. Ask the user if they have Konflux and GitLab credentials available.
2. If **yes** — walk them through setup:
   ```bash
   cp .env.containerized.template .env.containerized
   ```
   Key variables to fill in:
   - `IIB_KONFLUX_CLUSTER_URL` — Konflux cluster API URL
   - `IIB_KONFLUX_SA_TOKEN` — service account token for the Konflux namespace
   - `IIB_KONFLUX_CA_CERT_PATH` — path to Konflux CA cert (also copy cert to `docker/containerized/konflux-ca.crt`)
   - `IIB_GITLAB_TOKEN` — GitLab token with `read_repository`/`write_repository` permissions
   - `IIB_GITLAB_URL` — GitLab instance URL
   - Registry and namespace variables — see `.env.containerized.template` for the full list

   See `docker/containerized/README.md` for detailed setup instructions.
3. If **no** — skip local dev verification. Rely on tox and tests alone.

### Start the environment

```bash
podman-compose -f podman-compose-containerized.yml up -d

# Verify services are running (expect: iib-api, iib-worker-containerized, db, rabbitmq, registry, memcached, message-broker)
podman-compose -f podman-compose-containerized.yml ps
```

### Gather request parameters

Identify the endpoint affected by your changes. Ask the user for the **required** parameters for that endpoint, then ask if they want to provide any **optional** parameters.

**REQUIRED:** Read the `debugging-request-failures` skill's [references/endpoint-parameters.md](../debugging-request-failures/references/endpoint-parameters.md) for the full required/optional parameter table for each endpoint.

Quick reference — required parameters only:

| Endpoint | Required Parameters |
|---|---|
| `/builds/add` | `bundles`, `from_index` |
| `/builds/rm` | `from_index`, `operators` |
| `/builds/regenerate-bundle` | `from_bundle_image` |
| `/builds/merge-index-image` | `source_from_index` |
| `/builds/create-empty-index` | `from_index` |
| `/builds/fbc-operations` | `from_index`, plus `fbc_fragment` or `fbc_fragments` |

### Submit and verify

```bash
# Submit the request with user-provided parameters
curl -X POST http://localhost:8080/api/v1/builds/<endpoint> \
  -H 'Content-Type: application/json' \
  -d '<JSON payload with user-provided parameters>'

# Check request status
curl http://localhost:8080/api/v1/builds/<request_id>

# Monitor worker logs
podman-compose -f podman-compose-containerized.yml logs -f iib-worker-containerized
```

The request should reach `complete` state. If it reaches `failed`, check `state_reason` in the response and worker logs for details.

### Cleanup

```bash
podman-compose -f podman-compose-containerized.yml down
```
