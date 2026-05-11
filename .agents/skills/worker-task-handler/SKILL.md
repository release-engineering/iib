---
name: worker-task-patterns
description: Use when modifying or creating Celery worker task handlers in IIB, especially build_containerized or build_*.py files. Triggers on "worker handler", "celery task", "build handler", "Konflux pipeline", "ORAS", or "git revert on failure".
---

## Overview

IIB worker handlers in `iib/workers/tasks/build_containerized*.py` follow a specific lifecycle: pull index.db via ORAS, mutate FBC configuration, push changes to git, wait for Konflux to build the image, and handle failures by reverting git commits.

## Handler Structure

```python
from iib.common.tracing import instrument_tracing

@instrument_tracing(span_name="workers.tasks.handle_your_type_request")
def handle_your_type_request(
    from_index: str,
    request_id: int,
    binary_image: Optional[str] = None,
    # ... args must match api_v1.py dispatch exactly
) -> None:
    # 1. Mark in-progress
    set_request_state(request_id, 'in_progress', 'Starting request')

    # 2. Prebuild: resolve images, determine arches
    prebuild_info = prepare_request_for_build(request_id, ...)

    # 3. Pull index.db via ORAS
    # Uses /var/index_db cache — NOT /tmp (task cleanup wipes /tmp)

    # 4. Mutate FBC configuration

    # 5. Push to git (commit or PR)
    #    - Direct commit if overwrite_from_index_token provided
    #    - PR for throw-away requests (always closed, never merged)

    # 6. Wait for Konflux pipeline
    wait_for_pipeline_completion(request_id, pipeline_run_name)

    # 7. Handle overwrite_from_index_token push (if applicable)

    # 8. Mark complete
    set_request_state(request_id, 'complete', 'Request completed successfully')
```

## Critical: Git Revert on Failure

**Every failure path must revert the git commit before marking the request failed.** Dangling commits break subsequent builds.

```python
try:
    # ... build logic, git push, wait for pipeline ...
except Exception:
    # MUST revert git commit first
    revert_git_commit(commit_sha, repo_url, branch)
    set_request_state(request_id, 'failed', str(error))
    raise
```

This includes failures during:
- Konflux pipeline execution
- `overwrite_from_index_token` push to Quay
- Any post-commit operation

## State Updates via API

Workers never touch PostgreSQL directly. All state updates go through `iib/workers/api_utils.py`:

```python
from iib.workers.api_utils import set_request_state, update_request

set_request_state(request_id, 'in_progress', 'Resolving images')
update_request(request_id, {'from_index_resolved': resolved_pullspec})
```

## Registering a New Handler

Add to `iib/workers/config.py` `include` list:

```python
include = [
    'iib.workers.tasks.build',
    # ...existing entries...
    'iib.workers.tasks.build_your_type',  # add here
]
```

## Gotchas

- **`index.db` cache is at `/var/index_db`** — task-start cleanup wipes `/tmp`, so never store it there.
- **Tasks must be idempotent** — Celery may retry; images may already be pushed.
- **`regenerate_bundle` always opens a PR** — never add a direct-push path for this type.
- **Do not modify old handler functions** (`handle_add_request`, `handle_rm_request`, `handle_merge_request`, `handle_create_empty_index_request`, `handle_fbc_operation_request`, `handle_regenerate_bundle_request`) without asking first — these are legacy architecture.
- **Never use privileged container operations** — production runs unprivileged.
- **Handler function signature must match the args list in `api_v1.py`** — positional, same order. Renaming args silently breaks in-flight requests.
