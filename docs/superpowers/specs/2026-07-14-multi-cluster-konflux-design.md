# Multi-Cluster Konflux Support via Always-MR with Source Branch Routing

**Date:** 2026-07-14
**Status:** Approved

## Problem

IIB's containerized architecture submits builds to a single Konflux cluster. When the same index image (e.g., `registry.example.com/namespace/index-image:v4.14`) is tied to three Konflux instances (QE, Stage, Prod), all three environments' pipelines trigger on every git event because they share the same repo and branch, and their PaC CEL expressions match identically.

Production requests should be prioritized on their own dedicated Konflux instance, not compete with QE and Stage builds.

## Constraints

- The git repo branch (e.g., `v4.14`) is the single source of truth for that OCP version's configs. Cannot duplicate branches per environment.
- Three separate IIB deployments (QE, Stage, Prod), each with its own dedicated Konflux cluster/tenant.
- QE and Stage may share a Konflux cluster URL but use different tenants (namespaces). Prod uses a separate cluster.
- PaC CEL expressions for push events have no environment-differentiable fields when pushing to the same branch.

## Solution: Always-MR with Source Branch Routing

### Routing Mechanism

Each IIB deployment gets a config parameter `iib_environment_name` (e.g., `"qe"`, `"stage"`, `"prod"`). This value is injected into the MR source branch name:

| Current | New |
|---|---|
| `iib-request-{id}-{branch}` | `iib-{env}-request-{id}-{branch}` |
| `iib-request-456-v4.14` | `iib-qe-request-456-v4.14` |

Each Konflux tenant's pipeline CEL expression is updated to match only its environment prefix:

- QE: `event == "pull_request" && target_branch == "v4.14" && source_branch.startsWith("iib-qe-")`
- Stage: `event == "pull_request" && target_branch == "v4.14" && source_branch.startsWith("iib-stage-")`
- Prod: `event == "pull_request" && target_branch == "v4.14" && source_branch.startsWith("iib-prod-")`

Push-event pipelines are removed entirely. All builds go through MRs.

### Unified MR Flow

Today there are two code paths in `git_commit_and_create_mr_or_push()`:

- **Throw-away** (`overwrite_from_index=False`): create MR -> build -> close MR
- **Overwrite** (`overwrite_from_index=True`): push directly to branch -> build -> revert on failure

The new design unifies both into the MR path:

| Step | Throw-away | Overwrite |
|---|---|---|
| Create feature branch | `iib-{env}-request-{id}-{branch}` | `iib-{env}-request-{id}-{branch}` |
| Commit and push to feature branch | Yes | Yes |
| Create MR targeting target branch | Yes | Yes |
| Konflux builds from MR | Yes | Yes |
| After successful build | Close MR | Merge MR |
| Push index.db artifact | No | Yes (after merge) |
| Replicate image | Yes | Yes |

Exception: `regenerate_bundle` is always throw-away (MR is always closed, never merged) and has no index.db to push. Only change is the env prefix on the source branch.

### Overwrite Flow Ordering

```
1. Create MR -> 2. Konflux build -> 3. Merge MR -> 4. Push index.db -> 5. Replicate image
```

The index.db push happens after the merge, not after the build. This ensures the source of truth branch and the index.db artifact stay in sync.

### Error Handling

| Failure point | Current (direct push) | New (MR-based) |
|---|---|---|
| Build fails | Revert commit on source branch (force push) | Close MR (source of truth untouched) |
| index.db push fails | Revert commit + restore artifact digest | Close MR + restore artifact digest |
| Merge fails (transient) | N/A | Retry with backoff |
| Merge fails (permanent, e.g., conflict) | N/A | Close MR, mark request failed |

The risky `revert_last_commit()` path (which does `git reset --hard HEAD~1` + `git push --force`) is eliminated for the overwrite flow. The source of truth branch is never modified until the merge succeeds, so there's nothing to revert.

If the merge fails permanently (e.g., merge conflict from a concurrent request), the request is marked failed even though the image was built successfully. This is correct because the image was built from a state that's no longer compatible with the branch.

## IIB Code Changes

### 1. Worker config (`iib/workers/config.py`)

Add new config parameter:

```python
iib_environment_name: Optional[str] = None
```

Add validation in `_validate_konflux_config()`: if Konflux config is set, `iib_environment_name` must also be set (non-empty string, alphanumeric + hyphens only).

### 2. Git utils (`iib/workers/tasks/git_utils.py`)

**Modify `create_mr()`**: Accept an `environment_name` parameter. Change feature branch naming from `iib-request-{id}-{branch}` to `iib-{env}-request-{id}-{branch}`.

**Add `merge_mr()`**: New function that calls the GitLab merge API (`PUT /projects/:id/merge_requests/:mr_iid/merge`). Accepts `mr_details` and `repo_url`. Uses `squash: true`. Includes retry logic for transient failures (network errors, HTTP 5xx, HTTP 409 "merge in progress"). Non-retryable failures: HTTP 405 (cannot be merged, e.g., conflict), HTTP 401/403 (auth). Returns the merge commit SHA on success, raises `IIBError` on permanent failure.

**`revert_last_commit()`**: No changes. Stays for backward compatibility but is no longer called from the overwrite flow.

### 3. Containerized utils (`iib/workers/tasks/containerized_utils.py`)

**Modify `git_commit_and_create_mr_or_push()`**: Remove the `if not overwrite_from_index` branch. Always call `create_mr()`. Pass `environment_name` from config. Always return `mr_details` (never `None`).

**Add `merge_mr_after_build()`**: New function that merges the MR and returns the merge commit SHA. Handles failure by closing the MR and raising `IIBError`.

**Modify `cleanup_on_failure()`**: The `elif overwrite_from_index and last_commit_sha` branch (which calls `revert_last_commit`) becomes dead code. Since `mr_details` is always set, the first branch (close MR) always runs. The revert path can be kept for safety but won't be reached.

### 4. Build handlers (`iib/workers/tasks/build_containerized*.py`)

Each handler's post-build section changes from:

```
build succeeds -> replicate image -> push index.db -> close MR (if throw-away)
```

To:

```
build succeeds -> merge MR (if overwrite) -> push index.db (if overwrite) -> replicate image -> close MR (if throw-away)
```

Exception: `build_containerized_regenerate_bundle.py` has no index.db push and is always throw-away. Only the env prefix on the branch name changes.

### 5. Konflux utils (`iib/workers/tasks/konflux_utils.py`)

No changes. `find_pipelinerun()` still searches by commit SHA label. PaC labels MR-triggered PipelineRuns with the commit SHA. `wait_for_pipeline_completion()` is unchanged.

## Konflux/PaC Side Changes

For each index image + OCP version combination, per Konflux tenant:

1. Delete the `on-push` pipeline (e.g., `iib-pub-pending-v4-14-on-push.yaml`)
2. Update the `on-pull-request` pipeline's CEL expression to include `source_branch.startsWith("iib-{env}-")`

These are infra changes in the Konflux tenant configs, not IIB code changes.

## Edge Cases

### MR merge creates a push event

When IIB merges an MR, GitLab sends a push event to the target branch. Since push pipelines are deleted, nothing triggers. If any other system watches for push events on these branches, that needs to be accounted for separately.

### Concurrent requests from different environments

Two IIB deployments (QE and Prod) could submit overlapping requests for the same index image. Both create MRs targeting the same branch. The first to merge succeeds. The second's merge may fail with a merge conflict if their changes overlap. This is handled: merge failure -> close MR -> mark request failed. The submitter retries. This is no worse than today's direct-push model where the second push would also fail.

### cancel-in-progress behavior

PaC's `cancel-in-progress` cancels previous PipelineRuns on the same source branch (e.g., multiple pushes to the same MR). Since each request creates a unique source branch (`iib-qe-request-1-v4.14` vs `iib-qe-request-2-v4.14`), concurrent requests -- even from the same env -- won't cancel each other. `cancel-in-progress` only fires if the same MR branch receives multiple pushes, which doesn't happen in the IIB flow.

### regenerate_bundle

Always throw-away, always close MR, no index.db push, no merge. Only change is the env prefix on the source branch. CEL filtering applies correctly.

## Files Changed Summary

| File | Change type |
|---|---|
| `iib/workers/config.py` | Add `iib_environment_name`, add validation |
| `iib/workers/tasks/git_utils.py` | Modify `create_mr()`, add `merge_mr()` |
| `iib/workers/tasks/containerized_utils.py` | Modify `git_commit_and_create_mr_or_push()`, add `merge_mr_after_build()`, simplify `cleanup_on_failure()` |
| `iib/workers/tasks/build_containerized*.py` | Reorder post-build steps to insert merge before index.db push |
| `iib/workers/tasks/konflux_utils.py` | No changes |
