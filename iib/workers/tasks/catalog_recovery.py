# SPDX-License-Identifier: GPL-3.0-or-later
"""Verify and restore the Git catalog selected by a published index digest.

The public image tag and its digest-keyed database binding identify the catalog
that a new overwrite must use. A retained Git ref makes that revision available
even after GitLab squashes the build MR and deletes its source branch.
"""

import json
import re
import uuid
from pathlib import Path
from typing import Optional

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.git_utils import create_mr, merge_mr
from iib.workers.tasks.utils import get_resolved_image, run_cmd, set_registry_token


def _git(repo: str, *args: str) -> str:
    """Run Git in a catalog checkout and return its output."""
    return run_cmd(
        ['git', '-C', repo, *args], exc_msg=f'Git catalog operation failed: {args[0]}'
    ).strip()


def catalog_tree(repo: str, revision: str = 'HEAD') -> str:
    """Return the Git tree ID for every tracked path under ``configs/``."""
    tree = _git(repo, 'rev-parse', f'{revision}:configs')
    if not re.fullmatch(r'[0-9a-f]{40,64}', tree):
        raise IIBError(f'Invalid catalog tree for {revision}')
    return tree


def catalog_producer(repo: str, revision: str = 'HEAD') -> str:
    """Read the producer request ID from a revision's Git build metadata."""
    try:
        metadata = json.loads(_git(repo, 'show', f'{revision}:.iib-build-metadata.json'))
    except json.JSONDecodeError as exc:
        raise IIBError(f'Invalid catalog producer metadata in {revision}') from exc
    if not isinstance(metadata, dict):
        raise IIBError(f'Invalid catalog producer metadata in {revision}')
    producer = metadata.get('request_id')
    if not isinstance(producer, int) or producer < 0:
        raise IIBError(f'Missing catalog producer in {revision}')
    return str(producer)


def retained_ref(resolved_image: str) -> str:
    """Name the Git ref retaining the commit built for a pinned image digest."""
    match = re.search(r'@sha256:([0-9a-f]{64})$', resolved_image)
    if not match:
        raise IIBError('Expected a digest-pinned index image')
    return f'refs/tags/iib-index-{match.group(1)}'


def retain_catalog_revision(
    repo: str, built_image: str, revision: str, expected_tree: str, producer: str
) -> None:
    """Bind a built image digest to its fetchable, immutable Git revision.

    Reusing the same ref is safe on retry only if it still points to this
    exact commit. The local tree and producer are checked before publishing it.
    """
    if _git(repo, 'rev-parse', 'HEAD') != revision:
        raise IIBError('Built catalog revision differs from local HEAD')
    if catalog_tree(repo) != expected_tree or catalog_producer(repo) != producer:
        raise IIBError('Built catalog contents differ from their binding')
    ref = retained_ref(built_image)
    remote = _git(repo, 'ls-remote', 'origin', ref)
    if remote:
        if remote.split()[0] != revision:
            raise IIBError(f'Conflicting retained catalog ref {ref}')
        return
    _git(repo, 'push', 'origin', f'{revision}:{ref}')
    confirmed_ref = _git(repo, 'ls-remote', 'origin', ref)
    if not confirmed_ref or confirmed_ref.split()[0] != revision:
        raise IIBError(f'Retained catalog ref {ref} could not be verified')


def _branch_matches_binding(repo: str, tree: str, producer: str) -> bool:
    """Compare the branch tip with the catalog and producer selected by the tag."""
    try:
        return catalog_tree(repo) == tree and catalog_producer(repo) == producer
    except IIBError:
        return False


def verify_or_repair_catalog(
    *,
    repo: str,
    repo_url: str,
    branch: str,
    image: str,
    published: str,
    token: Optional[str],
    revision: str,
    tree: str,
    producer: str,
    request_id: int,
) -> None:
    """Verify a published Git binding and repair an ahead target branch.

    ``published`` is the resolved public image, while ``image`` is its mutable
    tag. The caller has already read the bound database from a pinned ORAS
    manifest. The retained commit is fetched and checked even when the branch
    appears to match: its tree and producer must agree with those annotations.

    If the target branch differs, restore the tracked catalog and build metadata
    through a separate GitLab MR. That repair does not move the public tag and
    remains in history if the caller's subsequent build fails. A retry after a
    completed repair sees a matching branch.

    :raises IIBError: If the ref or metadata conflicts, the public tag changes,
        or the GitLab repair cannot be verified.
    """
    ref = retained_ref(published)
    remote = _git(repo, 'ls-remote', 'origin', ref)
    if not remote or remote.split()[0] != revision:
        raise IIBError(f'Missing or conflicting retained catalog ref {ref}')
    # The branch checkout has depth one. Fetching the retained ref is the only
    # reliable way to inspect an older commit after MR branch cleanup.
    _git(repo, 'fetch', '--depth', '1', 'origin', ref)
    if _git(repo, 'rev-parse', 'FETCH_HEAD') != revision:
        raise IIBError('Fetched catalog revision differs from published binding')
    if catalog_tree(repo, revision) != tree or catalog_producer(repo, revision) != producer:
        raise IIBError('Published catalog revision differs from artifact annotations')
    if _branch_matches_binding(repo, tree, producer):
        return

    with set_registry_token(token, image, append=True):
        if get_resolved_image(image) != published:
            raise IIBError('Public index digest changed before catalog repair')

    # Remove the entire tracked tree first, so files absent from the published
    # revision are deleted as well. The checkout leaves unrelated files alone.
    _git(repo, 'rm', '-r', '--ignore-unmatch', '--', 'configs')
    _git(repo, 'checkout', revision, '--', 'configs')
    # The producer lives outside configs/. Restore its published value while
    # leaving every other path at the current branch tip.
    metadata = _git(repo, 'show', f'{revision}:.iib-build-metadata.json')
    (Path(repo) / '.iib-build-metadata.json').write_text(metadata + '\n')
    _git(repo, 'add', '--', 'configs', '.iib-build-metadata.json')

    # create_mr commits the staged repair on a fresh source branch. GitLab's
    # normal merge policy decides whether it may land on the target branch.
    # A previous attempt may have crashed with a repair MR still open. Its
    # source branch must not block this attempt's push or its normal MR flow.
    mr = create_mr(
        request_id=request_id,
        local_repo_path=repo,
        repo_url=repo_url,
        branch=branch,
        commit_message=f'IIB: Restore published catalog for request {request_id}',
        environment_name=get_worker_config().get('iib_environment_name'),
        source_branch_suffix=f'-repair-{uuid.uuid4().hex[:12]}',
    )
    merge_mr(mr, repo_url)
    # Verify GitLab's target branch rather than the local repair source branch;
    # the merge may have squashed the commit while preserving its catalog tree.
    _git(repo, 'fetch', '--depth', '1', 'origin', branch)
    _git(repo, 'checkout', '-B', branch, 'FETCH_HEAD')
    if not _branch_matches_binding(repo, tree, producer):
        raise IIBError('Catalog repair did not produce the published tree and producer')
    with set_registry_token(token, image, append=True):
        if get_resolved_image(image) != published:
            raise IIBError('Public index digest changed during catalog repair')
