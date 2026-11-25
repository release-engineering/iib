# SPDX-License-Identifier: GPL-3.0-or-later
"""Recovery of the catalog selected by a published index digest."""

import json
import subprocess
from contextlib import nullcontext
from pathlib import Path
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import catalog_recovery as recovery


def git(repo: Path, *args: str) -> str:
    """Run Git in a test repository."""
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


@pytest.fixture
def published_repo(tmp_path):
    """Create an ahead branch with a durable ref to its published predecessor."""
    bare = tmp_path / 'remote.git'
    bare.mkdir()
    git(bare, 'init', '--bare')
    seed = tmp_path / 'seed'
    subprocess.check_call(['git', 'clone', str(bare), str(seed)])
    git(seed, 'config', 'user.name', 'IIB test')
    git(seed, 'config', 'user.email', 'iib@example.test')
    git(seed, 'config', 'commit.gpgsign', 'false')
    git(seed, 'checkout', '-b', 'v4.17')
    configs = seed / 'configs'
    configs.mkdir()
    (configs / 'a.json').write_text('published a')
    (configs / 'deleted-later.json').write_text('published only')
    (seed / '.iib-build-metadata.json').write_text(json.dumps({'request_id': 10}))
    (seed / 'unrelated.txt').write_text('first')
    git(seed, 'add', '.')
    git(seed, 'commit', '-m', 'published')
    revision = git(seed, 'rev-parse', 'HEAD')
    tree = recovery.catalog_tree(str(seed))
    image = 'quay.io/ns/index:v4.17'
    published = 'quay.io/ns/index@sha256:' + 'd' * 64
    ref = recovery.retained_ref(published)
    git(seed, 'push', 'origin', 'v4.17', f'{revision}:{ref}')

    (configs / 'a.json').write_text('unpublished a')
    (configs / 'deleted-later.json').unlink()
    (configs / 'new.json').write_text('unpublished only')
    (seed / '.iib-build-metadata.json').write_text(json.dumps({'request_id': 11}))
    (seed / 'unrelated.txt').write_text('preserve me')
    git(seed, 'add', '-A')
    git(seed, 'commit', '-m', 'unpublished')
    git(seed, 'push', 'origin', 'v4.17')

    worker = tmp_path / 'worker'
    subprocess.check_call(
        ['git', 'clone', '--depth', '1', '--branch', 'v4.17', f'file://{bare}', str(worker)]
    )
    git(worker, 'config', 'user.name', 'IIB test')
    git(worker, 'config', 'user.email', 'iib@example.test')
    git(worker, 'config', 'commit.gpgsign', 'false')
    return worker, image, published, revision, tree


def repair_args(published_repo):
    """Build keyword arguments for a published catalog repair."""
    worker, image, published, revision, tree = published_repo
    return {
        'repo': str(worker),
        'repo_url': 'https://gitlab.example.test/catalog.git',
        'branch': 'v4.17',
        'image': image,
        'published': published,
        'token': 'user:token',
        'revision': revision,
        'tree': tree,
        'producer': '10',
        'request_id': 20,
    }


@pytest.mark.parametrize('delete_all', (False, True))
def test_repair_restores_exact_tree_and_preserves_unrelated_files(published_repo, delete_all):
    """A separate repair commit survives later B failures and is retry safe."""
    args = repair_args(published_repo)
    worker = Path(args['repo'])
    if delete_all:
        git(worker, 'rm', '-r', 'configs')
        git(worker, 'commit', '-m', 'remove complete catalog')
        git(worker, 'push', 'origin', 'v4.17')
    previous_tip = git(worker, 'rev-parse', 'HEAD')

    def create_mr(**kwargs):
        git(worker, 'checkout', '-b', 'repair')
        git(worker, 'commit', '-m', kwargs['commit_message'])
        git(worker, 'push', 'origin', 'repair')
        return {'mr_id': '1'}

    def merge_mr(_mr, _url):
        git(worker, 'checkout', 'v4.17')
        git(worker, 'merge', '--ff-only', 'repair')
        git(worker, 'push', 'origin', 'v4.17')
        return git(worker, 'rev-parse', 'HEAD')

    with (
        mock.patch.object(recovery, 'create_mr', side_effect=create_mr) as mr,
        mock.patch.object(recovery, 'merge_mr', side_effect=merge_mr),
        mock.patch.object(recovery, 'get_worker_config', return_value={}),
        mock.patch.object(recovery, 'set_registry_token', return_value=nullcontext()),
        mock.patch.object(recovery, 'get_resolved_image', return_value=args['published']),
    ):
        recovery.verify_or_repair_catalog(**args)
        assert recovery.catalog_tree(str(worker)) == args['tree']
        assert recovery.catalog_producer(str(worker)) == '10'
        assert (worker / 'configs' / 'deleted-later.json').read_text() == 'published only'
        assert not (worker / 'configs' / 'new.json').exists()
        assert (worker / 'unrelated.txt').read_text() == 'preserve me'
        assert f'parent {previous_tip}' in git(worker, 'cat-file', '-p', 'HEAD')
        recovery.verify_or_repair_catalog(**args)
        mr.assert_called_once()


def test_repair_rejects_missing_ref_before_editing_git(published_repo):
    """A lost MR branch without a retained revision must stop B."""
    args = repair_args(published_repo)
    worker = Path(args['repo'])
    old_head = git(worker, 'rev-parse', 'HEAD')
    git(worker, 'push', 'origin', '--delete', recovery.retained_ref(args['published']))
    with pytest.raises(IIBError, match='Missing or conflicting retained catalog ref'):
        recovery.verify_or_repair_catalog(**args)
    assert git(worker, 'rev-parse', 'HEAD') == old_head


def test_retain_catalog_revision_reuses_only_matching_ref(published_repo):
    """A digest ref is immutable across retries and conflicting builds."""
    worker, _image, published, _revision, _tree = published_repo
    built_revision = git(worker, 'rev-parse', 'HEAD')
    built_tree = recovery.catalog_tree(str(worker))
    candidate = 'quay.io/ns/index@sha256:' + 'e' * 64
    recovery.retain_catalog_revision(str(worker), candidate, built_revision, built_tree, '11')
    recovery.retain_catalog_revision(str(worker), candidate, built_revision, built_tree, '11')
    with pytest.raises(IIBError, match='Conflicting retained catalog ref'):
        recovery.retain_catalog_revision(str(worker), published, built_revision, built_tree, '11')


def test_repair_requires_ref_even_for_matching_catalog(published_repo):
    """A matching branch cannot stand in for a durable published revision."""
    args = repair_args(published_repo)
    worker = Path(args['repo'])
    args['revision'] = git(worker, 'rev-parse', 'HEAD')
    args['tree'] = recovery.catalog_tree(str(worker))
    args['producer'] = '11'
    git(worker, 'push', 'origin', '--delete', recovery.retained_ref(args['published']))
    with pytest.raises(IIBError, match='Missing or conflicting retained catalog ref'):
        recovery.verify_or_repair_catalog(**args)


def test_repair_retry_uses_new_source_branch_after_premerge_crash(published_repo, tmp_path):
    """A source MR left by a crash cannot block the next repair attempt."""
    args = repair_args(published_repo)
    suffixes = []

    def interrupted_mr(**kwargs):
        suffixes.append(kwargs['source_branch_suffix'])
        raise IIBError('interrupted before merge')

    with (
        mock.patch.object(recovery, 'create_mr', side_effect=interrupted_mr),
        mock.patch.object(recovery, 'get_worker_config', return_value={}),
        mock.patch.object(recovery, 'set_registry_token', return_value=nullcontext()),
        mock.patch.object(recovery, 'get_resolved_image', return_value=args['published']),
    ):
        with pytest.raises(IIBError, match='interrupted before merge'):
            recovery.verify_or_repair_catalog(**args)

        retry = tmp_path / 'retry'
        origin = git(Path(args['repo']), 'remote', 'get-url', 'origin')
        subprocess.check_call(
            ['git', 'clone', '--depth', '1', '--branch', 'v4.17', origin, str(retry)]
        )
        args['repo'] = str(retry)
        with pytest.raises(IIBError, match='interrupted before merge'):
            recovery.verify_or_repair_catalog(**args)

    assert len(suffixes) == 2
    assert suffixes[0] != suffixes[1]
