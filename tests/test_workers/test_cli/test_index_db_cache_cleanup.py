# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests for asynchronous index.db ImageStream cache retention."""

from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.cli import index_db_cache_cleanup as cleanup_cli
from iib.workers.cli.index_db_cache_cleanup import (
    cleanup_index_db_cache,
    sorted_cache_tags,
    tags_to_delete,
)


def _image_stream(*created):
    return {
        'status': {
            'tags': [
                {
                    'tag': f'idb-{index}',
                    'items': [{'created': timestamp, 'image': f'sha256:{index}'}],
                }
                for index, timestamp in enumerate(created)
            ]
        }
    }


def test_sorted_cache_tags_uses_creation_time_and_name_as_tiebreaker():
    image_stream = _image_stream(
        '2026-01-03T00:00:00Z',
        '2026-01-01T00:00:00Z',
        '2026-01-02T00:00:00Z',
    )

    assert [name for name, _ in sorted_cache_tags(image_stream)] == ['idb-1', 'idb-2', 'idb-0']


def test_tags_to_delete_deletes_oldest_batch_to_target():
    image_stream = _image_stream(*[f'2026-01-{day:02d}T00:00:00Z' for day in range(1, 6)])

    assert tags_to_delete(image_stream, threshold=4, target=2) == ['idb-0', 'idb-1', 'idb-2']


def test_cleanup_does_nothing_below_threshold():
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup._get_image_stream',
        return_value=_image_stream('2026-01-01T00:00:00Z'),
    ), mock.patch('iib.workers.cli.index_db_cache_cleanup.run_cmd') as run_cmd:
        assert cleanup_index_db_cache(threshold=2, target=1) == []
        run_cmd.assert_not_called()


def test_cleanup_deletes_selected_tags_and_is_idempotent():
    image_stream = _image_stream(*[f'2026-01-{day:02d}T00:00:00Z' for day in range(1, 5)])
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup._get_image_stream', return_value=image_stream
    ), mock.patch('iib.workers.cli.index_db_cache_cleanup.run_cmd') as run_cmd:
        assert cleanup_index_db_cache(threshold=3, target=2) == ['idb-0', 'idb-1']
        assert run_cmd.call_count == 2
        run_cmd.assert_has_calls(
            [
                mock.call(
                    [
                        'oc',
                        'delete',
                        'imagestreamtag',
                        'index-db-cache:idb-0',
                        '--ignore-not-found',
                    ],
                    exc_msg='Failed to delete ImageStream tag idb-0',
                ),
                mock.call(
                    [
                        'oc',
                        'delete',
                        'imagestreamtag',
                        'index-db-cache:idb-1',
                        '--ignore-not-found',
                    ],
                    exc_msg='Failed to delete ImageStream tag idb-1',
                ),
            ]
        )


def test_missing_imagestream_is_safe():
    with mock.patch('iib.workers.cli.index_db_cache_cleanup._get_image_stream', return_value=None):
        assert cleanup_index_db_cache() == []


@pytest.mark.parametrize(
    'image_stream',
    [
        {},
        {'status': {'tags': [{'tag': 'idb-bad'}]}},
        {'status': {'tags': [{'tag': 'idb-bad', 'items': [{'created': 'bad'}]}]}},
    ],
)
def test_malformed_imagestream_is_rejected(image_stream):
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup._get_image_stream', return_value=image_stream
    ), pytest.raises(IIBError, match='Invalid ImageStream'):
        cleanup_index_db_cache()


def test_tag_with_only_non_dict_items_is_rejected():
    image_stream = {'status': {'tags': [{'tag': 'idb-bad', 'items': [None, 'invalid']}]}}
    with pytest.raises(ValueError, match='no valid image items'):
        sorted_cache_tags(image_stream)


def test_read_command_failure_is_propagated():
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup.run_cmd',
        side_effect=IIBError('oc failed'),
    ), pytest.raises(IIBError, match='oc failed'):
        cleanup_index_db_cache()


def test_delete_command_failure_is_propagated():
    image_stream = _image_stream(*[f'2026-01-{day:02d}T00:00:00Z' for day in range(1, 4)])
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup._get_image_stream', return_value=image_stream
    ), mock.patch(
        'iib.workers.cli.index_db_cache_cleanup.run_cmd', side_effect=IIBError('delete failed')
    ), pytest.raises(
        IIBError, match='delete failed'
    ):
        cleanup_index_db_cache(threshold=2, target=1)


def test_configured_value_reads_integer_worker_config():
    with mock.patch.object(cleanup_cli, 'get_worker_config', return_value={'cache_limit': '42'}):
        assert cleanup_cli._configured_value('cache_limit', 10) == 42


def test_configured_value_uses_default_when_missing():
    with mock.patch.object(cleanup_cli, 'get_worker_config', return_value={}):
        assert cleanup_cli._configured_value('cache_limit', 10) == 10


def test_configured_value_rejects_non_integer():
    with mock.patch.object(cleanup_cli, 'get_worker_config', return_value={'cache_limit': 'bad'}):
        with pytest.raises(IIBError, match='cache_limit must be an integer'):
            cleanup_cli._configured_value('cache_limit', 10)


def test_main_passes_cli_arguments_to_cleanup():
    with mock.patch.object(cleanup_cli, 'cleanup_index_db_cache') as cleanup:
        assert cleanup_cli.main(['--threshold', '12', '--target', '7']) == 0
    cleanup.assert_called_once_with(threshold=12, target=7)


def test_main_falls_back_to_worker_config():
    with mock.patch.object(cleanup_cli, '_configured_value', side_effect=[12, 7]) as configured:
        with mock.patch.object(cleanup_cli, 'cleanup_index_db_cache') as cleanup:
            assert cleanup_cli.main([]) == 0
    assert configured.call_args_list == [
        mock.call('iib_index_db_cache_cleanup_threshold', cleanup_cli.DEFAULT_THRESHOLD),
        mock.call('iib_index_db_cache_cleanup_target', cleanup_cli.DEFAULT_TARGET),
    ]
    cleanup.assert_called_once_with(threshold=12, target=7)


def test_main_returns_one_when_cleanup_fails():
    error = IIBError('cleanup failed')
    with mock.patch.object(
        cleanup_cli, 'cleanup_index_db_cache', side_effect=error
    ), mock.patch.object(cleanup_cli.log, 'error') as log_error:
        assert cleanup_cli.main(['--threshold', '12', '--target', '7']) == 1
    log_error.assert_called_once_with('%s', error)


def test_main_returns_zero_on_success():
    with mock.patch.object(cleanup_cli, 'cleanup_index_db_cache') as cleanup:
        assert cleanup_cli.main(['--threshold', '12', '--target', '7']) == 0
    cleanup.assert_called_once_with(threshold=12, target=7)
