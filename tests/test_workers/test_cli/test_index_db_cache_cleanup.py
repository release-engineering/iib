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
                    'tag': f'idb-{index:064d}',
                    'items': [{'created': timestamp, 'image': f'sha256:{index:064d}'}],
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

    assert [name for name, _ in sorted_cache_tags(image_stream)] == [
        f'idb-{index:064d}' for index in (1, 2, 0)
    ]


def test_sorted_cache_tags_uses_current_item_after_refresh():
    image_stream = {
        'status': {
            'tags': [
                {
                    'tag': 'idb-' + 'a' * 64,
                    'items': [
                        {'created': '2026-01-03T00:00:00Z'},
                        {'created': '2025-01-01T00:00:00Z'},
                    ],
                }
            ]
        }
    }

    assert sorted_cache_tags(image_stream)[0][1].year == 2026


@pytest.mark.parametrize('created', [None, 'not-a-timestamp'])
def test_sorted_cache_tags_retains_tag_with_invalid_current_timestamp(created):
    image_stream = {
        'status': {
            'tags': [
                {
                    'tag': 'idb-' + 'a' * 64,
                    'items': [{'created': created}],
                }
            ]
        }
    }

    assert sorted_cache_tags(image_stream)[0][1] == cleanup_cli.OLDEST_TIMESTAMP


def test_sorted_cache_tags_normalizes_naive_timestamp_to_utc():
    image_stream = _image_stream(
        '2026-01-01T00:00:00',
        '2026-01-02T00:00:00Z',
    )

    assert [name for name, _ in sorted_cache_tags(image_stream)] == [
        f'idb-{index:064d}' for index in (0, 1)
    ]


def test_sorted_cache_tags_uses_configured_tag_template():
    image_stream = {
        'status': {
            'tags': [
                {'tag': 'cache-' + 'a' * 64, 'items': [{'created': '2026-01-01T00:00:00Z'}]},
                {'tag': 'idb-' + 'b' * 64, 'items': [{'created': '2026-01-02T00:00:00Z'}]},
            ]
        }
    }

    assert [name for name, _ in sorted_cache_tags(image_stream, 'cache-{digest}')] == [
        'cache-' + 'a' * 64
    ]


def test_sorted_cache_tags_rejects_invalid_tag_template():
    with pytest.raises(ValueError, match='invalid index.db artifact tag template'):
        sorted_cache_tags({'status': {'tags': []}}, tag_template='bad-template')


@pytest.mark.parametrize('config', [{}, {'iib_index_db_artifact_tag_template': 123}])
def test_configured_tag_template_rejects_invalid_value(config):
    with mock.patch.object(cleanup_cli, 'get_worker_config', return_value=config):
        with pytest.raises(IIBError, match='iib_index_db_artifact_tag_template'):
            cleanup_cli._configured_tag_template()


def test_sorted_cache_tags_keeps_spec_only_cache_tags():
    image_stream = {
        'spec': {'tags': [{'name': 'idb-' + 'a' * 64}]},
        'status': {'tags': []},
    }

    assert [name for name, _ in sorted_cache_tags(image_stream)] == ['idb-' + 'a' * 64]


def test_tags_to_delete_deletes_oldest_batch_to_target():
    image_stream = _image_stream(*[f'2026-01-{day:02d}T00:00:00Z' for day in range(1, 6)])

    assert tags_to_delete(image_stream, threshold=4, target=2) == [
        f'idb-{index:064d}' for index in (0, 1, 2)
    ]


def test_tags_to_delete_starts_cleanup_at_threshold():
    image_stream = _image_stream(*[f'2026-01-{day:02d}T00:00:00Z' for day in range(1, 5)])

    assert tags_to_delete(image_stream, threshold=4, target=2) == [
        f'idb-{index:064d}' for index in (0, 1)
    ]


def test_tags_to_delete_with_zero_target_removes_all_tags():
    image_stream = _image_stream(*[f'2026-01-{day:02d}T00:00:00Z' for day in range(1, 4)])

    assert tags_to_delete(image_stream, threshold=3, target=0) == [
        f'idb-{index:064d}' for index in (0, 1, 2)
    ]


@pytest.mark.parametrize(
    ('threshold', 'target'),
    [(2, 2), (2, 3), (2, -1)],
)
def test_tags_to_delete_rejects_invalid_limits(threshold, target):
    with pytest.raises(ValueError, match='target must be non-negative and lower than threshold'):
        tags_to_delete({}, threshold=threshold, target=target)


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
        assert cleanup_index_db_cache(threshold=3, target=2) == [
            f'idb-{index:064d}' for index in (0, 1)
        ]
        assert run_cmd.call_count == 2
        run_cmd.assert_has_calls(
            [
                mock.call(
                    [
                        'oc',
                        'delete',
                        'imagestreamtag',
                        f'index-db-cache:idb-{0:064d}',
                        '--ignore-not-found',
                    ],
                    exc_msg=f'Failed to delete ImageStream tag idb-{0:064d}',
                ),
                mock.call(
                    [
                        'oc',
                        'delete',
                        'imagestreamtag',
                        f'index-db-cache:idb-{1:064d}',
                        '--ignore-not-found',
                    ],
                    exc_msg=f'Failed to delete ImageStream tag idb-{1:064d}',
                ),
            ]
        )


def test_missing_imagestream_is_safe():
    with mock.patch('iib.workers.cli.index_db_cache_cleanup._get_image_stream', return_value=None):
        assert cleanup_index_db_cache(threshold=2, target=1) == []


@pytest.mark.parametrize(
    'image_stream',
    [
        {'status': {'tags': 'invalid'}},
    ],
)
def test_malformed_imagestream_is_rejected(image_stream):
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup._get_image_stream', return_value=image_stream
    ), pytest.raises(IIBError, match='Invalid ImageStream'):
        cleanup_index_db_cache(threshold=2, target=1)


def test_invalid_cleanup_limits_are_not_reported_as_imagestream_errors():
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup._get_image_stream',
        return_value=_image_stream('2026-01-01T00:00:00Z'),
    ), pytest.raises(IIBError, match='target must be non-negative and lower than threshold') as exc:
        cleanup_index_db_cache(threshold=2, target=2)

    assert 'Invalid ImageStream' not in str(exc.value)


def test_tag_with_only_non_dict_items_is_kept_for_cleanup():
    name = 'idb-' + 'a' * 64
    image_stream = {'status': {'tags': [{'tag': name, 'items': [None, 'invalid']}]}}
    assert sorted_cache_tags(image_stream)[0][0] == name


def test_malformed_non_cache_tag_does_not_abort_cleanup():
    image_stream = {
        'status': {
            'tags': [
                {'tag': 'unrelated', 'items': 'invalid'},
                {'tag': 'idb-' + 'a' * 64, 'items': [{'created': '2026-01-01T00:00:00Z'}]},
            ]
        }
    }

    assert [name for name, _ in sorted_cache_tags(image_stream)] == ['idb-' + 'a' * 64]


@pytest.mark.parametrize(
    ('output', 'message'),
    [
        ('not-json', 'returned invalid JSON'),
        ('[]', 'returned invalid data'),
    ],
)
def test_get_image_stream_rejects_invalid_response(output, message):
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup.run_cmd', return_value=output
    ), pytest.raises(IIBError, match=message):
        cleanup_cli._get_image_stream()


def test_read_command_failure_is_propagated():
    with mock.patch(
        'iib.workers.cli.index_db_cache_cleanup.run_cmd',
        side_effect=IIBError('oc failed'),
    ), pytest.raises(IIBError, match='oc failed'):
        cleanup_index_db_cache(threshold=2, target=1)


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
        assert cleanup_cli._configured_value('cache_limit') == 42


def test_configured_value_rejects_missing_value():
    with mock.patch.object(cleanup_cli, 'get_worker_config', return_value={}):
        with pytest.raises(IIBError, match='cache_limit is not configured'):
            cleanup_cli._configured_value('cache_limit')


def test_configured_value_rejects_non_integer():
    with mock.patch.object(cleanup_cli, 'get_worker_config', return_value={'cache_limit': 'bad'}):
        with pytest.raises(IIBError, match='cache_limit must be an integer'):
            cleanup_cli._configured_value('cache_limit')


def test_main_passes_cli_arguments_to_cleanup():
    with mock.patch.object(cleanup_cli, 'cleanup_index_db_cache') as cleanup:
        assert cleanup_cli.main(['--threshold', '12', '--target', '7']) == 0
    cleanup.assert_called_once_with(threshold=12, target=7)


def test_main_falls_back_to_worker_config():
    with mock.patch.object(cleanup_cli, '_configured_value', side_effect=[12, 7]) as configured:
        with mock.patch.object(cleanup_cli, 'cleanup_index_db_cache') as cleanup:
            assert cleanup_cli.main([]) == 0
    assert configured.call_args_list == [
        mock.call('iib_index_db_cache_cleanup_threshold'),
        mock.call('iib_index_db_cache_cleanup_target'),
    ]
    cleanup.assert_called_once_with(threshold=12, target=7)


def test_main_combines_cli_argument_with_worker_config():
    with mock.patch.object(cleanup_cli, '_configured_value', return_value=7) as configured:
        with mock.patch.object(cleanup_cli, 'cleanup_index_db_cache') as cleanup:
            assert cleanup_cli.main(['--threshold', '12']) == 0
    configured.assert_called_once_with('iib_index_db_cache_cleanup_target')
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
