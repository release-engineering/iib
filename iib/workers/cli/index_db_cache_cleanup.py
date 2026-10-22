# SPDX-License-Identifier: GPL-3.0-or-later
"""Retention cleanup for the OpenShift ImageStream index.db cache."""

import argparse
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import run_cmd

log = logging.getLogger(__name__)

IMAGESTREAM_NAME = 'index-db-cache'
DEFAULT_THRESHOLD = 900
DEFAULT_TARGET = 500


def _parse_created(value: Any) -> datetime:
    """Parse an ImageStream tag creation timestamp."""
    if not isinstance(value, str):
        raise ValueError('tag item has no creation timestamp')
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError(f'invalid creation timestamp: {value!r}') from exc


def _tag_creation_time(tag: Dict[str, Any]) -> datetime:
    """Return the oldest image creation time for a tag."""
    items = tag.get('items')
    if not isinstance(items, list) or not items:
        raise ValueError(f'tag {tag.get("tag")!r} has no image items')
    valid_items = [item for item in items if isinstance(item, dict)]
    if not valid_items:
        raise ValueError(f'tag {tag.get("tag")!r} has no valid image items')
    return min(_parse_created(item.get('created')) for item in valid_items)


def sorted_cache_tags(image_stream: Dict[str, Any]) -> List[Tuple[str, datetime]]:
    """
    Return index.db cache tags ordered from oldest to newest.

    :param dict image_stream: OpenShift ImageStream JSON object
    :return: tag names paired with their creation timestamps
    :rtype: list of tuple
    :raises ValueError: if the ImageStream contains malformed tags or timestamps
    """
    status = image_stream.get('status')
    if not isinstance(status, dict) or not isinstance(status.get('tags'), list):
        raise ValueError('ImageStream status.tags is missing or malformed')

    result = []
    for tag in status['tags']:
        if not isinstance(tag, dict) or not isinstance(tag.get('tag'), str):
            raise ValueError('ImageStream contains a malformed tag entry')
        name = tag['tag']
        if name.startswith('idb-'):
            result.append((name, _tag_creation_time(tag)))
    return sorted(result, key=lambda item: (item[1], item[0]))


def tags_to_delete(image_stream: Dict[str, Any], threshold: int, target: int) -> List[str]:
    """
    Select the oldest cache tags needed to reduce the ImageStream to ``target`` tags.

    :param dict image_stream: OpenShift ImageStream JSON object
    :param int threshold: tag count at which cleanup starts
    :param int target: desired tag count after cleanup
    :return: names of tags selected for deletion
    :rtype: list of str
    :raises ValueError: if the limits are invalid or the ImageStream is malformed
    """
    if threshold <= target or target < 0:
        raise ValueError('target must be non-negative and lower than threshold')
    tags = sorted_cache_tags(image_stream)
    if len(tags) < threshold:
        return []
    return [name for name, _ in tags[: len(tags) - target]]


def _get_image_stream() -> Optional[Dict[str, Any]]:
    """Fetch the cache ImageStream from OpenShift."""
    output = run_cmd(
        [
            'oc',
            'get',
            'imagestream',
            IMAGESTREAM_NAME,
            '-o',
            'json',
            '--ignore-not-found',
        ],
        exc_msg=f'Failed to read ImageStream {IMAGESTREAM_NAME}',
    ).strip()
    if not output:
        return None
    try:
        data = json.loads(output)
    except json.JSONDecodeError as exc:
        raise IIBError(f'ImageStream {IMAGESTREAM_NAME} returned invalid JSON') from exc
    if not isinstance(data, dict):
        raise IIBError(f'ImageStream {IMAGESTREAM_NAME} returned invalid data')
    return data


def cleanup_index_db_cache(
    threshold: int = DEFAULT_THRESHOLD, target: int = DEFAULT_TARGET
) -> List[str]:
    """
    Delete old ImageStream cache tags when the configured threshold is reached.

    This command is intended to run asynchronously from the CronJob maintained in
    ``iib-playbooks``.

    :param int threshold: tag count at which cleanup starts
    :param int target: desired tag count after cleanup
    :return: names of the tags deleted
    :rtype: list of str
    :raises IIBError: if the ImageStream cannot be read, parsed, or updated
    """
    image_stream = _get_image_stream()
    if image_stream is None:
        log.info('ImageStream %s does not exist; nothing to clean', IMAGESTREAM_NAME)
        return []
    try:
        selected = tags_to_delete(image_stream, threshold, target)
    except ValueError as exc:
        raise IIBError(f'Invalid ImageStream {IMAGESTREAM_NAME}: {exc}') from exc

    for tag in selected:
        run_cmd(
            ['oc', 'delete', 'imagestreamtag', f'{IMAGESTREAM_NAME}:{tag}', '--ignore-not-found'],
            exc_msg=f'Failed to delete ImageStream tag {tag}',
        )
    log.info('Deleted %d old index.db cache tags', len(selected))
    return selected


def _configured_value(name: str, default: int) -> int:
    """Return a worker config value as an integer, or raise IIBError if invalid."""
    value = get_worker_config().get(name, default)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise IIBError(f'{name} must be an integer') from exc


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Run cache cleanup and return a process exit status.

    :param sequence argv: optional command line arguments; defaults to ``sys.argv``
    :return: zero on success and one if cleanup fails
    :rtype: int
    :raises SystemExit: if argparse rejects the command line
    """
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--threshold', type=int)
    parser.add_argument('--target', type=int)
    args = parser.parse_args(argv)
    try:
        cleanup_index_db_cache(
            threshold=args.threshold
            if args.threshold is not None
            else _configured_value('iib_index_db_cache_cleanup_threshold', DEFAULT_THRESHOLD),
            target=args.target
            if args.target is not None
            else _configured_value('iib_index_db_cache_cleanup_target', DEFAULT_TARGET),
        )
    except IIBError as exc:
        log.error('%s', exc)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
