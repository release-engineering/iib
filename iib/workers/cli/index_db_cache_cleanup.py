# SPDX-License-Identifier: GPL-3.0-or-later
"""Retention cleanup for the OpenShift ImageStream index.db cache."""

import argparse
import json
import logging
import re
from datetime import datetime
from datetime import timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from iib.exceptions import IIBError
from iib.workers.constants import INDEX_DB_IMAGESTREAM_NAME
from iib.workers.config import get_worker_config

# Reuse the worker command runner intentionally: this CLI needs the same subprocess logging and
# error handling as worker tasks. Moving it to a shared utility is a separate refactor.
from iib.workers.tasks.utils import run_cmd

log = logging.getLogger(__name__)

# Tags without a usable status history are retained at the beginning of the deletion order. This
# gives cleanup a chance to remove failed or partially-created imports that still consume capacity.
OLDEST_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)


def _parse_created(value: Any) -> datetime:
    """
    Parse an ImageStream tag creation timestamp into a timezone-aware datetime.

    OpenShift normally returns RFC 3339 timestamps ending in ``Z``. Python's ISO parser expects
    an explicit UTC offset, so the trailing ``Z`` is converted to ``+00:00`` before parsing.

    :param Any value: Raw ``created`` value from an ImageStream tag item.
    :return: Parsed timestamp.
    :rtype: datetime
    :raises ValueError: if ``value`` is not a string or is not a valid timestamp.
    """
    if not isinstance(value, str):
        raise ValueError('tag item has no creation timestamp')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError(f'invalid creation timestamp: {value!r}') from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _tag_creation_time(tag: Dict[str, Any]) -> datetime:
    """
    Return the creation time of the current image associated with an ImageStream tag.

    ImageStream tag history is ordered with the current image first. Older history entries must
    not influence retention ordering after a tag has been refreshed. If the tag has no usable
    history, the oldest possible timestamp is returned so the tag remains eligible for cleanup.

    :param dict tag: ImageStream status tag object.
    :return: Current image creation time, or :data:`OLDEST_TIMESTAMP` when history is unavailable.
    :rtype: datetime
    """
    items = tag.get('items')
    if not isinstance(items, list) or not items:
        return OLDEST_TIMESTAMP
    current_item = next((item for item in items if isinstance(item, dict)), None)
    if current_item is None:
        return OLDEST_TIMESTAMP
    try:
        return _parse_created(current_item.get('created'))
    except ValueError:
        return OLDEST_TIMESTAMP


def _tag_pattern(template: str) -> re.Pattern[str]:
    """
    Build a regular expression matching tags rendered from a digest-based template.

    The worker configuration validates that the template contains only a ``{digest}``
    placeholder. This helper still validates the structure defensively because the cleanup CLI
    can be invoked independently of worker startup validation.

    :param str template: Tag template containing exactly one ``{digest}`` placeholder.
    :return: Regular expression matching the literal template prefix and suffix around a
        64-character hexadecimal digest.
    :rtype: re.Pattern
    :raises ValueError: if the template does not contain a usable digest placeholder.
    """
    prefix, separator, suffix = template.partition('{digest}')
    if not separator or '{' in prefix or '}' in suffix:
        raise ValueError(f'invalid index.db artifact tag template: {template!r}')
    return re.compile(rf'^{re.escape(prefix)}[0-9a-fA-F]{{64}}{re.escape(suffix)}$')


def _configured_tag_template() -> str:
    """
    Return the configured index.db artifact tag template.

    :return: Configured digest-based artifact tag template.
    :rtype: str
    :raises IIBError: if the configured value is missing or not a string.
    """
    template = get_worker_config().get('iib_index_db_artifact_tag_template')
    if template is None:
        raise IIBError('iib_index_db_artifact_tag_template is not configured')
    if not isinstance(template, str):
        raise IIBError('iib_index_db_artifact_tag_template must be a string')
    return template


def _spec_cache_tags(image_stream: Dict[str, Any], pattern: re.Pattern[str]) -> Set[str]:
    """
    Return cache tag names declared in the ImageStream specification.

    A failed or in-progress import can leave a tag in ``spec.tags`` without a corresponding
    successful entry in ``status.tags``. Such tags still consume ImageStream capacity and must be
    included in retention decisions.

    :param dict image_stream: OpenShift ImageStream JSON object.
    :param re.Pattern pattern: Pattern identifying tags generated by the configured template.
    :return: Matching tag names declared in ``spec.tags``.
    :rtype: Set[str]
    """
    spec = image_stream.get('spec')
    entries = spec.get('tags', []) if isinstance(spec, dict) else []
    if not isinstance(entries, list):
        return set()
    names = set()
    for entry in entries:
        name = (
            entry
            if isinstance(entry, str)
            else entry.get('name')
            if isinstance(entry, dict)
            else None
        )
        if isinstance(name, str) and pattern.fullmatch(name):
            names.add(name)
    return names


def sorted_cache_tags(
    image_stream: Dict[str, Any], tag_template: Optional[str] = None
) -> List[Tuple[str, datetime]]:
    """
    Return index.db cache tags ordered from oldest to newest.

    Successful imports are read from ``status.tags`` and ordered using the current image's
    creation time. Tags found only in ``spec.tags`` or tags with no usable status history are
    assigned :data:`OLDEST_TIMESTAMP`, ensuring that failed imports count toward the threshold and
    are removed before healthy, recently refreshed entries. Non-cache tags and malformed entries
    unrelated to the configured cache template are ignored.

    :param dict image_stream: OpenShift ImageStream JSON object
    :param str tag_template: digest-based cache tag template; defaults to worker config
    :return: Tag names paired with their effective creation timestamps.
    :rtype: list of tuple[str, datetime]
    :raises ValueError: if the ImageStream status structure or tag template is malformed
    """
    pattern = _tag_pattern(tag_template or _configured_tag_template())
    status = image_stream.get('status')
    status_tags = status.get('tags', []) if isinstance(status, dict) else []
    if not isinstance(status_tags, list):
        raise ValueError('ImageStream status.tags is malformed')

    # Use a mapping so a tag present in both status.tags and spec.tags is counted only once.
    result = {}
    for tag in status_tags:
        if not isinstance(tag, dict) or not isinstance(tag.get('tag'), str):
            continue
        name = tag['tag']
        if pattern.fullmatch(name):
            result[name] = _tag_creation_time(tag)

    # Merge spec-only tags after status processing; setdefault preserves a real status timestamp.
    for name in _spec_cache_tags(image_stream, pattern):
        result.setdefault(name, OLDEST_TIMESTAMP)

    return sorted(result.items(), key=lambda item: (item[1], item[0]))


def tags_to_delete(image_stream: Dict[str, Any], threshold: int, target: int) -> List[str]:
    """
    Select the oldest cache tags needed to reduce the ImageStream to ``target`` tags.

    Cleanup is deliberately performed in one batch once the threshold is reached. If the number
    of matching tags is below the threshold, the function returns an empty list and leaves the
    ImageStream untouched.

    :param dict image_stream: OpenShift ImageStream JSON object
    :param int threshold: tag count at which cleanup starts
    :param int target: desired tag count after cleanup
    :return: Names of tags selected for deletion, ordered from oldest to newest.
    :rtype: list of str
    :raises ValueError: if ``target`` is negative, ``threshold`` is not greater than ``target``,
        or the ImageStream structure is malformed.
    """
    if threshold <= target or target < 0:
        raise ValueError('target must be non-negative and lower than threshold')
    tags = sorted_cache_tags(image_stream)
    if len(tags) < threshold:
        return []
    return [name for name, _ in tags[: len(tags) - target]]


def _validate_cleanup_limits(threshold: int, target: int) -> None:
    """Validate the cache cleanup limits before reading an ImageStream."""
    if threshold <= target or target < 0:
        raise IIBError('target must be non-negative and lower than threshold')


def _get_image_stream() -> Optional[Dict[str, Any]]:
    """
    Fetch the index.db cache ImageStream from OpenShift.

    ``oc get --ignore-not-found`` produces an empty string when the ImageStream does not exist;
    that condition is treated as a safe no-op because there is nothing to clean. Any non-empty
    response must be a JSON object representing an ImageStream.

    :return: ImageStream JSON object, or ``None`` when the ImageStream does not exist.
    :rtype: dict or None
    :raises IIBError: if the ``oc`` command fails, returns invalid JSON, or returns a JSON value
        that is not an object.
    """
    output = run_cmd(
        [
            'oc',
            'get',
            'imagestream',
            INDEX_DB_IMAGESTREAM_NAME,
            '-o',
            'json',
            '--ignore-not-found',
        ],
        exc_msg=f'Failed to read ImageStream {INDEX_DB_IMAGESTREAM_NAME}',
    ).strip()
    if not output:
        return None
    try:
        data = json.loads(output)
    except json.JSONDecodeError as exc:
        raise IIBError(f'ImageStream {INDEX_DB_IMAGESTREAM_NAME} returned invalid JSON') from exc
    if not isinstance(data, dict):
        raise IIBError(f'ImageStream {INDEX_DB_IMAGESTREAM_NAME} returned invalid data')
    return data


def _configured_value(name: str) -> int:
    """
    Read and convert an integer-valued worker configuration setting.

    CLI flags take precedence over configuration, but this helper supplies the configured value
    when a flag was omitted. Conversion is performed here so invalid environment/configuration
    values result in a controlled ``IIBError`` instead of a traceback from ``main``.

    :param str name: Configuration key to read.
    :return: Configuration value converted to an integer.
    :rtype: int
    :raises IIBError: if the configuration value is missing or cannot be converted to an integer.
    """
    value = get_worker_config().get(name)
    if value is None:
        raise IIBError(f'{name} is not configured')
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise IIBError(f'{name} must be an integer') from exc


def cleanup_index_db_cache(threshold: int, target: int) -> List[str]:
    """
    Delete old ImageStream cache tags when the configured threshold is reached.

    The command is intended to run asynchronously from the CronJob maintained in
    ``iib-playbooks``. It is idempotent: deleting an already-removed tag is safe because the
    generated ``oc delete`` command uses ``--ignore-not-found``.

    The ImageStream is read once, the deletion batch is calculated locally, and each selected tag
    is then deleted individually. If a deletion fails, the corresponding ``IIBError`` is allowed
    to propagate so the CronJob reports a failed run.

    :param int threshold: tag count at which cleanup starts
    :param int target: desired tag count after cleanup
    :return: Names of the tags selected for deletion.
    :rtype: list of str
    :raises IIBError: if the ImageStream cannot be read or parsed, the limits are invalid, or a
        tag deletion fails.
    """
    _validate_cleanup_limits(threshold, target)
    image_stream = _get_image_stream()
    if image_stream is None:
        log.info('ImageStream %s does not exist; nothing to clean', INDEX_DB_IMAGESTREAM_NAME)
        return []
    try:
        selected = tags_to_delete(image_stream, threshold, target)
    except ValueError as exc:
        raise IIBError(f'Invalid ImageStream {INDEX_DB_IMAGESTREAM_NAME}: {exc}') from exc

    for tag in selected:
        run_cmd(
            [
                'oc',
                'delete',
                'imagestreamtag',
                f'{INDEX_DB_IMAGESTREAM_NAME}:{tag}',
                '--ignore-not-found',
            ],
            exc_msg=f'Failed to delete ImageStream tag {tag}',
        )
    log.info('Deleted %d old index.db cache tags', len(selected))
    return selected


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Parse CLI options, run cache cleanup, and return a process exit status.

    ``--threshold`` and ``--target`` override the corresponding worker configuration values for
    one invocation. ``IIBError`` is logged and translated to exit status ``1`` so this function
    can be used directly as a ``console_scripts`` entry point from an OpenShift CronJob.

    :param sequence argv: optional command line arguments; defaults to ``sys.argv``
    :return: ``0`` on success and ``1`` when cleanup reports an ``IIBError``.
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
            threshold=(
                args.threshold
                if args.threshold is not None
                else _configured_value('iib_index_db_cache_cleanup_threshold')
            ),
            target=(
                args.target
                if args.target is not None
                else _configured_value('iib_index_db_cache_cleanup_target')
            ),
        )
    except IIBError as exc:
        log.error('%s', exc)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
