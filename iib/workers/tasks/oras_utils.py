# SPDX-License-Identifier: GPL-3.0-or-later
"""Pull and publish OCI artifacts, including index.db and its Git binding."""

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from iib.common.tracing import instrument_tracing
from iib.exceptions import ArtifactNotFoundError, IIBError
from iib.workers.constants import INDEX_DB_IMAGESTREAM_NAME
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import (
    get_image_digest,
    run_cmd,
    sanitize_log_text,
    set_registry_auths,
)  # noqa: F401

log = logging.getLogger(__name__)

GIT_COMMIT_ANNOTATION = 'iib.git_commit'
CONFIGS_TREE_ANNOTATION = 'iib.configs_tree'
INDEX_DB_SHA256_ANNOTATION = 'iib.index_db_sha256'
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')


class _ArtifactResolutionError(IIBError):
    """Distinguish an unavailable artifact tag from an invalid artifact binding."""


class _MissingLegacyBindingError(IIBError):
    """Identify the one case where a new legacy sidecar may be published."""


@dataclass(frozen=True)
class PublishedIndexDbArtifact:
    """Pinned database payload with its declared Git revision and producer.

    New artifacts carry the binding in their manifest. Legacy artifacts keep
    their original manifest and obtain the binding from a separate pinned
    sidecar that identifies that exact manifest and database payload. The
    caller verifies the referenced Git commit before using this catalog.
    """

    artifact_ref: str
    manifest_ref: str
    index_db_path: str
    request_id: int
    git_commit: str
    configs_tree: str
    database_sha256: str


def index_db_sha256(index_db_path: str) -> str:
    """Calculate the digest of a database payload without loading it into memory."""
    digest = hashlib.sha256()
    with open(index_db_path, 'rb') as database:
        for chunk in iter(lambda: database.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _oras_auth_args() -> list[str]:
    """Use the same exclusive registry configuration as artifact pulls and pushes."""
    auth_path = get_worker_config()['iib_index_db_oras_auth_path']
    return ['--registry-config', auth_path] if auth_path and os.path.exists(auth_path) else []


def _resolve_oras_manifest(artifact_ref: str) -> str:
    """Pin an artifact tag to one OCI manifest digest before inspecting its content."""
    try:
        with set_registry_auths(None, use_empty_config=True):
            digest = run_cmd(
                ['oras', 'resolve', *_oras_auth_args(), artifact_ref],
                exc_msg=f'Failed to resolve OCI artifact {artifact_ref}',
            ).strip()
    except IIBError as exc:
        raise _ArtifactResolutionError(f'Failed to resolve OCI artifact {artifact_ref}') from exc
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise IIBError(f'Invalid OCI manifest digest for {sanitize_log_text(artifact_ref)}')
    return f"{artifact_ref.rsplit(':', 1)[0]}@{digest}"


def _manifest_annotations(manifest_ref: str) -> Dict[str, str]:
    """Read a pinned manifest and verify that its sole payload is index.db."""
    with set_registry_auths(None, use_empty_config=True):
        manifest_json = run_cmd(
            ['oras', 'manifest', 'fetch', *_oras_auth_args(), manifest_ref],
            exc_msg=f'Failed to read OCI manifest {manifest_ref}',
        )
    try:
        manifest = json.loads(manifest_json)
        if not isinstance(manifest, dict):
            raise TypeError('OCI manifest is not an object')
        annotations = manifest.get('annotations', {})
        layers = manifest['layers']
    except (ValueError, KeyError, TypeError) as exc:
        raise IIBError('Published index.db artifact has no usable OCI manifest') from exc
    if not isinstance(annotations, dict) or not isinstance(layers, list) or len(layers) != 1:
        raise IIBError('Published index.db artifact has ambiguous manifest content')
    if not isinstance(layers[0], dict):
        raise IIBError('Published index.db artifact does not contain index.db')
    layer_annotations = layers[0].get('annotations')
    if not isinstance(layer_annotations, dict) or (
        layer_annotations.get('org.opencontainers.image.title') != 'index.db'
    ):
        raise IIBError('Published index.db artifact does not contain index.db')
    payload_digest = annotations.get(INDEX_DB_SHA256_ANNOTATION)
    if payload_digest and layers[0].get('digest') != f'sha256:{payload_digest}':
        raise IIBError('Published index.db artifact layer conflicts with its binding')
    return annotations


def _legacy_binding_ref(image_resolved: str) -> str:
    """Name the sidecar tag without changing an existing digest-keyed artifact."""
    conf = get_worker_config()
    tag = f'{_get_content_addressed_artifact_tag(image_resolved)}-binding'
    return conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'], tag=tag
    )


def _read_legacy_binding(image_resolved: str, base_dir: str) -> Dict[str, Any]:
    """Pull a sidecar by manifest digest, or report that it is truly absent.

    A failed ``oras resolve`` alone cannot distinguish a missing tag from a
    registry or authentication failure. Only an explicit 404 permits onboarding.
    """
    sidecar_ref = _legacy_binding_ref(image_resolved)
    try:
        pinned_ref = _resolve_oras_manifest(sidecar_ref)
    except _ArtifactResolutionError as exc:
        try:
            get_oras_artifact(sidecar_ref, base_dir)
        except ArtifactNotFoundError as not_found:
            raise _MissingLegacyBindingError(
                'Published legacy index.db artifact has no verified sidecar'
            ) from not_found
        raise exc
    sidecar_dir = get_oras_artifact(pinned_ref, base_dir)
    sidecar_file = Path(sidecar_dir) / 'binding.json'
    if not sidecar_file.is_file():
        raise IIBError('Published legacy index.db sidecar has no binding.json payload')
    try:
        binding = json.loads(sidecar_file.read_text())
    except (OSError, ValueError) as exc:
        raise IIBError('Published legacy index.db sidecar is unreadable') from exc
    if not isinstance(binding, dict) or binding.get('version') != 1:
        raise IIBError('Published legacy index.db sidecar has an unsupported format')
    return binding


def _verified_git_ref(
    image_resolved: str, git_repo_path: str, git_commit: str, configs_tree: str, request_id: int
) -> None:
    """Verify an operator-supplied Git revision and retain it under the image digest.

    The matching commit, configs tree, and producer must be available before
    a legacy sidecar can claim that this Git revision belongs to the database.
    """
    if run_cmd(['git', '-C', git_repo_path, 'cat-file', '-t', git_commit]).strip() != 'commit':
        raise IIBError('Verified legacy Git revision is not a commit')
    tree = run_cmd(['git', '-C', git_repo_path, 'rev-parse', f'{git_commit}:configs']).strip()
    if tree != configs_tree:
        raise IIBError('Verified legacy Git revision has a different configs tree')
    metadata_json = run_cmd(
        ['git', '-C', git_repo_path, 'show', f'{git_commit}:.iib-build-metadata.json']
    )
    try:
        metadata = json.loads(metadata_json)
    except ValueError as exc:
        raise IIBError('Verified legacy Git revision has invalid producer metadata') from exc
    if not isinstance(metadata, dict) or metadata.get('request_id') != request_id:
        raise IIBError('Verified legacy Git revision has a different producer')

    digest = _get_index_digest(image_resolved)
    retained_ref = f'refs/tags/iib-index-{digest}'
    remote = run_cmd(['git', '-C', git_repo_path, 'ls-remote', 'origin', retained_ref]).strip()
    if remote:
        if remote.split()[0] != git_commit:
            raise IIBError('Legacy Git retained ref is bound to another commit')
    else:
        run_cmd(
            ['git', '-C', git_repo_path, 'push', 'origin', f'{git_commit}:{retained_ref}'],
            exc_msg='Failed to retain legacy Git revision',
        )
    confirmed = run_cmd(['git', '-C', git_repo_path, 'ls-remote', 'origin', retained_ref]).strip()
    if not confirmed or confirmed.split()[0] != git_commit:
        raise IIBError('Legacy Git retained ref could not be verified')


def publish_verified_legacy_binding(
    image_resolved: str,
    base_dir: str,
    *,
    git_repo_path: str,
    git_commit: str,
    configs_tree: str,
    request_id: int,
    database_sha256: str,
    original_manifest_digest: str,
) -> None:
    """Onboard a legacy digest using explicit, independently verified inputs.

    The operator must verify externally that the Git revision belongs to the
    published image. This function checks the original immutable artifact,
    database bytes, Git tree and producer, and creates a separate binding tag.
    ``original_manifest_digest`` and ``database_sha256`` are the values the
    operator verified, not values inferred from the current Git branch. It
    never rewrites the original digest-keyed database artifact. A matching
    sidecar is reused on retry; conflicting content stops onboarding.

    :param str image_resolved: Digest-pinned public index image
    :param str base_dir: Existing directory for temporary ORAS pulls
    :param str git_repo_path: Checkout containing the independently verified commit
    :param str git_commit: Commit used to build the published index image
    :param str configs_tree: Git object ID of that commit's configs tree
    :param int request_id: Producer recorded by that commit's build metadata
    :param str database_sha256: Independently verified SHA256 of index.db
    :param str original_manifest_digest: Verified digest of the legacy OCI artifact
    :raises IIBError: If any supplied value conflicts with Git, Quay, or an existing sidecar
    """
    image_digest_match = re.search(r'@sha256:([0-9a-f]{64})$', image_resolved)
    if not image_digest_match or not re.fullmatch(r'sha256:[0-9a-f]{64}', original_manifest_digest):
        raise IIBError('Legacy onboarding requires pinned image and artifact digests')
    if (
        not re.fullmatch(r'[0-9a-f]{40,64}', git_commit)
        or not re.fullmatch(r'[0-9a-f]{40,64}', configs_tree)
        or not _SHA256_RE.fullmatch(database_sha256)
        or request_id < 0
    ):
        raise IIBError('Legacy onboarding has invalid Git or database binding inputs')

    artifact_ref = get_indexdb_artifact_pullspec(image_resolved)
    original_manifest_ref = _resolve_oras_manifest(artifact_ref)
    if original_manifest_ref.split('@', 1)[1] != original_manifest_digest:
        raise IIBError('Legacy database artifact manifest differs from verified input')
    annotations = _manifest_annotations(original_manifest_ref)
    if annotations.get('request_id') is not None and annotations['request_id'] != str(request_id):
        raise IIBError('Legacy database artifact producer differs from verified input')
    artifact_dir = get_oras_artifact(original_manifest_ref, base_dir)
    index_db_path = Path(artifact_dir) / 'index.db'
    if not index_db_path.is_file() or index_db_sha256(str(index_db_path)) != database_sha256:
        raise IIBError('Legacy database payload differs from verified input')
    _verified_git_ref(image_resolved, git_repo_path, git_commit, configs_tree, request_id)

    binding = {
        'version': 1,
        'image_digest': image_digest_match.group(1),
        'original_manifest_digest': original_manifest_digest,
        'database_sha256': database_sha256,
        'request_id': request_id,
        'git_commit': git_commit,
        'configs_tree': configs_tree,
    }
    sidecar_ref = _legacy_binding_ref(image_resolved)
    try:
        existing = _read_legacy_binding(image_resolved, base_dir)
    except _MissingLegacyBindingError:
        pass
    else:
        if existing != binding:
            raise IIBError('Legacy digest sidecar has a conflicting binding')
        return
    with tempfile.TemporaryDirectory(prefix='iib-legacy-binding-', dir=base_dir) as sidecar_dir:
        (Path(sidecar_dir) / 'binding.json').write_text(json.dumps(binding, sort_keys=True))
        push_oras_artifact(
            sidecar_ref,
            'binding.json',
            artifact_type='application/vnd.iib.index-db-binding+json',
            cwd=sidecar_dir,
        )
    if _read_legacy_binding(image_resolved, base_dir) != binding:
        raise IIBError('Published legacy digest sidecar could not be verified')


def read_published_index_db_artifact(
    image_resolved: str, base_dir: str
) -> PublishedIndexDbArtifact:
    """Read a verified Git/database binding from the image's digest-keyed Quay artifact.

    Resolve the artifact tag once, then read its manifest and database from that
    pinned digest. A legacy artifact without Git annotations may use a sidecar
    only if the sidecar names the same original manifest, image digest, producer,
    and database hash. This path bypasses the ImageStream cache so a cached
    payload cannot be paired with another artifact's annotations.

    :param str image_resolved: Digest-pinned public index image
    :param str base_dir: Existing directory for temporary ORAS pulls
    :return: Database path and its artifact-verified Git binding; the caller
        must verify the referenced Git commit before applying operations
    :raises IIBError: If the artifact, sidecar, or payload is missing or conflicts
    """
    artifact_ref = get_indexdb_artifact_pullspec(image_resolved)
    manifest_ref = _resolve_oras_manifest(artifact_ref)
    annotations = _manifest_annotations(manifest_ref)
    required_binding_annotations = {
        GIT_COMMIT_ANNOTATION,
        CONFIGS_TREE_ANNOTATION,
        INDEX_DB_SHA256_ANNOTATION,
    }
    present_binding_annotations = required_binding_annotations.intersection(annotations)
    legacy_binding = None
    if not present_binding_annotations:
        legacy_binding = _read_legacy_binding(image_resolved, base_dir)
        expected_digest = _get_index_digest(image_resolved)
        legacy_producer = legacy_binding.get('request_id')
        if (
            legacy_binding.get('image_digest') != expected_digest
            or legacy_binding.get('original_manifest_digest') != manifest_ref.split('@', 1)[1]
            or not isinstance(legacy_producer, int)
            or legacy_producer < 0
            or (
                annotations.get('request_id') is not None
                and str(legacy_producer) != annotations['request_id']
            )
        ):
            raise IIBError('Published legacy index.db sidecar conflicts with the artifact')
    elif present_binding_annotations != required_binding_annotations:
        raise IIBError('Published index.db artifact has an incomplete Git binding')
    try:
        producer = (
            legacy_binding['request_id']
            if legacy_binding is not None
            else annotations['request_id']
        )
        git_commit = (
            legacy_binding['git_commit']
            if legacy_binding is not None
            else annotations[GIT_COMMIT_ANNOTATION]
        )
        configs_tree = (
            legacy_binding['configs_tree']
            if legacy_binding is not None
            else annotations[CONFIGS_TREE_ANNOTATION]
        )
        database_sha256 = (
            legacy_binding['database_sha256']
            if legacy_binding is not None
            else annotations[INDEX_DB_SHA256_ANNOTATION]
        )
        request_id = int(producer)
    except (KeyError, TypeError, ValueError) as exc:
        raise IIBError('Published index.db artifact has no verified Git binding') from exc
    if (
        request_id < 0
        or not isinstance(git_commit, str)
        or not re.fullmatch(r'[0-9a-f]{40,64}', git_commit)
        or not isinstance(configs_tree, str)
        or not re.fullmatch(r'[0-9a-f]{40,64}', configs_tree)
        or not isinstance(database_sha256, str)
        or not _SHA256_RE.fullmatch(database_sha256)
    ):
        raise IIBError('Published index.db artifact has an invalid Git binding')
    artifact_dir = get_oras_artifact(manifest_ref, base_dir)
    index_db_path = Path(artifact_dir) / 'index.db'
    if not index_db_path.is_file():
        raise IIBError('Published index.db artifact has no index.db payload')
    if index_db_sha256(str(index_db_path)) != database_sha256:
        raise IIBError('Published index.db artifact database digest conflicts with its binding')
    return PublishedIndexDbArtifact(
        artifact_ref=artifact_ref,
        manifest_ref=manifest_ref,
        index_db_path=str(index_db_path),
        request_id=request_id,
        git_commit=git_commit,
        configs_tree=configs_tree,
        database_sha256=database_sha256,
    )


def find_existing_index_db_artifact(
    image_resolved: str, base_dir: str
) -> Optional[PublishedIndexDbArtifact]:
    """Return a verified digest binding, or None only for a confirmed missing tag.

    Malformed bindings and registry errors must stop publication; otherwise a
    retry could overwrite an existing artifact with different catalog contents.
    """
    try:
        return read_published_index_db_artifact(image_resolved, base_dir)
    except _ArtifactResolutionError as resolution_error:
        # ORAS resolve errors do not preserve the missing-artifact distinction.
        # Probe with the existing pull helper; only its explicit 404 signal means
        # that a new digest tag may be created.
        try:
            get_oras_artifact(get_indexdb_artifact_pullspec(image_resolved), base_dir)
        except ArtifactNotFoundError:
            return None
        raise resolution_error


def verify_existing_index_db_artifact(
    artifact: PublishedIndexDbArtifact,
    *,
    request_id: int,
    git_commit: str,
    configs_tree: str,
    index_db_path: str,
) -> None:
    """Allow retry reuse only for the same producer, commit, tree, and DB bytes."""
    if (
        artifact.request_id != request_id
        or artifact.git_commit != git_commit
        or artifact.configs_tree != configs_tree
        or artifact.database_sha256 != index_db_sha256(index_db_path)
    ):
        raise IIBError('Existing digest-keyed index.db artifact has a conflicting binding')


def _get_name_and_tag_from_pullspec(image_pullspec: str) -> Tuple[str, str]:
    """
    Parse a container image pullspec (URL) to extract the index name and tag.

    :param str image_pullspec: The full image pullspec string (registry/path/name:tag[@digest]).
    :returns Tuple[str, str]: The extracted index name and tag (e.g., 'iib-pub-pending', 'v4.17').
    :raises IIBError: If the pullspec is missing the required tag delimiter (':')
                      or if the name:tag structure cannot be parsed.
    """
    # Regex to capture the image name and tag, ignoring an optional digest.
    # r'/([^/:]+):([^@]+)(@.*)?$'
    # Group 1: ([^/:]+) -> Image Name (the last path segment before the colon)
    # Group 2: ([^@]+)  -> Tag (the part after the colon, before '@' or end of string)
    # Group 3: (@.*)?   -> Optional digest part, which is ignored
    regex = re.compile(r'/([^/:]+):([^@]+)(@.*)?$')
    match = regex.search(image_pullspec)

    if not match:
        # Check for the most common error: missing the tag delimiter (:)
        if ':' not in image_pullspec:
            raise IIBError(
                f"Invalid pullspec format: '{image_pullspec}'. Missing tag (':') delimiter."
            )

        # Raise a general error if the regex failed for other reasons
        raise IIBError(
            f"Invalid pullspec format: '{image_pullspec}'. Could not parse name:tag structure."
        )

    # Group 1: Image Name (e.g., 'iib-pub-pending')
    index_name = match.group(1)

    # Group 2: Tag (e.g., 'v4.17')
    tag = match.group(2)

    # Final check to ensure the tag isn't empty (e.g., 'image:')
    if not tag:
        raise IIBError(f"Invalid pullspec format: '{image_pullspec}'. Tag is present but empty.")

    return index_name, tag


def _get_index_digest(pullspec: str) -> str:
    """
    Get the SHA-256 manifest digest used in artifact tags.

    Use the digest from a pinned pull specification directly so the database artifact
    is keyed to the selected image without another registry lookup.

    :param str pullspec: The tagged or digest-pinned index image pull specification.
    :return: The 64-char hex digest without the ``sha256:`` prefix.
    :rtype: str
    """
    # Match only a digest pinned at the end of the pullspec. For example,
    # "registry/catalog:v4@sha256:<64 hex chars>" yields the 64 hex chars;
    # "registry/catalog:v4" falls through to a registry lookup below.
    digest_match = re.search(r'@sha256:(?P<digest>[0-9a-f]{64})$', pullspec)
    if digest_match:
        return digest_match.group('digest')

    return get_image_digest(pullspec).split(':', 1)[-1]


def get_index_tag(from_index: str) -> str:
    """
    Return only the tag portion of a full index image pullspec.

    :param str from_index: The full index image pullspec (registry/namespace/repo:tag).
    :return: The tag portion of the pullspec.
    :rtype: str
    """
    _, tag = _get_name_and_tag_from_pullspec(from_index)
    return tag


def _get_content_addressed_artifact_tag(from_index: str) -> str:
    """
    Generate the content-addressed artifact/ImageStream tag for an index image.

    Keyed on the image's manifest digest ALONE (not name/tag/pullspec) so that
    identical content addressed by different pullspecs (e.g. a released mirror)
    shares one artifact, while different content never collides.

    :param str from_index: The full index image pullspec (registry/namespace/repo:tag).
    :return: Combined tag, e.g. "idb-<64-hex-sha256>".
    :rtype: str
    """
    return get_worker_config()['iib_index_db_artifact_tag_template'].format(
        digest=_get_index_digest(from_index)
    )


def get_indexdb_artifact_pullspec(from_index: str) -> str:
    """
    Construct the full pullspec for index_db artifact.

    :param str from_index: The original full pullspec of the index image.

    :raises IIBError: If the pullspec parsing fails within the helper function.
    :returns str: The full, formatted pullspec for the internal index DB artifact.
    :rtype: str
    """
    conf = get_worker_config()

    return conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'],
        tag=_get_content_addressed_artifact_tag(from_index),
    )


def get_request_indexdb_artifact_pullspec(index_image: str, request_id: int) -> str:
    """
    Construct the per-request index.db artifact pullspec.

    :param str index_image: The resolved output index image.
    :param int request_id: The IIB request that produced the artifact.
    :return: The request-specific index.db artifact pullspec.
    :rtype: str
    """
    conf = get_worker_config()
    tag = f'{_get_content_addressed_artifact_tag(index_image)}-{request_id}'
    return conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'], tag=tag
    )


@instrument_tracing(span_name="workers.tasks.oras_utils.get_oras_artifact")
def get_oras_artifact(
    artifact_ref: str,
    base_dir: str,
    registry_auths: Optional[Dict[str, Any]] = None,
    temp_dir_prefix: str = "iib-oras-",
) -> str:
    """
    Pull an OCI artifact from a registry to a temporary directory.

    This function is equivalent to: `oras pull {artifact_ref} -o {temp_dir}`

    :param str artifact_ref: OCI artifact reference (e.g., 'quay.io/repo/repo:tag')
    :param str base_dir: Base directory where the temporary subdirectory will be created.
        Can be an absolute or relative path. If relative, the directory must exist.
        The function always returns an absolute path regardless of the base_dir type.
    :param dict registry_auths: Optional dockerconfig.json auth information for private registries
    :param str temp_dir_prefix: Prefix for the temporary directory name
    :return: Path to the temporary directory containing the artifact (always absolute)
    :rtype: str
    :raises ArtifactNotFoundError: If the registry reports that the artifact does not exist
    :raises IIBError: If the pull operation fails for any other reason
    """
    log.info('Pulling OCI artifact %s to temporary directory', sanitize_log_text(artifact_ref))

    # Create a subdirectory within the provided base_dir
    temp_dir = tempfile.mkdtemp(prefix=temp_dir_prefix, dir=base_dir)

    # Use exclusive registry authentication file or the provided/default Docker config.json
    with set_registry_auths(registry_auths, use_empty_config=True):
        conf = get_worker_config()
        oras_exclusive_auth_path = conf['iib_index_db_oras_auth_path']
        cmd_args = []
        if oras_exclusive_auth_path and os.path.exists(oras_exclusive_auth_path):
            cmd_args = ['--registry-config', oras_exclusive_auth_path]
            log.debug('Using ORAS registry configuration file: %s', oras_exclusive_auth_path)
        else:
            log.warning(
                'No ORAS registry configuration file found, using default Docker config.json'
            )
        try:
            run_cmd(
                ['oras', 'pull', *cmd_args, artifact_ref, '-o', temp_dir],
                exc_msg=f'Failed to pull OCI artifact {artifact_ref}',
            )
            log.info(
                'Successfully pulled OCI artifact %s to %s',
                sanitize_log_text(artifact_ref),
                temp_dir,
            )
            return temp_dir
        except Exception as e:
            # Clean up temp directory on failure
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            # Preserve the "the artifact is absent" signal through the wrap, so callers can
            # tell a never-onboarded image from a registry that failed to answer.
            exc_msg = sanitize_log_text(f'Failed to pull OCI artifact {artifact_ref}: {e}')
            if isinstance(e, ArtifactNotFoundError):
                raise ArtifactNotFoundError(exc_msg) from e
            raise IIBError(exc_msg) from e


@instrument_tracing(span_name="workers.tasks.oras_utils.push_oras_artifact")
def push_oras_artifact(
    artifact_ref: str,
    local_path: str,
    artifact_type: str = "application/vnd.sqlite",
    registry_auths: Optional[Dict[str, Any]] = None,
    annotations: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
) -> None:
    """
    Push a local artifact to an OCI registry using ORAS.

    This function is equivalent to: `oras push {artifact_ref} {local_path}:{artifact_type}`

    :param str artifact_ref: OCI artifact reference to push to (e.g., 'quay.io/repo/repo:tag')
    :param str local_path: Local path to the artifact file. Should be a relative path.
        When using cwd, this should be a relative path (typically just
        the filename) relative to the cwd directory.
    :param str artifact_type: MIME type of the artifact (default: 'application/vnd.sqlite')
    :param dict registry_auths: Optional dockerconfig.json auth information for private registries
    :param dict annotations: Optional annotations to add to the artifact
    :param str cwd: Optional working directory for the ORAS command. When provided, local_path
        should be relative to this directory (e.g., just the filename).
    :raises IIBError: If the push operation fails
    """
    log.info(
        'Pushing artifact from %s to %s with type %s',
        local_path,
        sanitize_log_text(artifact_ref),
        artifact_type,
    )
    if cwd:
        log.info('Using working directory: %s', cwd)

    # Construct the full path for validation
    full_path = os.path.join(cwd, local_path) if cwd else local_path
    if not os.path.exists(full_path):
        raise IIBError(f'Local artifact path does not exist: {full_path}')

    # Use exclusive registry authentication file or the provided/default Docker config.json
    with set_registry_auths(registry_auths, use_empty_config=True):
        conf = get_worker_config()
        oras_exclusive_auth_path = conf['iib_index_db_oras_auth_path']
        cmd_args = []
        if oras_exclusive_auth_path and os.path.exists(oras_exclusive_auth_path):
            cmd_args = ['--registry-config', oras_exclusive_auth_path]
            log.debug('Using ORAS registry configuration file: %s', oras_exclusive_auth_path)
        else:
            log.warning(
                'No ORAS registry configuration file found, using default Docker config.json'
            )

        # Build ORAS push command
        cmd = ['oras', 'push', *cmd_args, artifact_ref, f'{local_path}:{artifact_type}']

        # Do not allow absolute paths.
        # Absolute paths are extracted to the same place (full path) which might cause collisions.
        if os.path.isabs(local_path):
            log.error('Local artifact path must be relative: %s', local_path)
            raise IIBError(f'Local artifact path must be relative: {local_path}')

        # Add annotations if provided
        if annotations:
            for key, value in annotations.items():
                cmd.extend(['--annotation', f'{key}={value}'])

        try:
            # Only pass params if cwd is provided
            if cwd:
                run_cmd(
                    cmd,
                    params={'cwd': cwd},
                    exc_msg=f'Failed to push OCI artifact to {artifact_ref}',
                )
            else:
                run_cmd(cmd, exc_msg=f'Failed to push OCI artifact to {artifact_ref}')
            log.info('Successfully pushed OCI artifact to %s', sanitize_log_text(artifact_ref))
        except Exception as e:
            raise IIBError(sanitize_log_text(f'Failed to push OCI artifact to {artifact_ref}: {e}'))


def get_image_stream_digest(
    tag: str,
):
    """
    Retrieve the image digest from the OpenShift ImageStream.

    This function queries the `index-db-cache` ImageStream to get the
    SHA256 digest for a specific tag.

    :param tag: The image tag to check.
    :return: The image digest (e.g., "sha256:...").
    :rtype: str
    """
    # This JSONPath expression navigates the ImageStream JSON structure to extract the image digest:
    # - .status.tags: Access the 'tags' array within the 'status' object
    # - [?(@.tag=="{tag}")]: Filter to find the tag object where the 'tag' field equals
    #   the specified tag (@. refers to the current item in the array being filtered)
    # - .items[0]: From the matched tag object, access the first item in its 'items' array
    # - .image: Extract the 'image' field, which contains the SHA256 digest
    jsonpath = f'\'{{.status.tags[?(@.tag=="{tag}")].items[0].image}}\''
    return run_cmd(
        ['oc', 'get', 'imagestream', INDEX_DB_IMAGESTREAM_NAME, '-o', f'jsonpath={jsonpath}'],
        exc_msg=f'Failed to get digest for ImageStream tag {tag}.',
    )


def verify_indexdb_cache_sync(tag: str) -> bool:
    """
    Compare the digest of the ImageStream with the digest of the image in repository.

    This function verifies if the local ImageStream cache is up to date with
    the latest image in the remote registry.

    :param tag: The image tag to verify.
    :return: True if the digests match (cache is synced), False otherwise.
    :rtype: bool
    """
    conf = get_worker_config()
    artifact_pullspec = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'], tag=tag
    )

    oras_exclusive_auth_path = conf['iib_index_db_oras_auth_path']
    cmd_args = []
    if oras_exclusive_auth_path and os.path.exists(oras_exclusive_auth_path):
        cmd_args = ['--registry-config', oras_exclusive_auth_path]

    descriptor = run_cmd(
        ['oras', 'manifest', 'fetch', '--descriptor', *cmd_args, artifact_pullspec],
        exc_msg=f'Failed to fetch manifest descriptor for {artifact_pullspec}',
    )
    quay_digest = json.loads(descriptor)['digest']
    is_digest = get_image_stream_digest(tag)

    return quay_digest == is_digest


def verify_indexdb_cache_for_image(index_image_pullspec: str) -> bool:
    """
    Verify the synchronization state of the index database cache for a given container image.

    This function generates the digest-based artifact combined tag for the
    specified image pullspec and verifies whether the database cache for the
    image is synchronized.

    :param str index_image_pullspec: The pull specification string of the container image.
    :return: The result of the cache synchronization verification process.
    :rtype: str
    """
    return verify_indexdb_cache_sync(_get_content_addressed_artifact_tag(index_image_pullspec))


def refresh_indexdb_cache(
    tag: str,
    registry_auths: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Force a synchronization of the ImageStream with the remote registry.

    This function imports the specified image from Quay.io into the `index-db-cache`
    ImageStream, ensuring the local cache is up-to-date.

    :param tag: The container image tag to refresh.
    :param registry_auths: Optional authentication data for the registry.
    """
    log.info('Refreshing OCI artifact cache: %s', tag)

    conf = get_worker_config()
    artifact_pullspec = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_artifact_registry'], tag=tag
    )

    # Use namespace-specific registry authentication if provided
    with set_registry_auths(registry_auths, use_empty_config=True):
        run_cmd(
            [
                'oc',
                'import-image',
                f'{INDEX_DB_IMAGESTREAM_NAME}:{tag}',
                f'--from={artifact_pullspec}',
                '--confirm',
            ],
            exc_msg=f'Failed to refresh OCI artifact {tag}.',
        )


def refresh_indexdb_cache_for_image(index_image_pullspec: str) -> None:
    """
    Refresh the cached data for an index database, associating it with the given image pullspec.

    This function generates the digest-based artifact combined tag for the
    specified image pullspec, and refreshes the associated index database cache.

    :param str index_image_pullspec: The pull specification of the index image to cache.
    """
    refresh_indexdb_cache(_get_content_addressed_artifact_tag(index_image_pullspec))


def get_imagestream_artifact_pullspec(from_index: str) -> str:
    """
    Get the ImageStream pullspec for the index.db artifact.

    This function constructs the internal OpenShift ImageStream pullspec that can be used
    to pull the index.db artifact from the cached ImageStream instead of directly from Quay.

    :param str from_index: The from_index pullspec
    :return: ImageStream pullspec for the artifact
    :rtype: str
    """
    conf = get_worker_config()
    artifact_tag = _get_content_addressed_artifact_tag(from_index)

    # ImageStream pullspec format:
    # image-registry.openshift-image-registry.svc:5000/{namespace}/index-db:{artifact_tag}
    imagestream_pullspec = conf['iib_index_db_artifact_template'].format(
        registry=conf['iib_index_db_imagestream_registry'], tag=artifact_tag
    )
    return imagestream_pullspec
