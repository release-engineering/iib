# SPDX-License-Identifier: GPL-3.0-or-later
import base64
import getpass
import socket
from typing import Any, Callable, Dict, Generator, List, Optional, Set, TYPE_CHECKING, Tuple, Union
from contextlib import contextmanager
import functools
import hashlib
import inspect
import json
import logging
import os
import re
import sqlite3
import subprocess

from pathlib import Path
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_chain,
)
from celery.app.log import TaskFormatter
from operator_manifest.operator import ImageName, OperatorManifest

from iib.common.common_utils import get_binary_versions
from iib.workers.dogpile_cache import (
    create_dogpile_region,
    dogpile_cache,
    skopeo_inspect_should_use_cache,
)

from iib.exceptions import IIBError, ExternalServiceError
from iib.workers.config import get_worker_config
from iib.workers.s3_utils import upload_file_to_s3_bucket
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.opm_operations import opm_registry_serve, opm_serve_from_index
from iib.workers.tasks.iib_static_types import (
    IndexImageInfo,
    AllIndexImagesInfo,
    PrebuildInfo,
    BundleImage,
    BundleMetadata,
)

# Add instrumentation
from iib.common.tracing import instrument_tracing

log = logging.getLogger(__name__)
dogpile_cache_region = create_dogpile_region()


def _add_property_to_index(db_path: str, property: Dict[str, str]) -> None:
    """
    Add a property to the index.

    :param str db_path: path to the index database
    :param dict property: a dict representing a property to be added to the index.db
    """
    insert = (
        'INSERT INTO properties '
        '(type, value, operatorbundle_name, operatorbundle_version, operatorbundle_path) '
        'VALUES (?, ?, ?, ?, ?);'
    )
    con = sqlite3.connect(db_path)
    # Insert property
    con.execute(
        insert,
        (
            property['type'],
            property['value'],
            property['operatorbundle_name'],
            property['operatorbundle_version'],
            property['operatorbundle_path'],
        ),
    )
    con.commit()
    con.close()


def add_max_ocp_version_property(resolved_bundles: List[str], temp_dir: str) -> None:
    """
    Add the max ocp version property to bundles.

    We need to ensure that any bundle which has deprecated/removed API(s) in 1.22/ocp 4.9
    will have this property to prevent users from upgrading clusters to 4.9 before upgrading
    the operator installed to a version that is compatible with 4.9

    :param list resolved_bundles: list of resolved bundles to which the max ocp version property
        will be added if missing
    :param str temp_dir: directory location of the index image
    """
    # Get the CSV name and version (not just the bundle path)
    temp_index_db_path = get_worker_config()['temp_index_db_path']
    db_path = os.path.join(temp_dir, temp_index_db_path)
    port, rpc_proc = opm_registry_serve(db_path=db_path)

    raw_bundles = run_cmd(
        ['grpcurl', '-plaintext', f'localhost:{port}', 'api.Registry/ListBundles'],
        exc_msg='Failed to get bundle data from index image',
    )
    terminate_process(rpc_proc)

    # This branch is hit when `bundles` attribute is empty and the index image is empty.
    # Ideally the code should not reach here if the bundles attribute is empty but adding
    # this here as a failsafe if it's called from some other place. Also, if the bundles
    # attribute is not empty, the index image cannot be empty here because we add the
    # bundle to the index before adding the maxOpenShiftVersion property
    if not raw_bundles:
        log.info('No bundles found in the index image')
        return

    # Filter index image bundles to get pull spec for bundles in the request
    updated_bundles: List[BundleImage] = list(
        filter(lambda b: b['bundlePath'] in resolved_bundles, get_bundle_json(raw_bundles))
    )

    for bundle in updated_bundles:
        if _requires_max_ocp_version(bundle['bundlePath']):
            log.info('adding property for %s', bundle['bundlePath'])
            max_openshift_version_property: Dict[str, str] = {
                'type': 'olm.maxOpenShiftVersion',
                'value': '4.8',
                #  MYPY  error: Dict entry 2 has incompatible type "str": "Optional[str]";
                #  expected "str": "str"
                'operatorbundle_name': bundle['csvName'],  # type: ignore
                'operatorbundle_version': bundle['version'],
                'operatorbundle_path': bundle['bundlePath'],
            }
            _add_property_to_index(db_path, max_openshift_version_property)
            log.info('property added for %s', bundle['bundlePath'])


def get_binary_image_from_config(
    ocp_version: str,
    distribution_scope: str,
    binary_image_config: Dict[str, Dict[str, str]] = {},
) -> str:
    """
    Determine the binary image to be used to build the index image.

    :param str ocp_version: the ocp_version label value of the index image.
    :param str distribution_scope: the distribution_scope label value of the index image.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :return: pull specification of the binary_image to be used for this build.
    :rtype: str
    :raises IIBError: when the config value for the ocp_version and distribution_scope is missing.
    """
    binary_image = binary_image_config.get(distribution_scope, {}).get(ocp_version, None)
    if not binary_image:
        raise IIBError(
            'IIB does not have a configured binary_image for'
            f' distribution_scope : {distribution_scope} and ocp_version: {ocp_version}.'
            ' Please specify a binary_image value in the request.'
        )

    return binary_image


def get_bundle_json(bundles: str) -> List[BundleImage]:
    """
    Get bundle json from grpcurl response.

    :param str bundles: response from grpcurl call to retrieve list of bundles in
        an index
    """
    return [json.loads(bundle) for bundle in re.split(r'(?<=})\n(?={)', bundles)]


def _requires_max_ocp_version(bundle: str) -> bool:
    """
    Check if the bundle requires the olm.maxOpenShiftVersion property.

    This property is required for bundles using deprecated APIs that don't already
    have the olm.maxOpenShiftVersion property set.

    :param str bundle: a string representing the bundle pull specification
    :returns bool:
    """
    cmd = [
        'operator-sdk',
        'bundle',
        'validate',
        bundle,
        '--select-optional',
        'name=community',
        '--output=json-alpha1',
        '--image-builder',
        'none',
    ]
    result = run_cmd(cmd, strict=False)
    if result:
        output = json.loads(result)
        # check if the bundle validation failed
        if not output['passed']:
            # check if the failure is due to the presence of deprecated APIs
            # and absence of the 'olm.maxOpenShiftVersion' property
            # Note: there is no other error in the sdk that mentions this field
            for msg in output['outputs']:
                if 'olm.maxOpenShiftVersion' in msg['message']:
                    return True
    return False


class RequestConfig:
    """Request config abstract class.

    :param str _binary_image:  the pull specification of the container image
                          where the opm binary gets copied from.
    :param str distribution_scope: the scope for distribution
        of the index image, defaults to ``None``.
    :param str source_from_index: the pull specification of the container image
        containing the index that will be used
        as a base of the merged index image.
    :param str target_index: the pull specification of the container image
        containing the index whose new data will be added
        to the merged index image.
    :param dict binary_image_config: the dict of config required to
        identify the appropriate ``binary_image`` to use.
    """

    # these attrs should not be printed out
    _secret_attrs: List[str] = [
        'cnr_token',
        'overwrite_from_index_token',
        'overwrite_target_index_token',
        'registry_auths',
    ]

    _attrs: List[str] = ["_binary_image", "distribution_scope", "binary_image_config"]
    __slots__ = _attrs
    if TYPE_CHECKING:
        _binary_image: str
        distribution_scope: str
        binary_image_config: Dict[str, Dict[str, str]]
        overwrite_from_index_token: str
        overwrite_target_index_token: str

    def __init__(self, **kwargs):
        """
        Request config __init__.

        Do not use this directly, use subclasses instead.
        :Keyword Arguments:
            See `_attrs` to check accepted keyword arguments.
        """
        for key in self.__slots__:
            setattr(self, key, None)
        for key, val in kwargs.items():
            setattr(self, key, kwargs[key])

    def __eq__(self, other: object) -> bool:
        if type(self) == type(other) and [getattr(self, x) for x in self.__slots__] == [
            getattr(self, x) for x in self.__slots__
        ]:
            return True
        return False

    def __repr__(self) -> str:
        # this is used to print() and log any instance of this class in dictionary format
        attrs = {x: getattr(self, x) for x in self.__slots__}
        for attr in self._secret_attrs:
            if attrs.get(attr) is not None:
                attrs[attr] = '*****'
        return str(attrs)

    def binary_image(self, index_info: IndexImageInfo, distribution_scope: str) -> str:
        """Get binary image based on self configuration, index image info and distribution scope."""
        if not self._binary_image:
            binary_image_ocp_version = index_info['ocp_version']
            return get_binary_image_from_config(
                binary_image_ocp_version, distribution_scope, self.binary_image_config
            )
        return self._binary_image


class RequestConfigAddRm(RequestConfig):
    """Request config for add and remove operations.

    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for
        non-privileged users to use ``overwrite_from_index``.
        The format of the token must be
        in the format "user:password".
    :param str from_index: the pull specification of the container image
        containing the index that the index image build
        will be based from.
    :param set add_arches: the set of arches to build in addition to the
        arches ``from_index`` is currently built for;
        if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param list bundles: the list of bundles to create the
        bundle mapping on the request
    """

    _attrs: List[str] = RequestConfig._attrs + [
        "overwrite_from_index_token",
        "from_index",
        "add_arches",
        "bundles",
        "operators",
    ]
    __slots__ = _attrs
    if TYPE_CHECKING:
        overwrite_from_index_token: str
        from_index: str
        add_arches: Set[str]
        bundles: List[str]
        operators: List[str]


class RequestConfigMerge(RequestConfig):
    """Request config for merge operation.

    :param str overwrite_target_index_token:  auth token used to pus index image
        when overwrite is set.
    :param str source_from_index: the pull specification of the container image
        containing the index that will be used
        as a base of the merged index image.
    :param str target_index: the pull specification of the container image
        containing the index whose new data will be added
        to the merged index image.
    """

    _attrs: List[str] = RequestConfig._attrs + [
        "source_from_index",
        "target_index",
        "overwrite_target_index_token",
    ]

    __slots__ = _attrs
    if TYPE_CHECKING:
        source_from_index: str
        target_index: str
        overwrite_target_index_token: str


class RequestConfigCreateIndexImage(RequestConfig):
    """Request config for add and remove operations.

    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for
        non-privileged users to use ``overwrite_from_index``.
        The format of the token must be
        in the format "user:password".
    :param str from_index: the pull specification of the container image
        containing the index that the index image build
        will be based from.
    :param dict labels: the dictionary of labels to build in addition to the
        labels ``from_index`` is currently built for;
    """

    _attrs: List[str] = RequestConfig._attrs + ["from_index", "labels"]
    __slots__ = _attrs
    if TYPE_CHECKING:
        from_index: str
        labels: Dict[str, str]


class RequestConfigFBCOperation(RequestConfig):
    """
    Request config for FBC operation.

    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for
        non-privileged users to use ``overwrite_from_index``.
        The format of the token must be
        in the format "user:password".
    :param str from_index: the pull specification of the container image
        containing the index that the index image build
        will be based from.
    :param set add_arches: the set of arches to build in addition to the
        arches ``from_index`` is currently built for;
        if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param str frb_fragment: the fbc_fragment to add in index image
    """

    _attrs: List[str] = RequestConfig._attrs + [
        "overwrite_from_index_token",
        "from_index",
        "add_arches",
        "fbc_fragment",
    ]
    __slots__ = _attrs
    if TYPE_CHECKING:
        overwrite_from_index_token: str
        from_index: str
        add_arches: Set[str]
        fbc_fragment: str


def get_bundles_from_deprecation_list(bundles: List[str], deprecation_list: List[str]) -> List[str]:
    """
    Get a list of to-be-deprecated bundles based on the data from the deprecation list.

    :param list bundles: list of bundles pull spec to apply the filter on.
    :param list deprecation_list: list of deprecated bundle pull specifications.
    :return: bundles which are to be deprecated.
    :rtype: list
    """
    resolved_deprecation_list = get_resolved_bundles(deprecation_list)
    deprecate_bundles = []
    for bundle in bundles:
        if bundle in resolved_deprecation_list:
            deprecate_bundles.append(bundle)

    if deprecation_list and not deprecate_bundles:
        log.warning('Deprecation list was set but no bundles were found for deprecation.')

    if deprecate_bundles:
        log.info(
            'Bundles that will be deprecated from the index image: %s', ', '.join(deprecate_bundles)
        )
    return deprecate_bundles


def get_resolved_bundles(bundles: List[str]) -> List[str]:
    """
    Get the pull specification of the bundle images using their digests.

    Determine if the pull spec refers to a manifest list.
    If so, simply use the digest of the first item in the manifest list.
    If not a manifest list, it must be a v2s2 image manifest and should be used as it is.

    :param list bundles: the list of bundle images to be resolved.
    :return: the list of bundle images resolved to their digests.
    :rtype: list
    :raises IIBError: if unable to resolve a bundle image.
    """
    log.info('Resolving bundles %s', ', '.join(bundles))
    resolved_bundles = set()
    for bundle_pull_spec in bundles:
        skopeo_raw = skopeo_inspect(
            f'docker://{bundle_pull_spec}', '--raw', require_media_type=True
        )
        if (
            skopeo_raw.get('mediaType')
            == 'application/vnd.docker.distribution.manifest.list.v2+json'
        ):
            # Get the digest of the first item in the manifest list
            digest = skopeo_raw['manifests'][0]['digest']
            name = _get_container_image_name(bundle_pull_spec)
            resolved_bundles.add(f'{name}@{digest}')
        elif (
            skopeo_raw.get('mediaType') == 'application/vnd.docker.distribution.manifest.v2+json'
            and skopeo_raw.get('schemaVersion') == 2
        ):
            resolved_bundles.add(get_resolved_image(bundle_pull_spec))
        else:
            error_msg = (
                f'The pull specification of {bundle_pull_spec} is neither '
                f'a v2 manifest list nor a v2s2 manifest. Type {skopeo_raw.get("mediaType")}'
                f' and schema version {skopeo_raw.get("schemaVersion")} is not supported by IIB.'
            )
            raise IIBError(error_msg)

    return list(resolved_bundles)


def _get_container_image_name(pull_spec: str) -> str:
    """
    Get the container image name from a pull specification.

    :param str pull_spec: the pull spec to analyze
    :return: the container image name
    """
    if '@' in pull_spec:
        return pull_spec.split('@', 1)[0]
    else:
        return pull_spec.rsplit(':', 1)[0]


def get_resolved_image(pull_spec: str) -> str:
    """
    Get the pull specification of the container image using its digest.

    :param str pull_spec: the pull specification of the container image to resolve
    :return: the resolved pull specification
    :rtype: str
    """
    log.debug('Resolving %s', pull_spec)
    name = _get_container_image_name(pull_spec)
    skopeo_output = skopeo_inspect(f'docker://{pull_spec}', '--raw', return_json=False)
    if json.loads(skopeo_output).get('schemaVersion') == 2:
        raw_digest = hashlib.sha256(skopeo_output.encode('utf-8')).hexdigest()
        digest = f'sha256:{raw_digest}'
    else:
        # Schema 1 is not a stable format. The contents of the manifest may change slightly
        # between requests causing a different digest to be computed. Instead, let's leverage
        # skopeo's own logic for determining the digest in this case. In the future, we
        # may want to use skopeo in all cases, but this will have significant performance
        # issues until https://github.com/containers/skopeo/issues/785
        digest = skopeo_inspect(f'docker://{pull_spec}')['Digest']
    pull_spec_resolved = f'{name}@{digest}'
    log.debug('%s resolved to %s', pull_spec, pull_spec_resolved)
    return pull_spec_resolved


def get_image_labels(pull_spec: str) -> Dict[str, str]:
    """
    Get the labels from the image.

    :param list<str> labels: the labels to get
    :return: the dictionary of the labels on the image
    :rtype: dict
    """
    if pull_spec.startswith('docker://') or pull_spec.startswith('containers-storage'):
        full_pull_spec = pull_spec
    else:
        full_pull_spec = f'docker://{pull_spec}'
    log.debug('Getting the labels from %s', full_pull_spec)
    return skopeo_inspect(full_pull_spec, '--config').get('config', {}).get('Labels', {})


def reset_docker_config() -> None:
    """Create a symlink from ``iib_docker_config_template`` to ``~/.docker/config.json``."""
    conf = get_worker_config()
    docker_config_path = os.path.join(os.path.expanduser('~'), '.docker', 'config.json')

    try:
        log.debug('Removing the Docker config at %s', docker_config_path)
        os.remove(docker_config_path)
    except FileNotFoundError:
        pass

    if os.path.exists(conf.iib_docker_config_template):
        log.debug(
            'Creating a symlink from %s to %s', conf.iib_docker_config_template, docker_config_path
        )
        os.symlink(conf.iib_docker_config_template, docker_config_path)


@contextmanager
def set_registry_token(
    token: Optional[str], container_image: Optional[str], append: bool = False
) -> Generator:
    """
    Configure authentication to the registry that ``container_image`` is from.

    This context manager will reset the authentication to the way it was after it exits. If
    ``token`` is falsy, this context manager will do nothing.

    :param str token: the token in the format of ``username:password``
    :param str container_image: the pull specification of the container image to parse to determine
        the registry this token is for.
    :param bool append: When enabled new token is appended to current configuration.
        Old token for the same registry is overwritten.
    :return: None
    :rtype: None
    """
    if not token:
        log.debug(
            'Not changing the Docker configuration since no overwrite_from_index_token was provided'
        )
        yield

        return

    if not container_image:
        log.debug('Not changing the Docker configuration since no from_index was provided')
        yield

        return

    registry = ImageName.parse(container_image).registry
    encoded_token = base64.b64encode(token.encode('utf-8')).decode('utf-8')
    registry_auths: Dict[str, Any] = {'auths': {}}
    if append:
        docker_config_path = os.path.join(os.path.expanduser('~'), '.docker', 'config.json')
        if os.path.exists(docker_config_path):
            with open(docker_config_path, 'r') as f:
                try:
                    registry_auths = json.load(f)
                except json.JSONDecodeError as e:
                    log.error("Invalid JSON file %s; %s", docker_config_path, e.msg)
                    raise e

                log.debug('Docker config will be updated')

    log.debug('Setting the override token for the registry %s ', registry)
    registry_auths['auths'].update({registry: {'auth': encoded_token}})

    with set_registry_auths(registry_auths):
        yield


@contextmanager
def set_registry_auths(registry_auths: Optional[Dict[str, Any]]) -> Generator:
    """
    Configure authentication to the registry with provided dockerconfig.json.

    This context manager will reset the authentication to the way it was after it exits. If
    ``registry_auths`` is falsy, this context manager will do nothing.
    :param dict registry_auths: dockerconfig.json auth only information to private registries

    :return: None
    :rtype: None
    """
    if not registry_auths:
        log.debug('Not changing the Docker configuration since no registry_auths were provided')
        yield

        return

    docker_config_path = os.path.join(os.path.expanduser('~'), '.docker', 'config.json')
    try:
        log.debug('Removing the Docker config symlink at %s', docker_config_path)
        try:
            os.remove(docker_config_path)
        except FileNotFoundError:
            log.debug('The Docker config symlink at %s does not exist', docker_config_path)

        conf = get_worker_config()
        if os.path.exists(conf.iib_docker_config_template):
            with open(conf.iib_docker_config_template, 'r') as f:
                docker_config = json.load(f)
        else:
            docker_config = {}

        registries = list(registry_auths.get('auths', {}).keys())
        log.debug(
            'Setting the override token for the registries %s in the Docker config', registries
        )

        docker_config.setdefault('auths', {})
        docker_config['auths'].update(registry_auths.get('auths', {}))
        with open(docker_config_path, 'w') as f:
            json.dump(docker_config, f)

        yield
    finally:
        reset_docker_config()


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(get_worker_config().iib_total_attempts),
    wait=wait_chain(wait_exponential(multiplier=get_worker_config().iib_retry_multiplier)),
)
@dogpile_cache(
    dogpile_region=dogpile_cache_region, should_use_cache_fn=skopeo_inspect_should_use_cache
)
def skopeo_inspect(
    *args,
    return_json: bool = True,
    require_media_type: bool = False,
) -> Union[Dict[str, Any], str]:
    """
    Wrap the ``skopeo inspect`` command.

    :param args: any arguments to pass to ``skopeo inspect``
    :param bool return_json: if ``True``, the output will be parsed as JSON and returned
    :param bool require_media_type: if ``True``, ``mediaType`` will be checked in the output
        and it will be ignored when ``return_json`` is ``False``
    :return: a dictionary of the JSON output from the skopeo inspect command
    :rtype: dict
    :raises IIBError: if the command fails and if ``mediaType`` is not found in the output while
        ``require_media_type`` is ``True``
    """
    exc_msg = None
    for arg in args:
        if arg.startswith('docker://'):
            exc_msg = f'Failed to inspect {arg}. Make sure it exists and is accessible to IIB.'
            break

    skopeo_timeout = get_worker_config().iib_skopeo_timeout
    cmd = ['skopeo', '--command-timeout', skopeo_timeout, 'inspect'] + list(args)
    output = run_cmd(cmd, exc_msg=exc_msg)
    if not return_json:
        return output

    json_output = json.loads(output)

    if require_media_type and not json_output.get('mediaType'):
        raise IIBError('mediaType not found')
    return json_output


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(get_worker_config().iib_total_attempts),
)
def podman_pull(*args) -> None:
    """
    Wrap the ``podman pull`` command.

    :param args: any arguments to pass to ``podman pull``
    :raises IIBError: if the command fails
    """
    run_cmd(
        ['podman', 'pull'] + list(args),
        exc_msg=f'Failed to pull the container image {" ".join(args)}',
    )


def _regex_reverse_search(
    regex: str,
    proc_response: subprocess.CompletedProcess,
) -> Optional[re.Match]:
    """
    Try to match the STDERR content with a regular expression from bottom to up.

    This is a complementary function for ``run_cmd``.

    :param str regex: The regular expression to try to match
    :param subprocess.CompletedProcess proc_response: the popen response to retrieve the STDERR from
    :return: the regex match or None if not matched
    :rtype: re.Match
    """
    # Start from the last log message since the failure occurs near the bottom
    for msg in reversed(proc_response.stderr.splitlines()):
        match = re.match(regex, msg)
        if match:
            return match
    return None


def run_cmd(
    cmd: List[str],
    params: Optional[Dict[str, Any]] = None,
    exc_msg: Optional[str] = None,
    strict: bool = True,
) -> str:
    """
    Run the given command with the provided parameters.

    :param list cmd: list of strings representing the command to be executed
    :param dict params: keyword parameters for command execution
    :param str exc_msg: an optional exception message when the command fails
    :param bool strict: when true function will throw exception when problem occurs
    :return: the command output
    :rtype: str
    :raises IIBError: if the command fails
    """
    exc_msg = exc_msg or 'An unexpected error occurred'
    if not params:
        params = {}
    params.setdefault('universal_newlines', True)
    params.setdefault('encoding', 'utf-8')
    params.setdefault('stderr', subprocess.PIPE)
    params.setdefault('stdout', subprocess.PIPE)

    log.debug('Running the command "%s"', ' '.join(cmd))
    response: subprocess.CompletedProcess = subprocess.run(cmd, **params)

    if strict and response.returncode != 0:
        if set(['buildah', 'manifest', 'rm']) <= set(cmd) and 'image not known' in response.stderr:
            raise IIBError('Manifest list not found locally.')
        log.error('The command "%s" failed with: %s', ' '.join(cmd), response.stderr)
        regex: str
        match: Optional[re.Match]
        if Path(cmd[0]).stem.startswith('opm'):
            # Capture the error message right before the help display
            regex = r'^(?:Error: )(.+)$'
            match = _regex_reverse_search(regex, response)
            if match:
                raise IIBError(f'{exc_msg.rstrip(".")}: {match.groups()[0]}')
        elif cmd[0] == 'buildah':
            # Check for HTTP 50X errors on buildah
            network_regexes = [
                r'.*([e,E]rror:? creating build container).*(:?(50[0-9]|125)\s.*$)',
                r'.*(read\/write on closed pipe.*$)',
            ]
            for regex in network_regexes:
                match = _regex_reverse_search(regex, response)
                if match:
                    raise ExternalServiceError(f'{exc_msg}: {": ".join(match.groups()).strip()}')

        raise IIBError(exc_msg)

    return response.stdout


def terminate_process(proc: subprocess.Popen, timeout: int = 5) -> None:
    """
    Terminate given process. Fallback to SIGKILL when process is not terminated in given timeout.

    :param subprocess.Popen proc: process to be terminated
    :param int timeout: number of seconds to wait for terminating process
    """
    log.debug('Terminating process %s; pid: %d', proc, proc.pid)
    proc.terminate()
    try:
        # not using proc.wait() because it might cause deadlock when using pipes
        # https://docs.python.org/3/library/subprocess.html#subprocess.Popen.wait
        proc.communicate(timeout=timeout)
        log.info('Process terminated.')
    except subprocess.TimeoutExpired:
        log.warning('Process not terminated in time (%ss). Sending SIGKILL.', timeout)
        proc.kill()


def request_logger(func: Callable) -> Callable:
    """
    Log messages relevant to the current request to a dedicated file.

    If ``iib_request_logs_dir`` is set, a temporary log handler is added before the decorated
    function is invoked. It's then removed once the decorated function completes execution.

    If ``iib_request_logs_dir`` is not set, the temporary log handler will not be added.

    :param function func: the function to be decorated. The function must take the ``request_id``
        parameter.
    :return: the decorated function
    :rtype: function
    """
    worker_config = get_worker_config()
    log_dir = worker_config.iib_request_logs_dir
    log_level = worker_config.iib_request_logs_level
    log_format = worker_config.iib_request_logs_format

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> None:
        request_log_handler = None
        if log_dir:
            request_id = _get_function_arg_value('request_id', func, args, kwargs)
            if not request_id:
                raise IIBError(f'Unable to get "request_id" from {func.__name__}')
            # for better filtering of all logs for one build in SPLUNK
            log_formatter = TaskFormatter(
                log_format.format(request_id=f'request-{request_id}'), use_color=False
            )
            log_file_path = os.path.join(log_dir, f'{request_id}.log')
            request_log_handler = logging.FileHandler(log_file_path)
            request_log_handler.setLevel(log_level)
            request_log_handler.setFormatter(log_formatter)
            os.chmod(log_file_path, 0o664)  # nosec
            logger = logging.getLogger()
            logger.addHandler(request_log_handler)
            worker_info = f'Host: {socket.getfqdn()}; User: {getpass.getuser()}'
            logger.info(worker_info)
            versions = get_binary_versions()
            logger.info(f"opm {versions['opm']}\n{versions['podman']}\n{versions['buildah']}")
        try:
            return func(*args, **kwargs)
        finally:
            if request_log_handler:
                logger.removeHandler(request_log_handler)
                request_log_handler.flush()
                if worker_config['iib_aws_s3_bucket_name']:
                    upload_file_to_s3_bucket(log_file_path, 'request_logs', f'{request_id}.log')

    return wrapper


def _get_function_arg_value(
    arg_name: str,
    func: Callable,
    args: tuple,
    kwargs: Dict[Any, Any],
) -> Any:
    """Return the value of the given argument name."""
    original_func = func
    while getattr(original_func, '__wrapped__', None):
        # Type is ignored as mypy solution is not supported yet
        #  MYPY  error: "Callable[..., Any]" has no attribute "__wrapped__"
        original_func = original_func.__wrapped__  # type: ignore
    argspec = inspect.getfullargspec(original_func).args

    arg_index = argspec.index(arg_name)
    arg_value = kwargs.get(arg_name, None)
    if arg_value is None and len(args) > arg_index:
        arg_value = args[arg_index]
    return arg_value


def chmod_recursively(dir_path: str, dir_mode: int, file_mode: int) -> None:
    """Change file mode bits recursively.

    :param str dir_path: the path to the starting directory to apply the file mode bits
    :param int dir_mode: the mode, as defined in the stat module, to apply to directories
    :param int file_mode: the mode, as defined in the stat module, to apply to files
    """
    for dirpath, dirnames, filenames in os.walk(dir_path):
        os.chmod(dirpath, dir_mode)
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            # As per the man pages:
            #   On Linux, the permissions of an ordinary symbolic link are not used in any
            #   operations; the permissions are always 0777, and can't be changed.
            #   - https://www.man7.org/linux/man-pages/man7/symlink.7.html
            #
            # The python docs state that islink will only return True if the symlink points
            # to an existing file.
            #   - https://docs.python.org/3/library/os.path.html#os.path.islink
            # To completely ignore attempting to set permissions on a symlink, first verify the
            # file exists.
            if not os.path.exists(file_path) or os.path.islink(file_path):
                continue
            os.chmod(file_path, file_mode)


def gather_index_image_arches(
    build_request_config: RequestConfig,
    index_image_infos: AllIndexImagesInfo,
) -> Set[str]:
    """Gather architectures from build_request_config and provided index image.

    :param RequestConfig build_request_config: build request configuration
    :param dict index_image_infos: dict with index image infos returned
        by `get_all_index_images_info`
    :return: set of architecture of all index images
    :rtype: set
    """
    arches = set(
        (
            build_request_config.add_arches
            if (
                hasattr(build_request_config, 'add_arches')
                and isinstance(build_request_config, RequestConfigAddRm)
            )
            else []
        )
        or []
    )

    for info in index_image_infos.values():
        #  MYPY error: Value of type "object" is not indexable
        arches |= set(info['arches'])  # type: ignore

    if not arches:
        raise IIBError('No arches were provided to build the index image')
    return arches


def get_image_arches(pull_spec: str) -> Set[str]:
    """
    Get the architectures this image was built for.

    :param str pull_spec: the pull specification to a v2 manifest list
    :return: a set of architectures of the container images contained in the manifest list
    :rtype: set
    :raises IIBError: if the pull specification is not a v2 manifest list
    """
    log.debug('Get the available arches for %s', pull_spec)
    skopeo_raw = skopeo_inspect(f'docker://{pull_spec}', '--raw')
    arches = set()
    if skopeo_raw.get('mediaType') == 'application/vnd.docker.distribution.manifest.list.v2+json':
        for manifest in skopeo_raw['manifests']:
            arches.add(manifest['platform']['architecture'])
    elif skopeo_raw.get('mediaType') == 'application/vnd.docker.distribution.manifest.v2+json':
        skopeo_out = skopeo_inspect(f'docker://{pull_spec}', '--config')
        arches.add(skopeo_out['architecture'])
    else:
        raise IIBError(
            f'The pull specification of {pull_spec} is neither a v2 manifest list nor a v2 manifest'
        )

    return arches


def get_index_image_info(
    overwrite_from_index_token: Optional[str],
    from_index: Optional[str] = None,
    default_ocp_version: str = 'v4.5',
) -> IndexImageInfo:
    """Get arches, resolved pull specification and ocp_version for the index image.

    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :param str from_index: the pull specification of the index image to be resolved.
    :param str default_ocp_version: default ocp_version to use if index image pull_spec is absent.
    :return: dictionary of resolved index image pull spec, set of arches, default ocp_version and
        resolved_distribution_scope
    :rtype: dict
    """
    result: IndexImageInfo = {
        'resolved_from_index': None,
        'ocp_version': default_ocp_version,
        'arches': set(),
        'resolved_distribution_scope': 'prod',
    }
    if not from_index:
        return result

    with set_registry_token(overwrite_from_index_token, from_index):
        from_index_resolved = get_resolved_image(from_index)
        result['arches'] = get_image_arches(from_index_resolved)
        result['ocp_version'] = (
            get_image_label(from_index_resolved, 'com.redhat.index.delivery.version') or 'v4.5'
        )
        result['resolved_distribution_scope'] = (
            get_image_label(from_index_resolved, 'com.redhat.index.delivery.distribution_scope')
            or 'prod'
        )
        result['resolved_from_index'] = from_index_resolved
    return result


def get_all_index_images_info(
    build_request_config: RequestConfig,
    index_version_map: List[Tuple[str, Any]],
) -> AllIndexImagesInfo:
    """Get image info of all images in version map.

    :param RequestConfig build_request_config: build request configuration
    :param list index_version_map: list of tuples with (index_name, index_ocp_version)
    :return: dictionary with index image information obtained from `get_index_image_info`
    :rtype: dict
    """
    #  MYPY error: Missing keys ("from_index", "source_from_index", "target_index")
    #  for TypedDict "AllIndexImagesInfo"
    infos: AllIndexImagesInfo = {}  # type: ignore
    for index, version in index_version_map:
        log.debug(f'Get index image info {index} for version {version}')
        if not hasattr(build_request_config, index):
            from_index = None
        else:
            from_index = getattr(build_request_config, index)

        # Cannot be None, as get_index_image_info do not accept None
        token: Optional[str] = None
        if hasattr(build_request_config, 'overwrite_from_index_token') and index == 'from_index':
            token = build_request_config.overwrite_from_index_token
        elif (
            hasattr(build_request_config, 'overwrite_target_index_token')
            and index == 'target_index'
        ):
            token = build_request_config.overwrite_target_index_token

        #  MYPY error: TypedDict key must be a string literal;
        #  expected one of ("from_index", "source_from_index", "target_index")
        infos[index] = get_index_image_info(  # type: ignore
            overwrite_from_index_token=token, from_index=from_index, default_ocp_version=version
        )
    return infos


def get_image_label(pull_spec: str, label: str) -> str:
    """
    Get a specific label from the container image.

    :param str pull_spec: pull spec of the image
    :param str label: the label to get
    :return: the label on the container image or None
    :rtype: str
    """
    log.debug('Getting the label of %s from %s', label, pull_spec)
    return get_image_labels(pull_spec).get(label, '')


def verify_labels(bundles: List[str]) -> None:
    """
    Verify that the required labels are set on the input bundles.

    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :raises IIBError: if one of the bundles does not have the correct label value.
    """
    conf = get_worker_config()
    if not conf['iib_required_labels']:
        return

    for bundle in bundles:
        labels = get_image_labels(bundle)
        for label, value in conf['iib_required_labels'].items():
            if labels.get(label) != value:
                raise IIBError(f'The bundle {bundle} does not have the label {label}={value}')


def _validate_distribution_scope(resolved_distribution_scope: str, distribution_scope: str) -> str:
    """
    Validate distribution scope is allowed to be updated.

    :param str resolved_distribution_scope: the distribution_scope that the index is for.
    :param str distribution_scope: the distribution scope that has been requested for
        the index image.
    :return: the valid distribution scope
    :rtype: str
    :raises IIBError: if the ``resolved_distribution_scope`` is of lesser scope than
        ``distribution_scope``
    """
    if not distribution_scope:
        return resolved_distribution_scope

    scopes = ["dev", "stage", "prod"]
    # Make sure the request isn't regressing the distribution scope
    if scopes.index(distribution_scope) > scopes.index(resolved_distribution_scope):
        raise IIBError(
            f'Cannot set "distribution_scope" to {distribution_scope} because from index is'
            f' already set to {resolved_distribution_scope}'
        )
    return distribution_scope


@instrument_tracing(span_name='iib.workers.tasks.utils.prepare_request_for_build')
def prepare_request_for_build(
    request_id: int,
    build_request_config: RequestConfig,
) -> PrebuildInfo:
    """Prepare the request for the index image build.

    All information that was retrieved and/or calculated for the next steps in the build are
    returned as a dictionary.
    This function was created so that code didn't need to be duplicated for the ``add`` and ``rm``
    request types.
    :param int request_id: id of the request
    :param RequestConfig build_request_config: build request configuration
    :return: a dictionary with the keys: arches, binary_image_resolved, from_index_resolved, and
    ocp_version.
    :rtype: dict
    :raises IIBError: if the container image resolution fails or the architectures couldn't be
    detected.
    """
    log.info(f'Prepare request for build with parameters {build_request_config}')
    bundles = (
        build_request_config.bundles
        if (
            isinstance(build_request_config, RequestConfigAddRm)
            and hasattr(build_request_config, "bundles")
        )
        else None
    )

    if bundles is None:
        bundles = []

    set_request_state(request_id, 'in_progress', 'Resolving the container images')

    # Use v4.5 as default version
    index_info = get_all_index_images_info(
        build_request_config,
        [("from_index", "v4.5"), ("source_from_index", "v4.5"), ("target_index", "v4.6")],
    )
    arches = gather_index_image_arches(build_request_config, index_info)
    if not arches:
        raise IIBError('No arches were provided to build the index image')

    arches_str = ', '.join(sorted(arches))
    log.debug('Set to build the index image for the following arches: %s', arches_str)

    # Use the distribution_scope of the from_index as the resolved distribution scope for `Add`,
    # and 'Rm' requests, but use the distribution_scope of the target_index as the resolved
    # distribution scope for `merge-index-image` requests.
    request_index = index_info['from_index']
    if (
        hasattr(build_request_config, "source_from_index")
        and isinstance(build_request_config, RequestConfigMerge)
        and build_request_config.source_from_index
    ):
        request_index = index_info['target_index']

    resolved_distribution_scope = request_index['resolved_distribution_scope']

    distribution_scope = _validate_distribution_scope(
        resolved_distribution_scope, build_request_config.distribution_scope
    )

    binary_image = build_request_config.binary_image(request_index, distribution_scope)

    binary_image_resolved = get_resolved_image(binary_image)
    binary_image_arches = get_image_arches(binary_image_resolved)

    if not arches.issubset(binary_image_arches):
        raise IIBError(
            'The binary image is not available for the following arches: {}'.format(
                ', '.join(sorted(arches - binary_image_arches))
            )
        )

    bundle_mapping: Dict[str, Any] = {}
    for bundle in bundles:
        operator = get_image_label(bundle, 'operators.operatorframework.io.bundle.package.v1')
        if operator:
            bundle_mapping.setdefault(operator, []).append(bundle)
    source_from_index_resolved = index_info['source_from_index']['resolved_from_index']

    # MYPY error: Incompatible types (expression has type "Optional[str]",
    # - TypedDict item "from_index_resolved" has type "str")
    # - TypedDict item "source_from_index_resolved" has type "str")
    # - TypedDict item "target_index_resolved" has type "str")
    return {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': binary_image_resolved,
        'bundle_mapping': bundle_mapping,
        'distribution_scope': distribution_scope,
        'from_index_resolved': index_info["from_index"]['resolved_from_index'],  # type: ignore
        'ocp_version': index_info["from_index"]['ocp_version'],
        'source_from_index_resolved': source_from_index_resolved,  # type: ignore
        'source_ocp_version': index_info['source_from_index']['ocp_version'],
        'target_index_resolved': index_info['target_index']['resolved_from_index'],  # type: ignore
        'target_ocp_version': index_info['target_index']['ocp_version'],
    }


def grpcurl_get_db_data(from_index: str, base_dir: str, endpoint: str) -> str:
    """Get a str  with operators already present in the index image.

    :param str from_index: index image to inspect.
    :param str base_dir: base directory to create temporary files in.
    :return: str result of the grpc query
    :rtype: str
    :raises IIBError: if any of the commands fail.
    """
    port, rpc_proc = opm_serve_from_index(base_dir, from_index=from_index)

    if endpoint not in ["api.Registry/ListPackages", "api.Registry/ListBundles"]:
        raise IIBError(f"The endpoint '{endpoint}' is not allowed to be used")

    result = run_cmd(
        ['grpcurl', '-plaintext', f'localhost:{port}', endpoint],
        exc_msg=f'Failed to get {endpoint} data from index image',
    )
    terminate_process(rpc_proc)
    return result


def get_bundle_metadata(
    operator_manifest: OperatorManifest,
    pinned_by_iib: bool,
) -> BundleMetadata:
    """
    Get bundle metadata i.e. CSV's and all relatedImages pull specifications.

    If the bundle is already pinned by IIB, it will be pinned again and the relatedImages will
    be regenerated.

    :param operator_manifest.operator.OperatorManifest operator_manifest: the operator manifest
        object.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    :raises IIBError: if the operator manifest has invalid entries
    :return: a dictionary of CSV's and relatedImages pull specifications
    :rtype: dict
    """
    bundle_metadata: BundleMetadata = {'found_pullspecs': set(), 'operator_csvs': []}
    for operator_csv in operator_manifest.files:
        if pinned_by_iib:
            # If the bundle image has already been previously pinned by IIB, the relatedImages
            # section will be populated and there may be related image environment variables.
            # This behavior is now valid and the images will be pinned again and the relatedImages
            # will be regenerated.
            log.info(
                'Bundle has been pinned by IIB. '
                'Pinning will be done again and relatedImages will be regenerated'
            )

        bundle_metadata['operator_csvs'].append(operator_csv)

        for pullspec in operator_csv.get_pullspecs():
            bundle_metadata['found_pullspecs'].add(pullspec)
    return bundle_metadata
