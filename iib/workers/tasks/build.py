# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import os
import re
import stat
import subprocess
import time
import tempfile
import textwrap

from operator_manifest.operator import ImageName, OperatorManifest
import ruamel.yaml

from iib.exceptions import IIBError, AddressAlreadyInUse
from iib.workers.api_utils import set_request_state, update_request
from iib.workers.config import get_worker_config
from iib.workers.tasks.celery import app
from iib.workers.greenwave import gate_bundles
from iib.workers.tasks.legacy import (
    export_legacy_packages,
    get_legacy_support_packages,
    validate_legacy_params_and_config,
)
from iib.workers.tasks.utils import (

    get_all_index_images_info,
    chmod_recursively,
    deprecate_bundles,
    get_bundles_from_deprecation_list,
    get_image_labels,
    get_resolved_bundles,
    get_resolved_image,
    podman_pull,
    request_logger,
    RequestConfigAddRm,
    reset_docker_config,
    retry,
    run_cmd,
    set_registry_token,
    set_registry_auths,
    skopeo_inspect,
    gather_index_image_arches,
    _validate_distribution_scope,
    _get_image_arches,
    get_image_label,
    _get_container_image_name,
)


__all__ = ['handle_add_request', 'handle_regenerate_bundle_request', 'handle_rm_request']

yaml = ruamel.yaml.YAML()
# IMPORTANT: ruamel will introduce a line break if the yaml line is longer than yaml.width.
# Unfortunately, this causes issues for JSON values nested within a YAML file, e.g.
# metadata.annotations."alm-examples" in a CSV file.
# The default value is 80. Set it to a more forgiving higher number to avoid issues
yaml.width = 200
log = logging.getLogger(__name__)


def _build_image(dockerfile_dir, dockerfile_name, request_id, arch):
    """
    Build the index image for the specified architecture.

    :param str dockerfile_dir: the path to the directory containing the data used for
        building the container image
    :param str dockerfile_name: the name of the Dockerfile in the dockerfile_dir to
        be used when building the container image
    :param int request_id: the ID of the IIB build request
    :param str arch: the architecture to build this image for
    :raises IIBError: if the build fails
    """
    destination = _get_local_pull_spec(request_id, arch)
    log.info(
        'Building the container image with the %s dockerfile for arch %s and tagging it as %s',
        dockerfile_name,
        arch,
        destination,
    )
    dockerfile_path = os.path.join(dockerfile_dir, dockerfile_name)
    run_cmd(
        [
            'buildah',
            'bud',
            '--no-cache',
            '--override-arch',
            arch,
            '-t',
            destination,
            '-f',
            dockerfile_path,
        ],
        {'cwd': dockerfile_dir},
        exc_msg=f'Failed to build the container image on the arch {arch}',
    )


def _cleanup():
    """
    Remove all existing container images on the host.

    This will ensure that the host will not run out of disk space due to stale data, and that
    all images referenced using floating tags will be up to date on the host.

    Additionally, this function will reset the Docker ``config.json`` to
    ``iib_docker_config_template``.

    :raises IIBError: if the command to remove the container images fails
    """
    log.info('Removing all existing container images')
    run_cmd(
        ['podman', 'rmi', '--all', '--force'],
        exc_msg='Failed to remove the existing container images',
    )
    reset_docker_config()


@retry(attempts=3, wait_on=IIBError, logger=log)
def _create_and_push_manifest_list(request_id, arches):
    """
    Create and push the manifest list to the configured registry.

    :param int request_id: the ID of the IIB build request
    :param iter arches: an iterable of arches to create the manifest list for
    :return: the pull specification of the manifest list
    :rtype: str
    :raises IIBError: if creating or pushing the manifest list fails
    """
    output_pull_spec = get_rebuilt_image_pull_spec(request_id)
    log.info('Creating the manifest list %s', output_pull_spec)
    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        manifest_yaml = os.path.abspath(os.path.join(temp_dir, 'manifest.yaml'))
        with open(manifest_yaml, 'w+') as manifest_yaml_f:
            manifest_yaml_f.write(
                textwrap.dedent(
                    f'''\
                    image: {output_pull_spec}
                    manifests:
                    '''
                )
            )
            for arch in sorted(arches):
                arch_pull_spec = _get_external_arch_pull_spec(request_id, arch)
                log.debug(
                    'Adding the manifest %s to the manifest list %s',
                    arch_pull_spec,
                    output_pull_spec,
                )
                manifest_yaml_f.write(
                    textwrap.dedent(
                        f'''\
                        - image: {arch_pull_spec}
                          platform:
                            architecture: {arch}
                            os: linux
                        '''
                    )
                )
            # Return back to the beginning of the file to output it to the logs
            manifest_yaml_f.seek(0)
            log.debug(
                'Created the manifest configuration with the following content:\n%s',
                manifest_yaml_f.read(),
            )

        run_cmd(
            ['manifest-tool', 'push', 'from-spec', manifest_yaml],
            exc_msg=f'Failed to push the manifest list to {output_pull_spec}',
        )

    return output_pull_spec


def _update_index_image_pull_spec(
    output_pull_spec,
    request_id,
    arches,
    from_index=None,
    overwrite_from_index=False,
    overwrite_from_index_token=None,
    resolved_prebuild_from_index=None,
    add_or_rm=False,
):
    """
    Update the request with the modified index image.

    This function was created so that code didn't need to be duplicated for the ``add`` and ``rm``
    request types.

    :param str output_pull_spec: pull spec of the index image generated by IIB
    :param int request_id: the ID of the IIB build request
    :param set arches: the set of arches that were built as part of this request
    :param str from_index: the pull specification of the container image containing the index that
        the index image build was based from.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image.
    :param str resolved_prebuild_from_index: resolved index image before starting the build.
    :param bool add_or_rm: true if the request is an ``Add`` or ``Rm`` request. defaults to false
    :raises IIBError: if the manifest list couldn't be created and pushed
    """
    conf = get_worker_config()
    if from_index and overwrite_from_index:
        _overwrite_from_index(
            request_id,
            output_pull_spec,
            from_index,
            resolved_prebuild_from_index,
            overwrite_from_index_token,
        )
        index_image = from_index
    elif conf['iib_index_image_output_registry']:
        index_image = output_pull_spec.replace(
            conf['iib_registry'], conf['iib_index_image_output_registry'], 1
        )
        log.info(
            'Changed the index_image pull specification from %s to %s',
            output_pull_spec,
            index_image,
        )
    else:
        index_image = output_pull_spec

    payload = {'arches': list(arches), 'index_image': index_image}


    if add_or_rm:
        with set_registry_token(overwrite_from_index_token, index_image):
            index_image_resolved = get_resolved_image(index_image)
        payload['index_image_resolved'] = index_image_resolved

    update_request(request_id, payload, exc_msg='Failed setting the index image on the request')


def _get_external_arch_pull_spec(request_id, arch, include_transport=False):
    """
    Get the pull specification of the single arch image in the external registry.

    :param int request_id: the ID of the IIB build request
    :param str arch: the specific architecture of the container image
    :param bool include_transport: if true, `docker://` will be prefixed in the returned pull
        specification
    :return: the pull specification of the single arch image in the external registry
    :rtype: str
    """
    pull_spec = get_rebuilt_image_pull_spec(request_id) + f'-{arch}'
    if include_transport:
        return f'docker://{pull_spec}'
    return pull_spec


def _get_local_pull_spec(request_id, arch, include_transport=False):
    """
    Get the local pull specification of the architecture specfic index image for this request.

    :param int request_id: the ID of the IIB build request
    :param str arch: the specific architecture of the container image.
    :param bool include_transport: if true, `containers-storage:localhost/` will be prefixed
        in the returned pull specification
    :return: the pull specification of the index image for this request.
    :rtype: str
    """
    pull_spec = f'iib-build:{request_id}-{arch}'
    if include_transport:
        return f'containers-storage:localhost/{pull_spec}'
    return pull_spec


def get_rebuilt_image_pull_spec(request_id):
    """
    Generate the pull specification of the container image rebuilt by IIB.

    :param int request_id: the ID of the IIB build request
    :return: pull specification of the rebuilt container image
    :rtype: str
    """
    conf = get_worker_config()
    return conf['iib_image_push_template'].format(
        registry=conf['iib_registry'], request_id=request_id
    )


def _get_resolved_bundles(bundles):
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
        skopeo_raw = skopeo_inspect(f'docker://{bundle_pull_spec}', '--raw')
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


def _get_index_database(from_index, base_dir):
    """
    Get database file from the specified index image and save it locally.

    :param str from_index: index image to get database file from.
    :param str base_dir: base directory to which the database file should be saved.
    :return: path to the copied database file.
    :rtype: str
    :raises IIBError: if any podman command fails.
    """
    db_path = get_image_label(from_index, 'operators.operatorframework.io.index.database.v1')
    if not db_path:
        raise IIBError('Index image doesn\'t have the label specifying its database location.')
    _copy_files_from_image(from_index, db_path, base_dir)
    local_path = os.path.join(base_dir, os.path.basename(db_path))
    return local_path


def _serve_index_registry(db_path):
    """
    Locally start OPM registry service, which can be communicated with using gRPC queries.

    Due to IIB's paralellism, the service can run multiple times, which could lead to port
    binding conflicts. Resolution of port conflicts is handled in this function as well.

    :param str db_path: path to index database containing the registry data.
    :return: tuple containing port number of the running service and the running Popen object.
    :rtype: (int, Popen)
    :raises IIBError: if all tried ports are in use, or the command failed for another reason.
    """
    conf = get_worker_config()
    port_start = conf['iib_grpc_start_port']
    port_end = port_start + conf['iib_grpc_max_port_tries']

    for port in range(port_start, port_end):
        try:
            return (
                port,
                _serve_index_registry_at_port(
                    db_path, port, conf['iib_grpc_max_tries'], conf['iib_grpc_init_wait_time']
                ),
            )
        except AddressAlreadyInUse:
            log.info('Port %d is in use, trying another.', port)

    err_msg = f'No free port has been found after {conf.get("iib_grpc_max_port_tries")} attempts.'
    log.error(err_msg)
    raise IIBError(err_msg)


@retry(attempts=2, wait_on=IIBError, logger=log)
def _serve_index_registry_at_port(db_path, port, max_tries, wait_time):
    """
    Start an image registry service at a specified port.

    :param str db_path: path to index database containing the registry data.
    :param str int port: port to start the service on.
    :param max_tries: how many times to try to start the service before giving up.
    :param wait_time: time to wait before checking if the service is initialized.
    :return: object of the running Popen process.
    :rtype: Popen
    :raises IIBError: if the process has failed to initialize too many times, or an unexpected
        error occured.
    :raises AddressAlreadyInUse: if the specified port is already being used by another service.
    """
    cmd = ['opm', 'registry', 'serve', '-p', str(port), '-d', db_path, '-t', '/dev/null']
    for attempt in range(max_tries):
        rpc_proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(db_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        start_time = time.time()
        while time.time() - start_time < wait_time:
            time.sleep(1)
            ret = rpc_proc.poll()
            # process has terminated
            if ret is not None:
                stderr = rpc_proc.stderr.read()
                if 'address already in use' in stderr:
                    raise AddressAlreadyInUse(f'Port {port} is already used by a different service')
                raise IIBError(f'Command "{" ".join(cmd)}" has failed with error "{stderr}"')

            # query the service to see if it has started
            try:
                output = run_cmd(
                    ['grpcurl', '-plaintext', f'localhost:{port}', 'list', 'api.Registry']
                )
            except IIBError:
                output = ''

            if 'api.Registry.ListBundles' in output:
                log.debug('Started the command "%s"', ' '.join(cmd))
                log.info('Index registry service has been initialized.')
                return rpc_proc

        rpc_proc.kill()

    raise IIBError(f'Index registry has not been initialized after {max_tries} tries')


def _get_present_bundles(from_index, base_dir):
    """
    Get a list of bundles already present in the index image.

    :param str from_index: index image to inspect.
    :param str base_dir: base directory to create temporary files in.
    :return: list of present bundles as provided by the grpc query.
    :rtype: list
    :raises IIBError: if any of the commands fail.
    """
    db_path = _get_index_database(from_index, base_dir)
    port, rpc_proc = _serve_index_registry(db_path)

    bundles = run_cmd(
        ['grpcurl', '-plaintext', f'localhost:{port}', 'api.Registry/ListBundles'],
        exc_msg='Failed to get bundle data from index image',
    )
    rpc_proc.kill()

    # If no data is returned there are not bundles present
    if not bundles:
        return []

    # Transform returned data to parsable json
    present_bundles = [json.loads(bundle) for bundle in re.split(r'(?<=})\n(?={)', bundles)]
    return present_bundles


def _get_missing_bundles(present_bundles, bundles):
    """
    Filter out bundles to only those not present in the index image.

    :param list present_bundles: list of bundles present in the index image, as provided by opm.
    :param list bundles: resolved bundles requested to be added to the index image.
    :return: list of bundles not present in the index image.
    :rtype: list
    """
    present_bundle_hashes = []
    filtered_bundles = []
    for bundle in present_bundles:
        if '@sha256:' in bundle['bundlePath']:
            present_bundle_hashes.append(bundle['bundlePath'].split('@sha256:')[-1])

    for bundle in bundles:
        if bundle.split('@sha256:')[-1] not in present_bundle_hashes:
            filtered_bundles.append(bundle)

    return filtered_bundles


@retry(attempts=2, wait_on=IIBError, logger=log)
def _opm_index_add(
    base_dir,
    bundles,
    binary_image,
    from_index=None,
    overwrite_from_index_token=None,
    overwrite_csv=False,
    container_tool=None,
):
    """
    Add the input bundles to an operator index.

    This only produces the index.Dockerfile file and does not build the container image.

    :param str base_dir: the base directory to generate the database and index.Dockerfile in.
    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from. This should point to a digest or stable tag.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :param bool overwrite_csv: a boolean determining if a bundle will be replaced if the CSV
        already exists.
    :param str container_tool: the container tool to be used to operate on the index image
    :raises IIBError: if the ``opm index add`` command fails.
    """
    # The bundles are not resolved since these are stable tags, and references
    # to a bundle image using a digest fails when using the opm command.
    bundle_str = ','.join(bundles) or '""'
    cmd = [
        'opm',
        'index',
        'add',
        '--generate',
        '--bundles',
        bundle_str,
        '--binary-image',
        binary_image,
    ]
    if container_tool:
        cmd.append('--container-tool')
        cmd.append(container_tool)

    log.info('Generating the database file with the following bundle(s): %s', ', '.join(bundles))
    if from_index:
        log.info('Using the existing database from %s', from_index)
        # from_index is not resolved because podman does not support digest references
        # https://github.com/containers/libpod/issues/5234 is filed for it
        cmd.extend(['--from-index', from_index])

    if overwrite_csv:
        log.info('Using force to add bundle(s) to index')
        cmd.extend(['--overwrite-latest'])

    with set_registry_token(overwrite_from_index_token, from_index):
        run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to add the bundles to the index image')


@retry(attempts=2, wait_on=IIBError, logger=log)
def _opm_index_rm(base_dir, operators, binary_image, from_index, overwrite_from_index_token=None):
    """
    Remove the input operators from the operator index.

    This only produces the index.Dockerfile file and does not build the container image.

    :param str base_dir: the base directory to generate the database and index.Dockerfile in.
    :param list operators: a list of strings representing the names of the operators to
        remove from the index image.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :raises IIBError: if the ``opm index rm`` command fails.
    """
    cmd = [
        'opm',
        'index',
        'rm',
        '--generate',
        '--binary-image',
        binary_image,
        '--from-index',
        from_index,
        '--operators',
        ','.join(operators),
    ]

    log.info(
        'Generating the database file from an existing database %s and excluding'
        ' the following operator(s): %s',
        from_index,
        ', '.join(operators),
    )

    with set_registry_token(overwrite_from_index_token, from_index):
        run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to remove operators from the index image')


def _overwrite_from_index(
    request_id,
    output_pull_spec,
    from_index,
    resolved_prebuild_from_index,
    overwrite_from_index_token=None,
):
    """
    Overwrite the ``from_index`` image.

    :param int request_id: the ID of the request this index image is for.
    :param str output_pull_spec: the pull specification of the manifest list for the index image
        that IIB built.
    :param str from_index: the pull specification of the image to overwrite.
    :param str resolved_prebuild_from_index: resolved index image before starting the build.
    :param str overwrite_from_index_token: the user supplied token to use when overwriting the
        ``from_index`` image. If this is not set, IIB's configured credentials will be used.
    :raises IIBError: if one of the skopeo commands fails or if the index image has changed
        since IIB build started.
    """
    _verify_index_image(resolved_prebuild_from_index, from_index, overwrite_from_index_token)

    state_reason = f'Overwriting the index image {from_index} with {output_pull_spec}'
    log.info(state_reason)
    set_request_state(request_id, 'in_progress', state_reason)

    new_index_src = f'docker://{output_pull_spec}'
    temp_dir = None
    try:
        if overwrite_from_index_token:
            output_pull_spec_registry = ImageName.parse(output_pull_spec).registry
            from_index_registry = ImageName.parse(from_index).registry
            # If the registries are the same and `overwrite_from_index_token` was supplied, that
            # means that IIB's token will likely not have access to read the `from_index` image.
            # This means IIB must first export the manifest list and all the manifests locally and
            # then overwrite the `from_index` image with the exported version using the user
            # supplied token.
            #
            # When a newer version of buildah is available in RHEL 8, then that can be used instead
            # of the manifest-tool to create the manifest list locally which means this workaround
            # can be removed.
            if output_pull_spec_registry == from_index_registry:
                temp_dir = tempfile.TemporaryDirectory(prefix='iib-')
                new_index_src = f'oci:{temp_dir.name}'
                log.info(
                    'The registry used by IIB (%s) is also the registry where from_index (%s) will '
                    'be overwritten using the user supplied token. Will perform a workaround which '
                    'will cause the manifest digests to change but the content is the same.',
                    output_pull_spec_registry,
                    from_index_registry,
                )
                exc_msg = f'Failed to export {output_pull_spec} to the OCI format'
                _skopeo_copy(
                    f'docker://{output_pull_spec}', new_index_src, copy_all=True, exc_msg=exc_msg
                )

        exc_msg = f'Failed to overwrite the input from_index container image of {from_index}'
        with set_registry_token(overwrite_from_index_token, from_index):
            _skopeo_copy(new_index_src, f'docker://{from_index}', copy_all=True, exc_msg=exc_msg)
    finally:
        if temp_dir:
            temp_dir.cleanup()


def get_index_image_info(overwrite_from_index_token, from_index=None, default_ocp_version='v4.5'):
    """
    Get arches, resolved pull specification and ocp_version for the index image.

    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :param str from_index: the pull specification of the index image to be resolved.
    :param str default_ocp_version: default ocp_version to use if index image pull_spec is absent.
    :return: dictionary of resolved index image pull spec, set of arches, default ocp_version and
        resolved_distribution_scope
    :rtype: dict
    """
    result = {
        'resolved_from_index': None,
        'ocp_version': default_ocp_version,
        'arches': set(),
        'resolved_distribution_scope': 'prod',
    }
    if not from_index:
        return result

    with set_registry_token(overwrite_from_index_token, from_index):
        from_index_resolved = get_resolved_image(from_index)
        result['arches'] = _get_image_arches(from_index_resolved)
        result['ocp_version'] = (
            get_image_label(from_index_resolved, 'com.redhat.index.delivery.version') or 'v4.5'
        )
        result['resolved_distribution_scope'] = (
            get_image_label(from_index_resolved, 'com.redhat.index.delivery.distribution_scope')
            or 'prod'
        )
        result['resolved_from_index'] = from_index_resolved
    return result


def get_binary_image_from_config(ocp_version, distribution_scope, binary_image_config={}):
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


def _prepare_request_for_build(request_id, build_request_config):
    """
    Prepare the request for the index image build.

    All information that was retrieved and/or calculated for the next steps in the build are
    returned as a dictionary.

    This function was created so that code didn't need to be duplicated for the ``add`` and ``rm``
    request types.

    :param int request_id: the ID of the IIB build request
    :param RequestConfig build_request_config: build request configuration
    :rtype: dict
    :raises IIBError: if the container image resolution fails or the architectures couldn't be
        detected.
    :return: a dictionary with the keys: arches, binary_image_resolved, from_index_resolved, and
        ocp_version.
    """
    bundles = build_request_config.bundles
    if build_request_config.bundles is None:
        bundles = []

    set_request_state(request_id, 'in_progress', 'Resolving the container images')

    # Use v4.5 as default version
    index_info = get_all_index_images_info(
        build_request_config,
        [("from_index", "v4.5"), ("source_from_index", "v4.5"), ("target_index", "v4.6")],
    )
    arches = gather_index_image_arches(build_request_config, index_info)
    arches_str = ', '.join(sorted(arches))
    log.debug('Set to build the index image for the following arches: %s', arches_str)

    # Use the distribution_scope of the from_index as the resolved distribution scope for `Add`,
    # and 'Rm' requests.
    resolved_distribution_scope = index_info["from_index"]['resolved_distribution_scope']
    if build_request_config.source_from_index:
        resolved_distribution_scope = index_info['target_index']['resolved_distribution_scope']

    distribution_scope = _validate_distribution_scope(
        resolved_distribution_scope, build_request_config.distribution_scope
    )
    binary_image = build_request_config.get_binary_image(
        index_info['from_index'], distribution_scope
    )
    binary_image_resolved = get_resolved_image(binary_image)
    binary_image_arches = _get_image_arches(binary_image_resolved)

    if not arches.issubset(binary_image_arches):
        raise IIBError(
            'The binary image is not available for the following arches: {}'.format(
                ', '.join(sorted(arches - binary_image_arches))
            )
        )

    bundle_mapping = {}
    for bundle in bundles:
        operator = get_image_label(bundle, 'operators.operatorframework.io.bundle.package.v1')
        if operator:
            bundle_mapping.setdefault(operator, []).append(bundle)

    return {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': binary_image_resolved,
        'bundle_mapping': bundle_mapping,
        'from_index_resolved': index_info["from_index"]['resolved_from_index'],
        'ocp_version': index_info["from_index"]['ocp_version'],
        'distribution_scope': distribution_scope,
    }


def _update_index_image_build_state(request_id, prebuild_info):
    """
    Update the build request state with pre-determined build information.

    :param int request_id: the ID of the IIB build request
    :param dict prebuild_info: the information relevant to the build operation. The key ``arches``
        is required and must be set to the list of arches to build for. The key
        ``binary_image_resolved`` is required and must be set to the image digest pull spec of the
        binary image. The key ``bundle_mapping`` is optional. When provided, its value must be a
        dict mapping an operator to a list of bundle images. The key ``from_index_resolved`` is
        optional. When provided it must be set to the image digest pull spec of the from index
        image.
    """
    arches_str = ', '.join(sorted(prebuild_info['arches']))
    payload = {
        'binary_image': prebuild_info['binary_image'],
        'binary_image_resolved': prebuild_info['binary_image_resolved'],
        'state': 'in_progress',
        'distribution_scope': prebuild_info['distribution_scope'],
        'state_reason': f'Building the index image for the following arches: {arches_str}',
    }

    bundle_mapping = prebuild_info.get('bundle_mapping')
    if bundle_mapping:
        payload['bundle_mapping'] = bundle_mapping

    from_index_resolved = prebuild_info.get('from_index_resolved')
    if from_index_resolved:
        payload['from_index_resolved'] = from_index_resolved

    source_from_index_resolved = prebuild_info.get('source_from_index_resolved')
    if source_from_index_resolved:
        payload['source_from_index_resolved'] = source_from_index_resolved

    target_index_resolved = prebuild_info.get('target_index_resolved')
    if target_index_resolved:
        payload['target_index_resolved'] = target_index_resolved

    exc_msg = 'Failed setting the resolved images on the request'
    update_request(request_id, payload, exc_msg)


@retry(wait_on=IIBError, logger=log)
def _push_image(request_id, arch):
    """
    Push the single arch container image to the configured registry.

    :param int request_id: the ID of the IIB build request
    :param str arch: the architecture of the container image to push
    :raises IIBError: if the push fails
    """
    source = _get_local_pull_spec(request_id, arch)
    destination = _get_external_arch_pull_spec(request_id, arch, include_transport=True)
    log.info('Pushing the container image %s to %s', source, destination)
    run_cmd(
        ['podman', 'push', '-q', source, destination],
        exc_msg=f'Failed to push the container image to {destination} for the arch {arch}',
    )

    log.debug(f'Verifying that {destination} was pushed as a v2 manifest due to RHBZ#1810768')
    skopeo_raw = skopeo_inspect(destination, '--raw')
    if skopeo_raw['schemaVersion'] != 2:
        log.warning(
            'The manifest for %s ended up using schema version 1 due to RHBZ#1810768. Manually '
            'fixing it with skopeo.',
            destination,
        )
        exc_msg = f'Failed to fix the manifest schema version on {destination}'
        _skopeo_copy(destination, destination, exc_msg=exc_msg)


@retry(wait_on=IIBError, logger=log)
def _skopeo_copy(source, destination, copy_all=False, exc_msg=None):
    """
    Wrap the ``skopeo copy`` command.

    :param str source: the source to copy
    :param str destination: the destination to copy the source to
    :param bool copy_all: if True, it passes ``--all`` to the command
    :param str exc_msg: a custom exception message to provide
    :raises IIBError: if the copy fails
    """
    skopeo_timeout = get_worker_config()['iib_skopeo_timeout']
    log.debug('Copying the container image %s to %s', source, destination)
    cmd = ['skopeo', '--command-timeout', skopeo_timeout, 'copy', '--format', 'v2s2']
    if copy_all:
        cmd.append('--all')
    cmd.extend([source, destination])

    run_cmd(cmd, exc_msg=exc_msg or f'Failed to copy {source} to {destination}')


def _verify_index_image(
    resolved_prebuild_from_index, unresolved_from_index, overwrite_from_index_token=None
):
    """
    Verify if the index image has changed since the IIB build request started.

    :param str resolved_prebuild_from_index: resolved index image before starting the build
    :param str unresolved_from_index: unresolved index image provided as API input
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :raises IIBError: if the index image has changed since IIB build started.
    """
    with set_registry_token(overwrite_from_index_token, unresolved_from_index):
        resolved_post_build_from_index = get_resolved_image(unresolved_from_index)

    if resolved_post_build_from_index != resolved_prebuild_from_index:
        raise IIBError(
            'The supplied from_index image changed during the IIB request.'
            ' Please resubmit the request.'
        )


def _verify_labels(bundles):
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


@app.task
@request_logger
def handle_add_request(
    bundles,
    request_id,
    binary_image=None,
    from_index=None,
    add_arches=None,
    cnr_token=None,
    organization=None,
    force_backport=False,
    overwrite_from_index=False,
    overwrite_from_index_token=None,
    distribution_scope=None,
    greenwave_config=None,
    binary_image_config=None,
    deprecation_list=[],
):
    """
    Coordinate the the work needed to build the index image with the input bundles.

    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param list add_arches: the list of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param str cnr_token: the token required to push backported packages to the legacy
        app registry via OMPS.
    :param str organization: organization name in the legacy app registry to which the backported
        packages should be pushed to.
    :param bool force_backport: if True, always export packages to the legacy app registry via OMPS.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param dict greenwave_config: the dict of config required to query Greenwave to gate bundles.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :param list deprecation_list: list of deprecated bundles for the target index image. Defaults
        to an empty list.
    :raises IIBError: if the index image build fails or legacy support is required and one of
        ``cnr_token`` or ``organization`` is not specified.
    """
    _cleanup()
    # Resolve bundles to their digests
    set_request_state(request_id, 'in_progress', 'Resolving the bundles')
    resolved_bundles = get_resolved_bundles(bundles)

    _verify_labels(resolved_bundles)

    # Check if Gating passes for all the bundles
    if greenwave_config:
        gate_bundles(resolved_bundles, greenwave_config)

    prebuild_info = _prepare_request_for_build(
        request_id,
        RequestConfigAddRm(
            binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            bundles=bundles,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )
    from_index_resolved = prebuild_info['from_index_resolved']

    log.info('Checking if interacting with the legacy app registry is required')
    legacy_support_packages = get_legacy_support_packages(
        resolved_bundles, request_id, prebuild_info['ocp_version'], force_backport=force_backport
    )
    if legacy_support_packages:
        validate_legacy_params_and_config(
            legacy_support_packages, resolved_bundles, cnr_token, organization
        )

    _update_index_image_build_state(request_id, prebuild_info)
    present_bundles = []
    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        if from_index:
            msg = 'Checking if bundles are already present in index image'
            log.info(msg)
            set_request_state(request_id, 'in_progress', msg)

            with set_registry_token(overwrite_from_index_token, from_index_resolved):
                present_bundles = _get_present_bundles(from_index_resolved, temp_dir)

            filtered_bundles = _get_missing_bundles(present_bundles, resolved_bundles)
            excluded_bundles = [
                bundle for bundle in resolved_bundles if bundle not in filtered_bundles
            ]
            resolved_bundles = filtered_bundles

            if excluded_bundles:
                log.info(
                    'Following bundles are already present in the index image: %s',
                    ' '.join(excluded_bundles),
                )

        _opm_index_add(
            temp_dir,
            resolved_bundles,
            prebuild_info['binary_image_resolved'],
            from_index_resolved,
            overwrite_from_index_token,
            (prebuild_info['distribution_scope'] in ['dev', 'stage']),
        )

        _add_label_to_index(
            'com.redhat.index.delivery.version',
            prebuild_info['ocp_version'],
            temp_dir,
            'index.Dockerfile',
        )

        _add_label_to_index(
            'com.redhat.index.delivery.distribution_scope',
            prebuild_info['distribution_scope'],
            temp_dir,
            'index.Dockerfile',
        )

        present_bundles_pull_spec = [bundle['bundlePath'] for bundle in present_bundles]
        deprecation_bundles = get_bundles_from_deprecation_list(
            present_bundles_pull_spec + resolved_bundles, deprecation_list
        )

        arches = prebuild_info['arches']
        if deprecation_bundles:
            # opm can only deprecate a bundle image on an existing index image. Build and
            # push a temporary index image to satisfy this requirement. Any arch will do.
            arch = sorted(arches)[0]
            log.info('Building a temporary index image to satisfy the deprecation requirement')
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            intermediate_image_name = _get_local_pull_spec(request_id, arch, include_transport=True)
            deprecate_bundles(
                deprecation_bundles,
                temp_dir,
                prebuild_info['binary_image'],
                intermediate_image_name,
                overwrite_from_index_token,
                # Use podman so opm can find the image locally
                container_tool='podman',
            )

        for arch in sorted(arches):
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            _push_image(request_id, arch)

        # If the container-tool podman is used in the opm commands above, opm will create temporary
        # files and directories without the write permission. This will cause the context manager
        # to fail to delete these files. Adjust the file modes to avoid this error.
        chmod_recursively(
            temp_dir,
            dir_mode=(stat.S_IRWXU | stat.S_IRWXG),
            file_mode=(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP),
        )

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)
    if legacy_support_packages:
        export_legacy_packages(
            legacy_support_packages, request_id, output_pull_spec, cnr_token, organization
        )

    _update_index_image_pull_spec(
        output_pull_spec,
        request_id,
        arches,
        from_index,
        overwrite_from_index,
        overwrite_from_index_token,
        from_index_resolved,
        add_or_rm=True,
    )
    set_request_state(
        request_id, 'complete', 'The operator bundle(s) were successfully added to the index image'
    )


@app.task
@request_logger
def handle_rm_request(
    operators,
    request_id,
    from_index,
    binary_image=None,
    add_arches=None,
    overwrite_from_index=False,
    overwrite_from_index_token=None,
    distribution_scope=None,
    binary_image_config=None,
):
    """
    Coordinate the work needed to remove the input operators and rebuild the index image.

    :param list operators: a list of strings representing the name of the operators to
        remove from the index image.
    :param int request_id: the ID of the IIB build request
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param list add_arches: the list of arches to build in addition to the arches ``from_index`` is
        currently built for.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :raises IIBError: if the index image build fails.
    """
    _cleanup()
    prebuild_info = _prepare_request_for_build(
        request_id,
        RequestConfigAddRm(
            binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )
    _update_index_image_build_state(request_id, prebuild_info)

    from_index_resolved = prebuild_info['from_index_resolved']

    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        _opm_index_rm(
            temp_dir,
            operators,
            prebuild_info['binary_image'],
            from_index_resolved,
            overwrite_from_index_token,
        )

        _add_label_to_index(
            'com.redhat.index.delivery.version',
            prebuild_info['ocp_version'],
            temp_dir,
            'index.Dockerfile',
        )

        _add_label_to_index(
            'com.redhat.index.delivery.distribution_scope',
            prebuild_info['distribution_scope'],
            temp_dir,
            'index.Dockerfile',
        )

        arches = prebuild_info['arches']
        for arch in sorted(arches):
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            _push_image(request_id, arch)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

    _update_index_image_pull_spec(
        output_pull_spec,
        request_id,
        arches,
        from_index,
        overwrite_from_index,
        overwrite_from_index_token,
        from_index_resolved,
        add_or_rm=True,
    )
    set_request_state(
        request_id, 'complete', 'The operator(s) were successfully removed from the index image'
    )


@app.task
@request_logger
def handle_regenerate_bundle_request(
    from_bundle_image, organization, request_id, registry_auths=None
):
    """
    Coordinate the work needed to regenerate the operator bundle image.

    :param str from_bundle_image: the pull specification of the bundle image to be regenerated.
    :param str organization: the name of the organization the bundle should be regenerated for.
    :param int request_id: the ID of the IIB build request
    :param dict registry_auths: Provide the dockerconfig.json for authentication to private
      registries, defaults to ``None``.
    :raises IIBError: if the regenerate bundle image build fails.
    """
    _cleanup()

    set_request_state(request_id, 'in_progress', 'Resolving from_bundle_image')

    with set_registry_auths(registry_auths):
        from_bundle_image_resolved = get_resolved_image(from_bundle_image)

        arches = _get_image_arches(from_bundle_image_resolved)
        if not arches:
            raise IIBError(
                'No arches were found in the resolved from_bundle_image '
                f'{from_bundle_image_resolved}'
            )

        pinned_by_iib = yaml.load(
            get_image_label(from_bundle_image_resolved, 'com.redhat.iib.pinned') or 'false'
        )

        arches_str = ', '.join(sorted(arches))
        log.debug('Set to regenerate the bundle image for the following arches: %s', arches_str)

        payload = {
            'from_bundle_image_resolved': from_bundle_image_resolved,
            'state': 'in_progress',
            'state_reason': f'Regenerating the bundle image for the following arches: {arches_str}',
        }
        exc_msg = 'Failed setting the resolved "from_bundle_image" on the request'
        update_request(request_id, payload, exc_msg=exc_msg)

        # Pull the from_bundle_image to ensure steps later on don't fail due to registry timeouts
        podman_pull(from_bundle_image_resolved)

        with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
            manifests_path = os.path.join(temp_dir, 'manifests')
            _copy_files_from_image(from_bundle_image_resolved, '/manifests', manifests_path)
            metadata_path = os.path.join(temp_dir, 'metadata')
            _copy_files_from_image(from_bundle_image_resolved, '/metadata', metadata_path)
            new_labels = _adjust_operator_bundle(
                manifests_path, metadata_path, organization, pinned_by_iib
            )

            with open(os.path.join(temp_dir, 'Dockerfile'), 'w') as dockerfile:
                dockerfile.write(
                    textwrap.dedent(
                        f"""\
                            FROM {from_bundle_image_resolved}
                            COPY ./manifests /manifests
                            COPY ./metadata /metadata
                        """
                    )
                )
                for name, value in new_labels.items():
                    dockerfile.write(f'LABEL {name}={value}\n')

            for arch in sorted(arches):
                _build_image(temp_dir, 'Dockerfile', request_id, arch)
                _push_image(request_id, arch)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

    conf = get_worker_config()
    if conf['iib_index_image_output_registry']:
        old_output_pull_spec = output_pull_spec
        output_pull_spec = output_pull_spec.replace(
            conf['iib_registry'], conf['iib_index_image_output_registry'], 1
        )
        log.info(
            'Changed the bundle_image pull specification from %s to %s',
            old_output_pull_spec,
            output_pull_spec,
        )

    payload = {
        'arches': list(arches),
        'bundle_image': output_pull_spec,
        'state': 'complete',
        'state_reason': 'The request completed successfully',
    }
    update_request(request_id, payload, exc_msg='Failed setting the bundle image on the request')


def _copy_files_from_image(image, src_path, dest_path):
    """
    Copy a file from the container image into the given destination path.

    The file may be a directory.

    :param str image: the pull specification of the container image.
    :param str src_path: the full path within the container image to copy from.
    :param str dest_path: the full path on the local host to copy into.
    """
    # One way to copy a file from the image is to create a container from its filesystem
    # so the contents can be read. To create a container, podman always requires that a
    # command for the container is set. In this method, however, the command is not needed
    # because the container is never started, only created. Use a dummy command to satisfy
    # podman.
    container_command = 'unused'
    container_id = run_cmd(
        ['podman', 'create', image, container_command],
        exc_msg=f'Failed to create a container for {image}',
    ).strip()
    try:
        run_cmd(
            ['podman', 'cp', f'{container_id}:{src_path}', dest_path],
            exc_msg=f'Failed to copy the contents of {container_id}:{src_path} into {dest_path}',
        )
    finally:
        try:
            run_cmd(
                ['podman', 'rm', container_id],
                exc_msg=f'Failed to remove the container {container_id} for image {image}',
            )
        except IIBError as e:
            # Failure to remove the temporary container shouldn't cause the IIB request to fail.
            log.exception(e)


def _apply_package_name_suffix(metadata_path, organization=None):
    """
    Add the package name suffix if configured for this organization.

    This adds the suffix to the value of
    ``annotations['operators.operatorframework.io.bundle.package.v1']`` in
    ``metadata/annotations.yaml``.

    The final package name value is returned as part of the tuple.

    :param str metadata_path: the path to the bundle's metadata directory.
    :param str organization: the organization this customization is for.
    :raise IIBError: if the ``metadata/annotations.yaml`` file is in an unexpected format.
    :return: a tuple with the package name and a dictionary of labels to set on the bundle.
    :rtype: tuple(str, dict)
    """
    annotations_yaml_path = os.path.join(metadata_path, 'annotations.yaml')
    if not os.path.exists(annotations_yaml_path):
        raise IIBError('metadata/annotations.yaml does not exist in the bundle')

    with open(annotations_yaml_path, 'r') as f:
        try:
            annotations_yaml = yaml.load(f)
        except ruamel.yaml.YAMLError:
            error = 'metadata/annotations/yaml is not valid YAML'
            log.exception(error)
            raise IIBError(error)

    if not isinstance(annotations_yaml.get('annotations', {}), dict):
        raise IIBError('The value of metadata/annotations.yaml must be a dictionary')

    package_label = 'operators.operatorframework.io.bundle.package.v1'
    package_annotation = annotations_yaml.get('annotations', {}).get(package_label)
    if not package_annotation:
        raise IIBError(f'{package_label} is not set in metadata/annotations.yaml')

    if not isinstance(package_annotation, str):
        raise IIBError(f'The value of {package_label} in metadata/annotations.yaml is not a string')

    if not organization:
        log.debug('No organization was provided to add the package name suffix')
        return package_annotation, {}

    conf = get_worker_config()
    package_name_suffix = (
        conf['iib_organization_customizations'].get(organization, {}).get('package_name_suffix')
    )
    if not package_name_suffix:
        log.debug(
            'The "package_name_suffix" configuration is not set for the organization %s',
            organization,
        )
        return package_annotation, {}

    if package_annotation.endswith(package_name_suffix):
        log.debug('No modifications are needed on %s in metadata/annotations.yaml', package_label)
        return package_annotation, {}

    annotations_yaml['annotations'][package_label] = f'{package_annotation}{package_name_suffix}'

    with open(annotations_yaml_path, 'w') as f:
        yaml.dump(annotations_yaml, f)

    log.info(
        'Modified %s in metadata/annotations.yaml from %s to %s',
        package_label,
        package_annotation,
        annotations_yaml['annotations'][package_label],
    )

    return (
        annotations_yaml['annotations'][package_label],
        {package_label: annotations_yaml['annotations'][package_label]},
    )


def _adjust_operator_bundle(manifests_path, metadata_path, organization=None, pinned_by_iib=False):
    """
    Apply modifications to the operator manifests at the given location.

    For any container image pull spec found in the Operator CSV files, replace floating
    tags with pinned digests, e.g. `image:latest` becomes `image@sha256:...`.

    If spec.relatedImages is not set, it will be set with the pinned digests. If it is set but
    there are also RELATED_IMAGE_* environment variables set, an exception will be raised.

    This method relies on the OperatorManifest class to properly identify and apply the
    modifications as needed.

    :param str manifests_path: the full path to the directory containing the operator manifests.
    :param str metadata_path: the full path to the directory containing the bundle metadata files.
    :param str organization: the organization this bundle is for. If no organization is provided,
        no custom behavior will be applied.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    :raises IIBError: if the operator manifest has invalid entries
    :return: a dictionary of labels to set on the bundle
    :rtype: dict
    """
    package_name, labels = _apply_package_name_suffix(metadata_path, organization)

    try:
        operator_manifest = OperatorManifest.from_directory(manifests_path)
    except (ruamel.yaml.YAMLError, ruamel.yaml.constructor.DuplicateKeyError) as e:
        error = f'The Operator Manifest is not in a valid YAML format: {e}'
        log.exception(error)
        raise IIBError(error)

    found_pullspecs = set()
    operator_csvs = []
    for operator_csv in operator_manifest.files:
        if pinned_by_iib:
            # If the bundle image has already been previously pinned by IIB, the relatedImages
            # section will be populated and there may be related image environment variables.
            # However, we still want to process the image to apply any of the other possible
            # changes.
            log.info('Skipping pinning because related images have already been pinned by IIB')
        elif operator_csv.has_related_images():
            csv_file_name = os.path.basename(operator_csv.path)
            if operator_csv.has_related_image_envs():
                raise IIBError(
                    f'The ClusterServiceVersion file {csv_file_name} has entries in '
                    'spec.relatedImages and one or more containers have RELATED_IMAGE_* '
                    'environment variables set. This is not allowed for bundles regenerated with '
                    'IIB.'
                )
            log.debug(
                'Skipping pinning since the ClusterServiceVersion file %s has entries in '
                'spec.relatedImages',
                csv_file_name,
            )
            continue

        operator_csvs.append(operator_csv)

        for pullspec in operator_csv.get_pullspecs():
            found_pullspecs.add(pullspec)

    conf = get_worker_config()
    registry_replacements = (
        conf['iib_organization_customizations']
        .get(organization, {})
        .get('registry_replacements', {})
    )

    # Resolve pull specs to container image digests
    replacement_pullspecs = {}
    for pullspec in found_pullspecs:
        replacement_needed = False
        new_pullspec = ImageName.parse(pullspec.to_str())

        if not pinned_by_iib:
            # Resolve the image only if it has not already been processed by IIB. This
            # helps making sure the pullspec is valid
            resolved_image = ImageName.parse(get_resolved_image(pullspec.to_str()))

            # If the tag is in the format "<algorithm>:<checksum>", the image is already pinned.
            # Otherwise, always pin it to a digest.
            if ':' not in ImageName.parse(pullspec).tag:
                log.debug('%s will be pinned to %s', pullspec, resolved_image.to_str())
                new_pullspec = resolved_image
                replacement_needed = True
                labels['com.redhat.iib.pinned'] = 'true'

        # Apply registry modifications
        new_registry = registry_replacements.get(new_pullspec.registry)
        if new_registry:
            replacement_needed = True
            new_pullspec.registry = new_registry

        if replacement_needed:
            log.debug('%s will be replaced with %s', pullspec, new_pullspec.to_str())
            replacement_pullspecs[pullspec] = new_pullspec

    # Apply modifications to the operator bundle image metadata
    for operator_csv in operator_csvs:
        csv_file_name = os.path.basename(operator_csv.path)
        log.info('Replacing the pull specifications on %s', csv_file_name)
        operator_csv.replace_pullspecs_everywhere(replacement_pullspecs)

        log.info('Setting spec.relatedImages on %s', csv_file_name)
        operator_csv.set_related_images()

        operator_csv.dump()

    if organization:
        _adjust_csv_annotations(operator_manifest.files, package_name, organization)

    return labels


def _adjust_csv_annotations(operator_csvs, package_name, organization):
    """
    Annotate ClusterServiceVersion objects based on an organization configuration.

    :param list operator_csvs: the list of ``OperatorCSV`` objects to examine.
    :param str package_name: the operator package name.
    :param str organization: the organization this bundle is for. This determines what annotations
        to make.
    """
    conf = get_worker_config()
    org_csv_annotations = (
        conf['iib_organization_customizations'].get(organization, {}).get('csv_annotations')
    )
    if not org_csv_annotations:
        log.debug('The organization %s does not have CSV annotations configured', organization)
        return

    for operator_csv in operator_csvs:
        log.debug(
            'Processing the ClusterServiceVersion file %s', os.path.basename(operator_csv.path)
        )
        csv_annotations = operator_csv.data.setdefault('metadata', {}).setdefault('annotations', {})
        for annotation, value_template in org_csv_annotations.items():
            value = value_template.format(package_name=package_name)
            csv_annotations[annotation] = value

        operator_csv.dump()


def _add_label_to_index(label_key, label_value, temp_dir, dockerfile_name):
    """
    Add the OCP delivery label to the provided dockerfile.

    :param str label_key: the key for the label to add to the dockerfile.
    :param str label_value: the value that the index should contain for the key.
    :param str temp_dir: the temp directory to look for the dockerfile.
    :param str dockerfile_name: the dockerfile name.
    """
    with open(os.path.join(temp_dir, dockerfile_name), 'a') as dockerfile:
        label = f'LABEL {label_key}="{label_value}"'
        dockerfile.write(f'\n{label}\n')
        log.debug('Added the following line to %s: %s', dockerfile_name, label)


def _validate_distribution_scope(resolved_distribution_scope, distribution_scope):
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
