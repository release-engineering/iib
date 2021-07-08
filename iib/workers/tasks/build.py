# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import os
import re
import sqlite3
import stat
import subprocess
import time
import tempfile
import textwrap

from operator_manifest.operator import ImageName

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
    chmod_recursively,
    deprecate_bundles,
    get_bundles_from_deprecation_list,
    get_resolved_bundles,
    get_resolved_image,
    podman_pull,
    request_logger,
    reset_docker_config,
    retry,
    run_cmd,
    set_registry_token,
    skopeo_inspect,
    RequestConfigAddRm,
    get_image_label,
    verify_labels,
    prepare_request_for_build,
)


__all__ = ['handle_add_request', 'handle_rm_request']


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

            if 'api.Registry.ListBundles' in output or 'api.Registry.ListPackages' in output:
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
    :return: list of unique present bundles as provided by the grpc query and a list of unique
        bundle pull_specs
    :rtype: list, list
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
        return [], []

    # Transform returned data to parsable json
    unique_present_bundles = []
    unique_present_bundles_pull_spec = []
    present_bundles = _get_bundle_json(bundles)

    for bundle in present_bundles:
        bundle_path = bundle['bundlePath']
        if bundle_path in unique_present_bundles_pull_spec:
            continue
        unique_present_bundles.append(bundle)
        unique_present_bundles_pull_spec.append(bundle_path)

    return unique_present_bundles, unique_present_bundles_pull_spec


def _get_bundle_json(bundles):
    return [json.loads(bundle) for bundle in re.split(r'(?<=})\n(?={)', bundles)]


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
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
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
        # This enables substitutes-for functionality for rebuilds. See
        # https://github.com/operator-framework/enhancements/blob/master/enhancements/substitutes-for.md
        '--enable-alpha',
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
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
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
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :raises IIBError: if the index image has changed since IIB build started.
    """
    with set_registry_token(overwrite_from_index_token, unresolved_from_index):
        resolved_post_build_from_index = get_resolved_image(unresolved_from_index)

    if resolved_post_build_from_index != resolved_prebuild_from_index:
        raise IIBError(
            'The supplied from_index image changed during the IIB request.'
            ' Please resubmit the request.'
        )


def _add_property_to_index(db_path, property):
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


def _requires_max_ocp_version(bundle):
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
    deprecation_list=None,
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
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param dict greenwave_config: the dict of config required to query Greenwave to gate bundles.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :param list deprecation_list: list of deprecated bundles for the target index image. Defaults
        to ``None``.
    :raises IIBError: if the index image build fails or legacy support is required and one of
        ``cnr_token`` or ``organization`` is not specified.
    """
    _cleanup()
    # Resolve bundles to their digests
    set_request_state(request_id, 'in_progress', 'Resolving the bundles')
    resolved_bundles = get_resolved_bundles(bundles)

    verify_labels(resolved_bundles)

    # Check if Gating passes for all the bundles
    if greenwave_config:
        gate_bundles(resolved_bundles, greenwave_config)

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigAddRm(
            _binary_image=binary_image,
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
    present_bundles_pull_spec = []
    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        if from_index:
            msg = 'Checking if bundles are already present in index image'
            log.info(msg)
            set_request_state(request_id, 'in_progress', msg)

            with set_registry_token(overwrite_from_index_token, from_index_resolved):
                present_bundles, present_bundles_pull_spec = _get_present_bundles(
                    from_index_resolved, temp_dir
                )

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

        # Add the max ocp version property
        # We need to ensure that any bundle which has deprecated/removed API(s) in 1.22/ocp 4.9
        # will have this property to prevent users from upgrading clusters to 4.9 before upgrading
        # the operator installed to a version that is compatible with 4.9

        # Get the CSV name and version (not just the bundle path)
        # If there is no from_index provided then this is the path to the
        # the index.db
        db_path = temp_dir + "/database/index.db"
        # Duplicating if statement in case opm_index_add has the side effect of moving the db
        if from_index:
            db_path = _get_index_database(from_index, temp_dir)
            
        port, rpc_proc = _serve_index_registry(db_path)

        raw_bundles = run_cmd(
            ['grpcurl', '-plaintext', f'localhost:{port}', 'api.Registry/ListBundles'],
            exc_msg='Failed to get bundle data from index image',
        )
        rpc_proc.kill()

        # Get bundle json for bundles in the request
        updated_bundles = list(
            filter(lambda b: b['bundlePath'] in resolved_bundles, _get_bundle_json(raw_bundles))
        )

        for bundle in updated_bundles:
            if _requires_max_ocp_version(bundle['bundlePath']):
                log.info('adding property for %s', bundle['bundlePath'])
                max_openshift_version_property = {
                    'type': 'olm.maxOpenShiftVersion',
                    'value': '4.8',
                    'operatorbundle_name': bundle['csvName'],
                    'operatorbundle_version': bundle['version'],
                    'operatorbundle_path': bundle['bundlePath'],
                }
                _add_property_to_index(db_path, max_openshift_version_property)
                log.info('property added for %s', bundle['bundlePath'])

        deprecation_bundles = get_bundles_from_deprecation_list(
            present_bundles_pull_spec + resolved_bundles, deprecation_list or []
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
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :raises IIBError: if the index image build fails.
    """
    _cleanup()
    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigAddRm(
            _binary_image=binary_image,
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


def _copy_files_from_image(image, src_path, dest_path):
    """
    Copy a file from the container image into the given destination path.

    The file may be a directory.

    :param str image: the pull specification of the container image.
    :param str src_path: the full path within the container image to copy from.
    :param str dest_path: the full path on the local host to copy into.
    """
    # Check that image is pullable
    podman_pull(image)

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
