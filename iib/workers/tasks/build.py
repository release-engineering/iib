# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import stat
import tempfile

from operator_manifest.operator import ImageName
from retry import retry

from iib.exceptions import IIBError, ExternalServiceError
from iib.workers.api_utils import set_request_state, update_request
from iib.workers.config import get_worker_config
from iib.workers.tasks.celery import app
from iib.workers.greenwave import gate_bundles
from iib.workers.tasks.fbc_utils import is_image_fbc
from iib.workers.tasks.opm_operations import (
    opm_serve_from_index,
    opm_registry_add_fbc,
    opm_migrate,
    opm_registry_rm_fbc,
    deprecate_bundles_fbc,
)
from iib.workers.tasks.utils import (
    add_max_ocp_version_property,
    chmod_recursively,
    deprecate_bundles,
    get_bundles_from_deprecation_list,
    get_bundle_json,
    get_resolved_bundles,
    get_resolved_image,
    podman_pull,
    request_logger,
    reset_docker_config,
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


@retry(
    exceptions=ExternalServiceError,
    tries=get_worker_config().iib_total_attempts,
    delay=get_worker_config().iib_retry_delay,
    jitter=get_worker_config().iib_retry_jitter,
    logger=log,
)
def _build_image(dockerfile_dir: str, dockerfile_name: str, request_id: int, arch: str) -> None:
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
    destination: str = _get_local_pull_spec(request_id, arch)
    log.info(
        'Building the container image with the %s dockerfile for arch %s and tagging it as %s',
        dockerfile_name,
        arch,
        destination,
    )
    dockerfile_path: str = os.path.join(dockerfile_dir, dockerfile_name)
    # NOTE: It's important to provide both --override-arch and --arch to ensure the metadata
    # on the image, **and** on its config blob are set correctly.
    #
    # NOTE: The argument "--format docker" ensures buildah will not generate an index image with
    # default OCI v1 manifest but always use Docker v2 format.
    run_cmd(
        [
            'buildah',
            'bud',
            '--no-cache',
            '--format',
            'docker',
            '--override-arch',
            arch,
            '--arch',
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


@retry(exceptions=IIBError, tries=get_worker_config().iib_total_attempts, logger=log)
def _create_and_push_manifest_list(request_id, arches, build_tags):
    """
    Create and push the manifest list to the configured registry.

    :param int request_id: the ID of the IIB build request
    :param iter arches: an iterable of arches to create the manifest list for
    :param build_tags: list of extra tag to use for intermediate index image
    :return: the pull specification of the manifest list
    :rtype: str
    :raises IIBError: if creating or pushing the manifest list fails
    """
    buildah_manifest_cmd = ['buildah', 'manifest']
    _tags = [request_id]
    if build_tags:
        _tags += build_tags
    conf = get_worker_config()
    output_pull_specs = []
    for tag in _tags:
        output_pull_spec = conf['iib_image_push_template'].format(
            registry=conf['iib_registry'], request_id=tag
        )
        output_pull_specs.append(output_pull_spec)
        try:
            run_cmd(
                buildah_manifest_cmd + ['rm', output_pull_spec],
                exc_msg=f'Failed to remove local manifest list. {output_pull_spec} does not exist',
            )
        except IIBError as e:
            error_msg = str(e)
            if 'Manifest list not found locally.' not in error_msg:
                raise IIBError(f'Error removing local manifest list: {error_msg}')
            log.debug(
                'Manifest list cannot be removed. No manifest list %s found', output_pull_spec
            )
        log.info('Creating the manifest list %s locally', output_pull_spec)
        run_cmd(
            buildah_manifest_cmd + ['create', output_pull_spec],
            exc_msg=f'Failed to create the manifest list locally: {output_pull_spec}',
        )
        for arch in sorted(arches):
            arch_pull_spec = _get_external_arch_pull_spec(request_id, arch, include_transport=True)
            run_cmd(
                buildah_manifest_cmd + ['add', output_pull_spec, arch_pull_spec],
                exc_msg=(
                    f'Failed to add {arch_pull_spec} to the'
                    f' local manifest list: {output_pull_spec}'
                ),
            )

        log.debug('Pushing manifest list %s', output_pull_spec)
        run_cmd(
            buildah_manifest_cmd
            + [
                'push',
                '--all',
                '--format',
                'v2s2',
                output_pull_spec,
                f'docker://{output_pull_spec}',
            ],
            exc_msg=f'Failed to push the manifest list to {output_pull_spec}',
        )

    # return 1st item as it holds production tag
    return output_pull_specs[0]


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
    port, rpc_proc = opm_serve_from_index(base_dir, from_index=from_index)

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
    present_bundles = get_bundle_json(bundles)

    for bundle in present_bundles:
        bundle_path = bundle['bundlePath']
        if bundle_path in unique_present_bundles_pull_spec:
            continue
        unique_present_bundles.append(bundle)
        unique_present_bundles_pull_spec.append(bundle_path)

    return unique_present_bundles, unique_present_bundles_pull_spec


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


@retry(exceptions=IIBError, tries=2, logger=log)
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


@retry(exceptions=IIBError, tries=2, logger=log)
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


@retry(exceptions=IIBError, tries=get_worker_config().iib_total_attempts, logger=log)
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


@retry(exceptions=IIBError, tries=get_worker_config().iib_total_attempts, logger=log)
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
    build_tags=None,
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
    :param str cnr_token: (deprecated) legacy support was disabled.
        the token required to push backported packages to the legacy app registry via OMPS.
    :param str organization: (deprecated) legacy support was disabled.
        organization name in the legacy app registry to which the backported packages
        should be pushed to.
    :param bool force_backport: (deprecated) legacy support was disabled.
        if True, always export packages to the legacy app registry via OMPS.
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
    :param list build_tags: List of tags which will be applied to intermediate index images.
    :raises IIBError: if the index image build fails.
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
    with set_registry_token(overwrite_from_index_token, from_index_resolved):
        is_fbc = is_image_fbc(from_index_resolved)
        if is_fbc:
            # logging requested by stakeholders do not delete
            log.info("Processing File-Based Catalog image")

    if (cnr_token and organization) or force_backport:
        log.warning(
            "Legacy support is deprecated in IIB. "
            "cnr_token, organization and force_backport parameters will be ignored."
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

        if is_fbc:
            opm_registry_add_fbc(
                base_dir=temp_dir,
                bundles=resolved_bundles,
                binary_image=prebuild_info['binary_image_resolved'],
                from_index=from_index_resolved,
                overwrite_csv=(prebuild_info['distribution_scope'] in ['dev', 'stage']),
            )
        else:
            _opm_index_add(
                base_dir=temp_dir,
                bundles=resolved_bundles,
                binary_image=prebuild_info['binary_image_resolved'],
                from_index=from_index_resolved,
                overwrite_from_index_token=overwrite_from_index_token,
                overwrite_csv=(prebuild_info['distribution_scope'] in ['dev', 'stage']),
            )

        # Add the max ocp version property
        # We need to ensure that any bundle which has deprecated/removed API(s) in 1.22/ocp 4.9
        # will have this property to prevent users from upgrading clusters to 4.9 before upgrading
        # the operator installed to a version that is compatible with 4.9
        if resolved_bundles:
            add_max_ocp_version_property(resolved_bundles, temp_dir)

        deprecation_bundles = get_bundles_from_deprecation_list(
            present_bundles_pull_spec + resolved_bundles, deprecation_list or []
        )

        arches = prebuild_info['arches']
        if deprecation_bundles:
            if is_fbc:
                deprecate_bundles_fbc(
                    bundles=deprecation_bundles,
                    base_dir=temp_dir,
                    binary_image=prebuild_info['binary_image'],
                    from_index=from_index_resolved,
                )
            else:
                # opm can only deprecate a bundle image on an existing index image. Build and
                # push a temporary index image to satisfy this requirement. Any arch will do.
                arch = sorted(arches)[0]
                log.info('Building a temporary index image to satisfy the deprecation requirement')
                _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
                _push_image(request_id, arch)
                intermediate_image_name = _get_external_arch_pull_spec(
                    request_id, arch, include_transport=False
                )
                deprecate_bundles(
                    bundles=deprecation_bundles,
                    base_dir=temp_dir,
                    binary_image=prebuild_info['binary_image'],
                    from_index=intermediate_image_name,
                    overwrite_target_index_token=overwrite_from_index_token,
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

        if is_fbc:
            index_db_file = os.path.join(temp_dir, get_worker_config()['temp_index_db_path'])
            # make sure FBC is generated right before build
            opm_migrate(index_db=index_db_file, base_dir=temp_dir)

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
    output_pull_spec = _create_and_push_manifest_list(request_id, arches, build_tags)

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
    build_tags=None,
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
    :param list build_tags: List of tags which will be applied to intermediate index images.
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
        with set_registry_token(overwrite_from_index_token, from_index_resolved):
            if is_image_fbc(from_index_resolved):
                log.info("Processing File-Based Catalog image")
                opm_registry_rm_fbc(
                    base_dir=temp_dir,
                    from_index=from_index_resolved,
                    operators=operators,
                    binary_image=prebuild_info['binary_image'],
                )

            else:
                _opm_index_rm(
                    base_dir=temp_dir,
                    operators=operators,
                    binary_image=prebuild_info['binary_image'],
                    from_index=from_index_resolved,
                    overwrite_from_index_token=overwrite_from_index_token,
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
    output_pull_spec = _create_and_push_manifest_list(request_id, arches, build_tags)

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
