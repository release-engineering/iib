# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import tempfile
import textwrap

from operator_manifest.operator import ImageName, OperatorManifest

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state, update_request
from iib.workers.config import get_worker_config
from iib.workers.tasks.celery import app
from iib.workers.greenwave import gate_bundles
from iib.workers.tasks.legacy import (
    export_legacy_packages,
    get_legacy_support_packages,
    validate_legacy_params_and_config,
)
from iib.workers.tasks.utils import get_image_labels, podman_pull, retry, run_cmd, skopeo_inspect


__all__ = ['handle_add_request', 'handle_regenerate_bundle_request', 'handle_rm_request']

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

    :raises IIBError: if the command to remove the container images fails
    """
    log.info('Removing all existing container images')
    run_cmd(
        ['podman', 'rmi', '--all', '--force'],
        exc_msg='Failed to remove the existing container images',
    )


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


def _finish_request_post_build(
    output_pull_spec,
    request_id,
    arches,
    from_index=None,
    overwrite_from_index=False,
    overwrite_from_index_token=None,
):
    """
    Finish the request after the manifest list has been pushed.

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
    :raises IIBError: if the manifest list couldn't be created and pushed
    """
    conf = get_worker_config()
    if from_index and overwrite_from_index:
        log.info(f'Ovewriting the index image {from_index} with {output_pull_spec}')
        index_image = from_index
        exc_msg = f'Failed to overwrite the input from_index container image of {index_image}'
        args = [f'docker://{output_pull_spec}', f'docker://{index_image}']
        _skopeo_copy(*args, copy_all=True, dest_token=overwrite_from_index_token, exc_msg=exc_msg)
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

    payload = {
        'arches': list(arches),
        'index_image': index_image,
        'state': 'complete',
        'state_reason': 'The request completed successfully',
    }
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


def _get_local_pull_spec(request_id, arch):
    """
    Get the local pull specification of the architecture specfic index image for this request.

    :param int request_id: the ID of the IIB build request
    :param str arch: the specific architecture of the container image.
    :return: the pull specification of the index image for this request.
    :rtype: str
    """
    return f'iib-build:{request_id}-{arch}'


def _get_image_arches(pull_spec):
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
        skopeo_out = skopeo_inspect(f'docker://{pull_spec}')
        arches.add(skopeo_out['Architecture'])
    else:
        raise IIBError(
            f'The pull specification of {pull_spec} is neither a v2 manifest list nor a v2 manifest'
        )

    return arches


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
            if '@' in bundle_pull_spec:
                repo = bundle_pull_spec.split('@', 1)[0]
            else:
                repo = bundle_pull_spec.rsplit(':', 1)[0]
            resolved_bundles.add(f'{repo}@{digest}')
        elif (
            skopeo_raw.get('mediaType') == 'application/vnd.docker.distribution.manifest.v2+json'
            and skopeo_raw.get('schemaVersion') == 2
        ):
            resolved_bundles.add(_get_resolved_image(bundle_pull_spec))
        else:
            error_msg = (
                f'The pull specification of {bundle_pull_spec} is neither '
                f'a v2 manifest list nor a v2s2 manifest. Type {skopeo_raw.get("mediaType")}'
                f' and schema version {skopeo_raw.get("schemaVersion")} is not supported by IIB.'
            )
            raise IIBError(error_msg)

    return list(resolved_bundles)


def _get_resolved_image(pull_spec):
    """
    Get the pull specification of the container image using its digest.

    :param str pull_spec: the pull specification of the container image to resolve
    :return: the resolved pull specification
    :rtype: str
    """
    log.debug('Resolving %s', pull_spec)
    skopeo_output = skopeo_inspect(f'docker://{pull_spec}')
    pull_spec_resolved = f'{skopeo_output["Name"]}@{skopeo_output["Digest"]}'
    log.debug('%s resolved to %s', pull_spec, pull_spec_resolved)
    return pull_spec_resolved


@retry(attempts=2, wait_on=IIBError, logger=log)
def _opm_index_add(base_dir, bundles, binary_image, from_index=None):
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
    :raises IIBError: if the ``opm index add`` command fails.
    """
    # The bundles are not resolved since these are stable tags, and references
    # to a bundle image using a digest fails when using the opm command.
    cmd = [
        'opm',
        'index',
        'add',
        '--generate',
        '--bundles',
        ','.join(bundles),
        '--binary-image',
        binary_image,
    ]

    log.info('Generating the database file with the following bundle(s): %s', ', '.join(bundles))
    if from_index:
        log.info('Using the existing database from %s', from_index)
        # from_index is not resolved because podman does not support digest references
        # https://github.com/containers/libpod/issues/5234 is filed for it
        cmd.extend(['--from-index', from_index])

    run_cmd(
        cmd, {'cwd': base_dir}, exc_msg='Failed to add the bundles to the index image',
    )


@retry(attempts=2, wait_on=IIBError, logger=log)
def _opm_index_rm(base_dir, operators, binary_image, from_index):
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

    run_cmd(
        cmd, {'cwd': base_dir}, exc_msg='Failed to remove operators from the index image',
    )


def _prepare_request_for_build(
    binary_image, request_id, from_index=None, add_arches=None, bundles=None
):
    """
    Prepare the request for the index image build.

    All information that was retrieved and/or calculated for the next steps in the build are
    returned as a dictionary.

    This function was created so that code didn't need to be duplicated for the ``add`` and ``rm``
    request types.

    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param int request_id: the ID of the IIB build request
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param list add_arches: the list of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param list bundles: the list of bundles to create the bundle mapping on the request
    :return: a dictionary with the keys: arches, binary_image_resolved, and from_index_resolved.
    :raises IIBError: if the container image resolution fails or the architectures couldn't be
        detected.
    """
    if bundles is None:
        bundles = []

    set_request_state(request_id, 'in_progress', 'Resolving the container images')

    if add_arches:
        arches = set(add_arches)
    else:
        arches = set()

    binary_image_resolved = _get_resolved_image(binary_image)
    binary_image_arches = _get_image_arches(binary_image_resolved)

    if from_index:
        from_index_resolved = _get_resolved_image(from_index)
        from_index_arches = _get_image_arches(from_index_resolved)
        arches = arches | from_index_arches
    else:
        from_index_resolved = None

    if not arches:
        raise IIBError('No arches were provided to build the index image')

    arches_str = ', '.join(sorted(arches))
    log.debug('Set to build the index image for the following arches: %s', arches_str)

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

    payload = {
        'binary_image_resolved': binary_image_resolved,
        'state': 'in_progress',
        'state_reason': f'Building the index image for the following arches: {arches_str}',
    }
    if bundle_mapping:
        payload['bundle_mapping'] = bundle_mapping
    if from_index_resolved:
        payload['from_index_resolved'] = from_index_resolved
    exc_msg = 'Failed setting the resolved images on the request'
    update_request(request_id, payload, exc_msg)

    return {
        'arches': arches,
        'binary_image_resolved': binary_image_resolved,
        'from_index_resolved': from_index_resolved,
    }


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
def _skopeo_copy(source, destination, copy_all=False, dest_token=None, exc_msg=None):
    """
    Wrap the ``skopeo copy`` command.

    :param str source: the source to copy
    :param str destination: the destination to copy the source to
    :param bool copy_all: if True, it passes ``--all`` to the command
    :param str dest_token: the token to pass to the ``--dest-token` parameter of the command.
        If not provided, ``--dest-token`` parameter is also not provided.
    :param str exc_msg: a custom exception message to provide
    :raises IIBError: if the copy fails
    """
    skopeo_timeout = get_worker_config()['iib_skopeo_timeout']
    log.debug('Copying the container image %s to %s', source, destination)
    cmd = [
        'skopeo',
        '--command-timeout',
        skopeo_timeout,
        'copy',
        '--format',
        'v2s2',
    ]
    if copy_all:
        cmd.append('--all')
    if dest_token:
        log.debug('Using user-provided token to copy the container image')
        cmd.append('--dest-creds')
        cmd.append(dest_token)

    cmd.extend([source, destination])
    cmd_repr = ['*****' if part == dest_token else part for part in cmd]

    run_cmd(cmd, exc_msg=exc_msg or f'Failed to copy {source} to {destination}', cmd_repr=cmd_repr)


def _verify_index_image(resolved_prebuild_from_index, unresolved_from_index):
    """
    Verify if the index image has changed since the IIB build request started.

    :param str resolved_prebuild_from_index: resolved index image before starting the build
    :param str unresolved_from_index: unresolved index image provided as API input
    :raises IIBError: if the index image has changed since IIB build started.
    """
    resolved_post_build_from_index = _get_resolved_image(unresolved_from_index)
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


def get_image_label(pull_spec, label):
    """
    Get a specific label from the container image.

    :param str label: the label to get
    :return: the label on the container image or None
    :rtype: str
    """
    log.debug('Getting the label of %s from %s', label, pull_spec)
    return get_image_labels(pull_spec).get(label)


@app.task
def handle_add_request(
    bundles,
    binary_image,
    request_id,
    from_index=None,
    add_arches=None,
    cnr_token=None,
    organization=None,
    overwrite_from_index=False,
    overwrite_from_index_token=None,
    greenwave_config=None,
):
    """
    Coordinate the the work needed to build the index image with the input bundles.

    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param int request_id: the ID of the IIB build request
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param list add_arches: the list of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param str cnr_token: the token required to push backported packages to the legacy
        app registry via OMPS.
    :param str organization: organization name in the legacy app registry to which the backported
        packages should be pushed to.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param dict greenwave_config: the dict of config required to query Greenwave to gate bundles.
    :raises IIBError: if the index image build fails or legacy support is required and one of
        ``cnr_token`` or ``organization`` is not specified.
    """
    # Resolve bundles to their digests
    resolved_bundles = _get_resolved_bundles(bundles)

    _verify_labels(resolved_bundles)

    # Check if Gating passes for all the bundles
    if greenwave_config:
        gate_bundles(resolved_bundles, greenwave_config)

    log.info('Checking if interacting with the legacy app registry is required')
    legacy_support_packages = get_legacy_support_packages(resolved_bundles)
    if legacy_support_packages:
        validate_legacy_params_and_config(
            legacy_support_packages, resolved_bundles, cnr_token, organization
        )

    _cleanup()
    prebuild_info = _prepare_request_for_build(
        binary_image, request_id, from_index, add_arches, bundles
    )

    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        _opm_index_add(
            temp_dir, resolved_bundles, prebuild_info['binary_image_resolved'], from_index,
        )

        arches = prebuild_info['arches']
        for arch in sorted(arches):
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            _push_image(request_id, arch)

    if from_index:
        _verify_index_image(prebuild_info['from_index_resolved'], from_index)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

    if legacy_support_packages:
        export_legacy_packages(
            legacy_support_packages, request_id, output_pull_spec, cnr_token, organization
        )

    _finish_request_post_build(
        output_pull_spec,
        request_id,
        arches,
        from_index,
        overwrite_from_index,
        overwrite_from_index_token,
    )


@app.task
def handle_rm_request(
    operators,
    binary_image,
    request_id,
    from_index,
    add_arches=None,
    overwrite_from_index=False,
    overwrite_from_index_token=None,
):
    """
    Coordinate the work needed to remove the input operators and rebuild the index image.

    :param list operators: a list of strings representing the name of the operators to
        remove from the index image.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param int request_id: the ID of the IIB build request
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param list add_arches: the list of arches to build in addition to the arches ``from_index`` is
        currently built for.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required for non-privileged users to use
        ``overwrite_from_index``. The format of the token must be in the format "user:password".
    :raises IIBError: if the index image build fails.
    """
    _cleanup()
    prebuild_info = _prepare_request_for_build(binary_image, request_id, from_index, add_arches)

    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        _opm_index_rm(temp_dir, operators, binary_image, from_index)

        arches = prebuild_info['arches']
        for arch in sorted(arches):
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            _push_image(request_id, arch)

    _verify_index_image(prebuild_info['from_index_resolved'], from_index)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

    _finish_request_post_build(
        output_pull_spec,
        request_id,
        arches,
        from_index,
        overwrite_from_index,
        overwrite_from_index_token,
    )


@app.task
def handle_regenerate_bundle_request(from_bundle_image, organization, request_id):
    """
    Coordinate the work needed to regenerate the operator bundle image.

    :param str from_bundle_image: the pull specification of the bundle image to be regenerated.
    :param str organization: the name of the organization the bundle should be regenerated for.
    :param int request_id: the ID of the IIB build request
    :raises IIBError: if the regenerate bundle image build fails.
    """
    _cleanup()

    set_request_state(request_id, 'in_progress', 'Resolving from_bundle_image')
    from_bundle_image_resolved = _get_resolved_image(from_bundle_image)
    arches = _get_image_arches(from_bundle_image_resolved)
    if not arches:
        raise IIBError(
            f'No arches were found in the resolved from_bundle_image {from_bundle_image_resolved}'
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
        _adjust_operator_manifests(manifests_path)

        with open(os.path.join(temp_dir, 'Dockerfile'), 'w') as dockerfile:
            dockerfile.write(
                textwrap.dedent(
                    f"""\
                        FROM {from_bundle_image_resolved}
                        COPY ./manifests /manifests
                    """
                )
            )

        for arch in sorted(arches):
            _build_image(temp_dir, 'Dockerfile', request_id, arch)
            _push_image(request_id, arch)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

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


def _adjust_operator_manifests(manifests_path):
    """
    Apply modifications to the operator manifests at the given location.

    For any container image pull spec found in the Operator CSV files, replace floating
    tags with pinned digests, e.g. `image:latest` becomes `image@sha256:...`.

    If spec.relatedImages is not set, it will be set with the pinned digests. If it is set but
    there are also RELATED_IMAGE_* environment variables set, an exception will be raised.

    This method relies on the OperatorManifest class to properly identify and apply the
    modifications as needed.

    :param str manifests_path: the full path to the directory containing the operator manifests.
    :raises IIBError: if the operator manifest has invalid entries
    """
    operator_manifest = OperatorManifest.from_directory(manifests_path)
    found_pullspecs = set()
    operator_csvs = []
    for operator_csv in operator_manifest.files:
        if operator_csv.has_related_images():
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

    # Resolve pull specs to container image digests
    replacement_pullspecs = {}
    for pullspec in found_pullspecs:
        # Skip images that are already pinned
        if ':' not in ImageName.parse(pullspec).tag:
            replacement_pullspecs[pullspec] = ImageName.parse(_get_resolved_image(pullspec))

    # Apply modifications to the operator bundle image metadata
    for operator_csv in operator_csvs:
        csv_file_name = os.path.basename(operator_csv.path)
        log.info('Pinning the pull specifications on %s', csv_file_name)
        operator_csv.replace_pullspecs_everywhere(replacement_pullspecs)

        log.info('Setting spec.relatedImages on %s', csv_file_name)
        operator_csv.set_related_images()

        operator_csv.dump()
