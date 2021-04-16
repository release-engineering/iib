# SPDX-License-Identifier: GPL-3.0-or-later
import itertools
import logging
import stat
import tempfile

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.build import (
    _add_label_to_index,
    _build_image,
    _cleanup,
    _create_and_push_manifest_list,
    _get_external_arch_pull_spec,
    get_image_label,
    _get_present_bundles,
    _opm_index_add,
    _prepare_request_for_build,
    _push_image,
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.utils import (
    chmod_recursively,
    deprecate_bundles,
    get_bundles_from_deprecation_list,
    request_logger,
    set_registry_token,
)


__all__ = ['handle_merge_request']

log = logging.getLogger(__name__)


def _add_bundles_missing_in_source(
    source_index_bundles,
    target_index_bundles,
    base_dir,
    binary_image,
    source_from_index,
    request_id,
    arch,
    ocp_version,
    overwrite_target_index_token=None,
    distribution_scope=None,
):
    """
    Rebuild index image with bundles missing from source image but present in target image.

    If no bundles are missing in the source index image, the index image is still rebuilt
    using the new binary image.

    :param list source_index_bundles: bundles present in the source index image.
    :param list target_index_bundles: bundles present in the target index image.
    :param str base_dir: base directory where operation files will be located.
    :param str binary_image: binary image to be used by the new index image.
    :param str source_from_index: index image, whose data will be contained in the new index image.
    :param int request_id: the ID of the IIB build request.
    :param str arch: the architecture to build this image for.
    :param str ocp_version: ocp version which will be added as a label to the image.
    :param str overwrite_target_index_token: the token used for overwriting the input
        ``source_from_index`` image. This is required for non-privileged users to use
        ``overwrite_target_index``. The format of the token must be in the format "user:password".
    :return: bundles which were added to the index image.
    :rtype: list
    """
    set_request_state(request_id, 'in_progress', 'Adding bundles missing in source index image')
    log.info('Adding bundles from target index image which are missing from source index image')
    missing_bundles = []
    source_bundle_digests = []
    source_bundle_csv_names = []
    target_bundle_digests = []

    for bundle in source_index_bundles:
        if '@sha256:' in bundle['bundlePath']:
            source_bundle_digests.append(bundle['bundlePath'].split('@sha256:')[-1])
            source_bundle_csv_names.append(bundle['csvName'])
        else:
            raise IIBError(
                f'Bundle {bundle["bundlePath"]} in the source index image is not defined via digest'
            )
    for bundle in target_index_bundles:
        if '@sha256:' in bundle['bundlePath']:
            target_bundle_digests.append((bundle['bundlePath'].split('@sha256:')[-1], bundle))
        else:
            raise IIBError(
                f'Bundle {bundle["bundlePath"]} in the target index image is not defined via digest'
            )

    for target_bundle_digest, bundle in target_bundle_digests:
        if (
            target_bundle_digest not in source_bundle_digests
            and bundle['csvName'] not in source_bundle_csv_names
        ):
            missing_bundles.append(bundle)

    missing_bundle_paths = [
        bundle['bundlePath']
        for bundle in itertools.chain(missing_bundles, source_index_bundles)
        if is_bundle_version_valid(bundle['bundlePath'], ocp_version)
    ]

    if missing_bundle_paths:
        log.info('%s bundles are missing in the source index image.', len(missing_bundle_paths))
    else:
        log.info(
            'No bundles are missing in the source index image. However, the index image is '
            'still being rebuilt with the new binary image.'
        )

    _opm_index_add(
        base_dir,
        missing_bundle_paths,
        binary_image,
        overwrite_from_index_token=overwrite_target_index_token,
        # Use podman until opm's default mechanism is more resilient:
        #   https://bugzilla.redhat.com/show_bug.cgi?id=1937097
        container_tool='podman',
    )
    _add_label_to_index(
        'com.redhat.index.delivery.version', ocp_version, base_dir, 'index.Dockerfile'
    )
    _add_label_to_index(
        'com.redhat.index.delivery.distribution_scope',
        distribution_scope,
        base_dir,
        'index.Dockerfile',
    )
    _build_image(base_dir, 'index.Dockerfile', request_id, arch)
    _push_image(request_id, arch)
    _create_and_push_manifest_list(request_id, [arch])
    log.info('New index image created')

    return missing_bundles


@app.task
@request_logger
def handle_merge_request(
    source_from_index,
    deprecation_list,
    request_id,
    binary_image=None,
    target_index=None,
    overwrite_target_index=False,
    overwrite_target_index_token=None,
    distribution_scope=None,
    binary_image_config=None,
):
    """
    Coordinate the work needed to merge old (N) index image with new (N+1) index image.

    :param str source_from_index: pull specification to be used as the base for building the new
        index image.
    :param str target_index: pull specification of content stage index image for the
        corresponding target index image.
    :param list deprecation_list: list of deprecated bundles for the target index image.
    :param int request_id: the ID of the IIB build request.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param bool overwrite_target_index: if True, overwrite the input ``target_index`` with
        the built index image.
    :param str overwrite_target_index_token: the token used for overwriting the input
        ``target_index`` image. This is required for non-privileged users to use
        ``overwrite_target_index``. The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :raises IIBError: if the index image merge fails.
    """
    _cleanup()
    prebuild_info = _prepare_request_for_build(
        request_id,
        binary_image,
        overwrite_from_index_token=overwrite_target_index_token,
        source_from_index=source_from_index,
        target_index=target_index,
        distribution_scope=distribution_scope,
        binary_image_config=binary_image_config,
    )
    _update_index_image_build_state(request_id, prebuild_info)
    source_from_index_resolved = prebuild_info['source_from_index_resolved']
    target_index_resolved = prebuild_info['target_index_resolved']

    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        set_request_state(request_id, 'in_progress', 'Getting bundles present in the index images')
        log.info('Getting bundles present in the source index image')
        with set_registry_token(overwrite_target_index_token, source_from_index):
            source_index_bundles, source_index_bundles_pull_spec = _get_present_bundles(
                source_from_index_resolved, temp_dir
            )

            target_index_bundles = []
            if target_index:
                log.info('Getting bundles present in the target index image')
                target_index_bundles, _ = _get_present_bundles(target_index_resolved, temp_dir)

        arches = list(prebuild_info['arches'])
        arch = 'amd64' if 'amd64' in arches else arches[0]

        missing_bundles = _add_bundles_missing_in_source(
            source_index_bundles,
            target_index_bundles,
            temp_dir,
            prebuild_info['binary_image'],
            source_from_index_resolved,
            request_id,
            arch,
            prebuild_info['target_ocp_version'],
            overwrite_target_index_token,
            distribution_scope=prebuild_info['distribution_scope'],
        )

        set_request_state(request_id, 'in_progress', 'Deprecating bundles in the deprecation list')
        log.info('Deprecating bundles in the deprecation list')
        intermediate_bundles = [
            bundle['bundlePath'] for bundle in missing_bundles
        ] + source_index_bundles_pull_spec
        deprecation_bundles = get_bundles_from_deprecation_list(
            intermediate_bundles, deprecation_list
        )
        intermediate_image_name = _get_external_arch_pull_spec(
            request_id, arch, include_transport=False
        )

        if deprecation_bundles:
            deprecate_bundles(
                deprecation_bundles,
                temp_dir,
                prebuild_info['binary_image'],
                intermediate_image_name,
                overwrite_target_index_token,
            )

        for arch in sorted(prebuild_info['arches']):
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

    output_pull_spec = _create_and_push_manifest_list(request_id, prebuild_info['arches'])
    _update_index_image_pull_spec(
        output_pull_spec,
        request_id,
        prebuild_info['arches'],
        target_index,
        overwrite_target_index,
        overwrite_target_index_token,
        target_index_resolved,
    )
    set_request_state(
        request_id, 'complete', 'The index image was successfully cleaned and updated.'
    )


def is_bundle_version_valid(bundle_path, valid_ocp_version):
    """
    Check if the version label of the bundle satisfies the index ocp_version.

    :param str bundle_path: pull specification of the bundle to be validated.
    :param str valid_ocp_version: the index ocp version against which the bundles will be validated.
    :return: a boolean indicating if the bundle_path satisfies the index ocp_version
    :rtype: bool

           |  "v4.5"   |   "=v4.6"    | "v4.5-v4.7" | "v4.5,v4.6"  | "v4.6,v4.5"
    -------------------------------------------------------------------------------
    v4.5   | included  | NOT included |  included   |  included    |  NOT included
    -------------------------------------------------------------------------------
    v4.6   | included  |   included   |  included   |  included    |  included
    -------------------------------------------------------------------------------
    v4.7   | included  | NOT included |  included   |  included    |  included
    """
    try:
        float_valid_ocp_version = float(valid_ocp_version.replace('v', ''))
    except ValueError:
        raise IIBError(f'Invalid OCP version, "{valid_ocp_version}", specified in Index Image')
    try:
        bundle_version_label = get_image_label(bundle_path, 'com.redhat.openshift.versions')
        bundle_version = bundle_version_label.replace('v', '')
        if bundle_version_label.startswith('='):
            if float(bundle_version.strip('=')) == float_valid_ocp_version:
                return True
        elif '-' in bundle_version_label:
            min_version, max_version = [float(version) for version in bundle_version.split('-')]
            if min_version <= float_valid_ocp_version <= max_version:
                return True
        elif "," in bundle_version_label:
            versions = [float(version) for version in bundle_version.split(",")]
            if float_valid_ocp_version >= versions[0]:
                return True
        elif float_valid_ocp_version >= float(bundle_version):
            return True
    except ValueError:
        log.warning(
            'Bundle %s has an invalid `com.redhat.openshift.versions` label value set: %s',
            bundle_path,
            bundle_version_label,
        )

    return False
