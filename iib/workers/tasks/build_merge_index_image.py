# SPDX-License-Identifier: GPL-3.0-or-later
import itertools
import logging
import os
import stat
import tempfile

from iib.workers.config import get_worker_config
from iib.workers.tasks.opm_operations import (
    opm_registry_add_fbc,
    opm_migrate,
    opm_generate_dockerfile,
    deprecate_bundles_fbc,
)
from packaging.version import Version

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
    _push_image,
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.fbc_utils import is_image_fbc
from iib.workers.tasks.utils import (
    add_max_ocp_version_property,
    chmod_recursively,
    deprecate_bundles,
    get_bundles_from_deprecation_list,
    request_logger,
    set_registry_token,
    prepare_request_for_build,
    RequestConfigMerge,
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
        ``source_from_index`` image. This is required to use ``overwrite_target_index``.
        The format of the token must be in the format "user:password".
    :return: tuple where the first value is a list of bundles which were added to the index image
        and the second value is a list of bundles in the new index whose ocp_version range does not
        satisfy the ocp_version value of the target index.
    :rtype: tuple
    """
    set_request_state(request_id, 'in_progress', 'Adding bundles missing in source index image')
    log.info('Adding bundles from target index image which are missing from source index image')
    missing_bundles = []
    missing_bundle_paths = []
    # This list stores the bundles whose ocp_version range does not satisfy the ocp_version
    # of the target index
    invalid_bundles = []
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
            missing_bundle_paths.append(bundle['bundlePath'])

    for bundle in itertools.chain(missing_bundles, source_index_bundles):
        if not is_bundle_version_valid(bundle['bundlePath'], ocp_version):
            invalid_bundles.append(bundle)

    if invalid_bundles:
        log.info(
            '%s bundles have invalid version label and will be deprecated.', len(invalid_bundles)
        )

    is_source_fbc = is_image_fbc(source_from_index)
    if is_source_fbc:
        opm_registry_add_fbc(
            base_dir=base_dir,
            bundles=missing_bundle_paths,
            binary_image=binary_image,
            from_index=source_from_index,
            container_tool='podman',
        )
    else:
        _opm_index_add(
            base_dir=base_dir,
            bundles=missing_bundle_paths,
            binary_image=binary_image,
            from_index=source_from_index,
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
    log.info('New index image created')

    return (missing_bundles, invalid_bundles)


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
    build_tags=None,
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
        ``target_index`` image. This is required to use ``overwrite_target_index``.
        The format of the token must be in the format "user:password".
    :param str distribution_scope: the scope for distribution of the index image, defaults to
        ``None``.
    :param build_tags: list of extra tag to use for intermetdiate index image
    :raises IIBError: if the index image merge fails.
    """
    _cleanup()
    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigMerge(
            _binary_image=binary_image,
            overwrite_target_index_token=overwrite_target_index_token,
            source_from_index=source_from_index,
            target_index=target_index,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )
    _update_index_image_build_state(request_id, prebuild_info)
    source_from_index_resolved = prebuild_info['source_from_index_resolved']
    target_index_resolved = prebuild_info['target_index_resolved']
    dockerfile_name = 'index.Dockerfile'

    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        source_fbc = is_image_fbc(source_from_index_resolved)
        target_fbc = is_image_fbc(target_index_resolved)

        # do not remove - logging requested by stakeholders
        if source_fbc:
            log.info("Processing source index image as File-Based Catalog image")
        if target_fbc:
            log.info("Processing target index image as File-Based Catalog image")

        if source_fbc and not target_fbc:
            err_msg = (
                'Cannot merge source File-Based Catalog index image into target SQLite index image.'
            )
            log.error(err_msg)
            raise IIBError(err_msg)

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
        arch = sorted(arches)[0]

        missing_bundles, invalid_version_bundles = _add_bundles_missing_in_source(
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

        missing_bundle_paths = [bundle['bundlePath'] for bundle in missing_bundles]
        if missing_bundle_paths:
            add_max_ocp_version_property(missing_bundle_paths, temp_dir)
        set_request_state(request_id, 'in_progress', 'Deprecating bundles in the deprecation list')
        log.info('Deprecating bundles in the deprecation list')
        intermediate_bundles = missing_bundle_paths + source_index_bundles_pull_spec
        deprecation_bundles = get_bundles_from_deprecation_list(
            intermediate_bundles, deprecation_list
        )
        # We do not need to pass the invalid_version_bundles through the
        # get_bundles_from_deprecation_list function because we already know
        # they are present in the newly created index.
        deprecation_bundles = deprecation_bundles + [
            bundle['bundlePath'] for bundle in invalid_version_bundles
        ]

        if deprecation_bundles:
            intermediate_image_name = _get_external_arch_pull_spec(
                request_id, arch, include_transport=False
            )

            # we can check if source index is FBC or not because intermediate_image
            # will be always the same type because it is built
            # from source index image in _add_bundles_missing_in_source()
            if source_fbc:
                deprecate_bundles_fbc(
                    bundles=deprecation_bundles,
                    base_dir=temp_dir,
                    binary_image=prebuild_info['binary_image'],
                    from_index=intermediate_image_name,
                )
            else:
                # opm can only deprecate a bundle image on an existing index image. Build and
                # push a temporary index image to satisfy this requirement. Any arch will do.
                # NOTE: we cannot use local builds because opm commands fails,
                # index image has to be pushed to registry
                _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
                _push_image(request_id, arch)

                deprecate_bundles(
                    bundles=deprecation_bundles,
                    base_dir=temp_dir,
                    binary_image=prebuild_info['binary_image'],
                    from_index=intermediate_image_name,
                    overwrite_target_index_token=overwrite_target_index_token,
                )

        if target_fbc:
            index_db_file = os.path.join(temp_dir, get_worker_config()['temp_index_db_path'])
            # make sure FBC is generated right before build
            fbc_dir = opm_migrate(index_db=index_db_file, base_dir=temp_dir)
            if not source_fbc:
                # when source image is not FBC, but final image should be an FBC image
                # we have to generate Dockerfile for FBC (with hidden index.db)
                dockerfile_path = os.path.join(temp_dir, dockerfile_name)
                if os.path.isfile(dockerfile_path):
                    log.info('Removing previously generated dockerfile.')
                    os.remove(dockerfile_path)
                opm_generate_dockerfile(
                    fbc_dir=fbc_dir,
                    base_dir=temp_dir,
                    index_db=index_db_file,
                    binary_image=prebuild_info['binary_image'],
                    dockerfile_name=dockerfile_name,
                )

        _add_label_to_index(
            'com.redhat.index.delivery.version',
            prebuild_info['target_ocp_version'],
            temp_dir,
            dockerfile_name,
        )

        _add_label_to_index(
            'com.redhat.index.delivery.distribution_scope',
            prebuild_info['distribution_scope'],
            temp_dir,
            dockerfile_name,
        )

        for arch in sorted(prebuild_info['arches']):
            _build_image(temp_dir, dockerfile_name, request_id, arch)
            _push_image(request_id, arch)

        # If the container-tool podman is used in the opm commands above, opm will create temporary
        # files and directories without the write permission. This will cause the context manager
        # to fail to delete these files. Adjust the file modes to avoid this error.
        chmod_recursively(
            temp_dir,
            dir_mode=(stat.S_IRWXU | stat.S_IRWXG),
            file_mode=(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP),
        )

    output_pull_spec = _create_and_push_manifest_list(
        request_id, prebuild_info['arches'], build_tags
    )
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

           |  "v4.5"   |   "=v4.6"    | "v4.5-v4.7"  | "v4.5,v4.6" | "v4.6,v4.5"
    -------------------------------------------------------------------------------
    v4.5   | included  | NOT included |   included   |  included   |  NOT included
    -------------------------------------------------------------------------------
    v4.6   | included  |   included   |   included   |  included   |  included
    -------------------------------------------------------------------------------
    v4.7   | included  | NOT included |   included   |  included   |  included
    -------------------------------------------------------------------------------
    v4.8   | included  | NOT included | NOT included |  included   |  included
    """
    try:
        ocp_version = Version(valid_ocp_version.replace('v', ''))
    except ValueError:
        raise IIBError(f'Invalid OCP version, "{valid_ocp_version}", specified in Index Image')
    try:
        bundle_version_label = get_image_label(bundle_path, 'com.redhat.openshift.versions')
        bundle_version = bundle_version_label.replace('v', '')
        log.debug(f'Bundle version {bundle_version}, Index image version {valid_ocp_version}')
        if bundle_version_label.startswith('='):
            if Version(bundle_version.strip('=')) == ocp_version:
                return True
        elif '-' in bundle_version_label:
            min_version, max_version = [Version(version) for version in bundle_version.split('-')]
            if min_version <= ocp_version <= max_version:
                return True
        elif "," in bundle_version_label:
            versions = [Version(version) for version in bundle_version.split(",")]
            if versions[0] <= ocp_version:
                return True
        elif Version(bundle_version) <= ocp_version:
            return True
    except ValueError:
        log.warning(
            'Bundle %s has an invalid `com.redhat.openshift.versions` label value set: %s',
            bundle_path,
            bundle_version_label,
        )

    return False
