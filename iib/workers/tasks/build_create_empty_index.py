# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import tempfile
from typing import Dict, List, Optional

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.common.common_utils import get_binary_versions
from iib.workers.tasks.build import (
    _add_label_to_index,
    _build_image,
    _cleanup,
    _create_and_push_manifest_list,
    _push_image,
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.fbc_utils import is_image_fbc
from iib.workers.tasks.opm_operations import (
    opm_create_empty_fbc,
    opm_index_rm,
    Opm,
    get_operator_package_list,
)
from iib.workers.tasks.utils import (
    request_logger,
    prepare_request_for_build,
    RequestConfigCreateIndexImage,
)
from iib.workers.tasks.iib_static_types import PrebuildInfo

__all__ = ['handle_create_empty_index_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.handle_create_empty_index_request", attributes=get_binary_versions()
)
def handle_create_empty_index_request(
    from_index: str,
    request_id: int,
    output_fbc: bool = False,
    binary_image: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    binary_image_less_arches_allowed_versions: Optional[List[str]] = None,
) -> None:
    """Coordinate the the work needed to create the index image with labels.

    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param int request_id: the ID of the IIB build request
    :param bool output_fbc: specifies whether a File-based Catalog index image should be created
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param dict labels: the dict of labels required to be added to a new index image
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    :param list binary_image_less_arches_allowed_versions: list of versions of the binary image
        that are allowed to build for less arches. Defaults to ``None``.
    """
    _cleanup()
    prebuild_info: PrebuildInfo = prepare_request_for_build(
        request_id,
        RequestConfigCreateIndexImage(
            _binary_image=binary_image,
            from_index=from_index,
            binary_image_config=binary_image_config,
            binary_image_less_arches_allowed_versions=binary_image_less_arches_allowed_versions,
        ),
    )
    from_index_resolved = prebuild_info['from_index_resolved']
    prebuild_info['labels'] = labels
    Opm.set_opm_version(from_index_resolved)

    if not output_fbc and is_image_fbc(from_index_resolved):
        log.debug('%s is FBC index image', from_index_resolved)
        err_msg = 'Cannot create SQLite index image from File-Based Catalog index image'
        log.error(err_msg)
        raise IIBError(err_msg)

    _update_index_image_build_state(request_id, prebuild_info)

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        set_request_state(request_id, 'in_progress', 'Checking operators present in index image')

        operators = get_operator_package_list(from_index_resolved, temp_dir)

        # if output_fbc parameter is true, create an empty FBC index image
        # else create empty SQLite index image
        if output_fbc:
            log.debug('Creating empty FBC index image from %s', from_index)
            opm_create_empty_fbc(
                request_id=request_id,
                temp_dir=temp_dir,
                from_index_resolved=from_index_resolved,
                from_index=from_index,
                binary_image=prebuild_info['binary_image'],
                operators=operators,
            )
        else:
            set_request_state(request_id, 'in_progress', 'Removing operators from index image')
            opm_index_rm(
                temp_dir,
                operators,
                prebuild_info['binary_image'],
                from_index_resolved,
                container_tool='podman',
            )

        set_request_state(
            request_id, 'in_progress', 'Getting and updating labels for new index image'
        )

        iib_labels = {
            'com.redhat.index.delivery.version': prebuild_info['ocp_version'],
            'com.redhat.index.delivery.distribution_scope': prebuild_info['distribution_scope'],
        }

        if labels:
            iib_labels.update(labels)
        for index_label, value in iib_labels.items():
            _add_label_to_index(index_label, value, temp_dir, 'index.Dockerfile')

        arches = prebuild_info['arches']

        for arch in sorted(arches):
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            _push_image(request_id, arch)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches, [])

    _update_index_image_pull_spec(
        output_pull_spec=output_pull_spec,
        request_id=request_id,
        arches=arches,
        from_index=from_index,
        resolved_prebuild_from_index=from_index_resolved,
    )
    _cleanup()
    set_request_state(request_id, 'complete', 'The empty index image was successfully created')
