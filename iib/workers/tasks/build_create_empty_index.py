# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import tempfile
import json
import re

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.tasks.build import (
    _add_label_to_index,
    _build_image,
    _cleanup,
    _create_and_push_manifest_list,
    _opm_index_rm,
    _push_image,
    _update_index_image_build_state,
    _update_index_image_pull_spec,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.dc_utils import is_image_dc
from iib.workers.tasks.utils import (
    request_logger,
    prepare_request_for_build,
    RequestConfigCreateIndexImage,
    grpcurl_get_db_data,
)

__all__ = ['handle_create_empty_index_request']

log = logging.getLogger(__name__)


def _get_present_operators(from_index, base_dir):
    """Get a list of operators already present in the index image.

    :param str from_index: index image to inspect.
    :param str base_dir: base directory to create temporary files in.
    :return: list of unique present operators as provided by the grpc query
    :rtype: list
    :raises IIBError: if any of the commands fail.
    """
    operators = grpcurl_get_db_data(from_index, base_dir, "api.Registry/ListPackages")

    # If no data is returned there are not operators present
    if not operators:
        return []

    # Transform returned data to parsable json
    present_operators = []
    new_operators = [json.loads(operator) for operator in re.split(r'(?<=})\n(?={)', operators)]

    for operator in new_operators:
        present_operators.append(operator['name'])

    return present_operators


@app.task
@request_logger
def handle_create_empty_index_request(
    from_index, request_id, binary_image=None, labels=None, binary_image_config=None
):
    """Coordinate the the work needed to create the index image with labels.

    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param dict labels: the dict of labels required to be added to a new index image
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    """
    _cleanup()

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigCreateIndexImage(
            _binary_image=binary_image,
            from_index=from_index,
            binary_image_config=binary_image_config,
        ),
    )
    from_index_resolved = prebuild_info['from_index_resolved']
    prebuild_info['labels'] = labels

    _update_index_image_build_state(request_id, prebuild_info)
    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        if is_image_dc(from_index):
            err_msg = 'Declarative config image type is not supported yet.'
            log.error(err_msg)
            raise IIBError(err_msg)
        set_request_state(request_id, 'in_progress', 'Checking operators present in index image')

        operators = _get_present_operators(from_index_resolved, temp_dir)
        set_request_state(request_id, 'in_progress', 'Removing operators from index image')

        _opm_index_rm(temp_dir, operators, prebuild_info['binary_image'], from_index_resolved)

        set_request_state(
            request_id, 'in_progress', 'Getting and updating labels for new index image'
        )

        iib_labels = {
            'com.redhat.index.delivery.version': prebuild_info['ocp_version'],
            'com.redhat.index.delivery.distribution_scope': prebuild_info['distribution_scope'],
        }

        iib_labels.update(labels)
        for index_label, value in iib_labels.items():
            _add_label_to_index(index_label, value, temp_dir, 'index.Dockerfile')

        arches = prebuild_info['arches']

        for arch in sorted(arches):
            _build_image(temp_dir, 'index.Dockerfile', request_id, arch)
            _push_image(request_id, arch)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

    _update_index_image_pull_spec(
        output_pull_spec=output_pull_spec,
        request_id=request_id,
        arches=arches,
        from_index=from_index,
        resolved_prebuild_from_index=from_index_resolved,
    )
    set_request_state(request_id, 'complete', 'The empty index image was successfully created')
