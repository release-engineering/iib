# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import tempfile
from typing import Dict, Optional, Set

from iib.workers.api_utils import set_request_state
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
from iib.workers.tasks.opm_operations import opm_registry_add_fbc_fragment
from iib.workers.tasks.utils import (
    get_resolved_image,
    prepare_request_for_build,
    request_logger,
    set_registry_token,
    RequestConfigFBCOperation,
)

__all__ = ['handle_fbc_operation_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
def handle_fbc_operation_request(
    request_id: int,
    fbc_fragment: str,
    from_index: Optional[str] = None,
    binary_image: Optional[str] = None,
    distribution_scope: Optional[str] = None,
    overwrite_from_index: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    build_tags: Optional[Set[str]] = None,
    add_arches: Optional[Set[str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
) -> None:
    """
    Add a fbc fragment to an fbc index image.

    :param str fbc_fragment: fbc fragment that needs to be added to final FBC index image
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param set add_arches: the set of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    """
    _cleanup()
    set_request_state(request_id, 'in_progress', 'Resolving the fbc fragment')

    with set_registry_token(overwrite_from_index_token, fbc_fragment, append=True):
        resolved_fbc_fragment = get_resolved_image(fbc_fragment)

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            fbc_fragment=fbc_fragment,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )

    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']

    prebuild_info['fbc_fragment_resolved'] = resolved_fbc_fragment

    _update_index_image_build_state(request_id, prebuild_info)

    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        opm_registry_add_fbc_fragment(
            request_id,
            temp_dir,
            from_index_resolved,
            binary_image_resolved,
            resolved_fbc_fragment,
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
        request_id, 'complete', 'The FBC fragment was successfully added in the index image'
    )
