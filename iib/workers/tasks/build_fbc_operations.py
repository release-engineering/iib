# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import tempfile
from typing import Dict, List, Optional, Set

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
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
from iib.workers.tasks.opm_operations import opm_registry_add_fbc_fragment, Opm
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
@instrument_tracing(
    span_name="workers.tasks.build.handle_fbc_operation_request", attributes=get_binary_versions()
)
def handle_fbc_operation_request(
    request_id: int,
    fbc_fragments: List[str],
    from_index: Optional[str] = None,
    binary_image: Optional[str] = None,
    distribution_scope: Optional[str] = None,
    overwrite_from_index: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    build_tags: Optional[Set[str]] = None,
    add_arches: Optional[Set[str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
) -> None:
    """
    Add fbc fragments to an fbc index image.

    :param list fbc_fragments: list of fbc fragments that need to be added to final FBC index image
    :param int request_id: the ID of the IIB build request
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param set add_arches: the set of arches to build in addition to the arches ``from_index`` is
        currently built for; if ``from_index`` is ``None``, then this is used as the list of arches
        to build the index image for
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
        (values) in order to push their catalogs into GitLab.
    """
    _cleanup()
    set_request_state(request_id, 'in_progress', 'Resolving the fbc fragments')

    # Resolve all fbc fragments
    resolved_fbc_fragments = []
    for fbc_fragment in fbc_fragments:
        with set_registry_token(overwrite_from_index_token, fbc_fragment, append=True):
            resolved_fbc_fragment = get_resolved_image(fbc_fragment)
            resolved_fbc_fragments.append(resolved_fbc_fragment)

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigFBCOperation(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            add_arches=add_arches,
            fbc_fragments=fbc_fragments,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )

    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']
    Opm.set_opm_version(from_index_resolved)

    # Store the first resolved fragment for backward compatibility
    prebuild_info['fbc_fragment_resolved'] = (
        resolved_fbc_fragments[0] if resolved_fbc_fragments else None
    )
    # Store all resolved fragments
    prebuild_info['fbc_fragments_resolved'] = resolved_fbc_fragments

    _update_index_image_build_state(request_id, prebuild_info)

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        # Process all resolved fbc fragments at once
        opm_registry_add_fbc_fragment(
            request_id,
            temp_dir,
            from_index_resolved,
            binary_image_resolved,
            resolved_fbc_fragments,
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
        output_pull_spec=output_pull_spec,
        request_id=request_id,
        arches=arches,
        from_index=from_index,
        overwrite_from_index=overwrite_from_index,
        overwrite_from_index_token=overwrite_from_index_token,
        resolved_prebuild_from_index=from_index_resolved,
        add_or_rm=True,
    )
    _cleanup()
    set_request_state(
        request_id,
        'complete',
        f"The {len(resolved_fbc_fragments)} FBC fragment(s) were successfully added"
        "in the index image",
    )
