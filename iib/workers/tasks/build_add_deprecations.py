# SPDX-License-Identifier: GPL-3.0-or-later
import os
import json
import logging
import tempfile
from typing import Dict, Optional, Set

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
from iib.workers.tasks.fbc_utils import get_catalog_dir
from iib.workers.tasks.opm_operations import (
    Opm,
    create_dockerfile,
    generate_cache_locally,
    opm_validate,
    verify_operators_exists,
)
from iib.workers.tasks.utils import (
    prepare_request_for_build,
    request_logger,
    RequestConfigAddDeprecations,
    IIBError,
    get_worker_config,
)

__all__ = ['handle_add_deprecations_request']

log = logging.getLogger(__name__)


@app.task
@request_logger
@instrument_tracing(
    span_name="workers.tasks.build.handle_add_deprecations_request",
    attributes=get_binary_versions(),
)
def handle_add_deprecations_request(
    deprecation_schema: str,
    from_index: str,
    operator_package: str,
    request_id: int,
    overwrite_from_index: bool = False,
    binary_image: Optional[str] = None,
    build_tags: Optional[Set[str]] = None,
    binary_image_config: Optional[Dict[str, Dict[str, str]]] = None,
    overwrite_from_index_token: Optional[str] = None,
) -> None:
    """
    Add a deprecation schema to index image.

    :param int request_id: the ID of the IIB build request.
    :param str operator_package: Operator package of deprecation schema.
    :param str deprecation_schema: deprecation_schema to be added to index image.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from.
    :param bool overwrite_from_index: if True, overwrite the input ``from_index`` with the built
        index image.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param list build_tags: List of tags which will be applied to intermediate index images.
    :param dict binary_image_config: the dict of config required to identify the appropriate
        ``binary_image`` to use.
    """
    _cleanup()
    set_request_state(request_id, 'in_progress', 'Resolving the index images')

    prebuild_info = prepare_request_for_build(
        request_id,
        RequestConfigAddDeprecations(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=overwrite_from_index_token,
            operator_package=operator_package,
            deprecation_schema=deprecation_schema,
            binary_image_config=binary_image_config,
        ),
    )

    from_index_resolved = prebuild_info['from_index_resolved']
    binary_image_resolved = prebuild_info['binary_image_resolved']
    Opm.set_opm_version(from_index_resolved)

    _update_index_image_build_state(request_id, prebuild_info)

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        operators_in_db, index_db_path = verify_operators_exists(
            from_index_resolved,
            temp_dir,
            [operator_package],
            overwrite_from_index_token,
        )
        if not operators_in_db:
            err_msg = (
                f'Cannot add deprecations for {operator_package},'
                f' It is either not present in index or opted in fbc'
            )
            log.error(err_msg)
            raise IIBError(err_msg)

        add_deprecations_to_index(
            request_id,
            temp_dir,
            from_index_resolved,
            operator_package,
            deprecation_schema,
            binary_image_resolved,
            index_db_path,
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
        request_id, 'complete', 'The deprecation schema was successfully added to the index image'
    )


def add_deprecations_to_index(
    request_id,
    temp_dir,
    from_index_resolved,
    operator_package,
    deprecation_schema,
    binary_image_resolved,
    index_db_path,
) -> None:
    """
    Add deprecation schema for a package in operator-deprecations sub-directory.

    :param int request_id: the ID of the IIB build request.
    :param str temp_dir: the base directory to generate the database and index.Dockerfile in.
    :param str from_index_resolved: the resolved pull specification of the container image.
        containing the index that the index image build will be based from.
    :param str operator_package: the operator package for which deprecations need to be added.
    :param str deprecation_schema: deprecation_schema to be added to index image.
    :param str binary_image_resolved: the pull specification of the image where the opm binary
        gets copied from.
    :param str index_db: path to locally stored index.db.

    """
    set_request_state(request_id, 'in_progress', 'Getting all deprecations present in index image')

    from_index_configs_dir = get_catalog_dir(from_index=from_index_resolved, base_dir=temp_dir)
    conf = get_worker_config()
    from_index_configs_deprecations_dir = os.path.join(
        from_index_configs_dir, conf['operator_deprecations_dir']
    )

    operator_dir = os.path.join(from_index_configs_deprecations_dir, operator_package)
    if not os.path.exists(operator_dir):
        os.makedirs(operator_dir)

    operator_deprecations_file = os.path.join(operator_dir, f'{operator_package}.json')
    set_request_state(request_id, 'in_progress', 'Adding deprecations to from_index')

    with open(operator_deprecations_file, 'w') as output_file:
        json.dump(json.loads(deprecation_schema), output_file)

    opm_validate(from_index_configs_dir)

    local_cache_path = os.path.join(temp_dir, 'cache')
    generate_cache_locally(
        base_dir=temp_dir, fbc_dir=from_index_configs_dir, local_cache_path=local_cache_path
    )

    log.info("Dockerfile generated from %s", from_index_configs_dir)
    create_dockerfile(
        fbc_dir=from_index_configs_dir,
        base_dir=temp_dir,
        index_db=index_db_path,
        binary_image=binary_image_resolved,
        dockerfile_name='index.Dockerfile',
    )
