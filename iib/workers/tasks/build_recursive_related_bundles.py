# SPDX-License-Identifier: GPL-3.0-or-later
import copy
import logging
import os
import tempfile
from typing import List, Optional

from operator_manifest.operator import OperatorManifest
import ruamel.yaml

from iib.common.pydantic_models import RecursiveRelatedBundlesPydanticModel
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state, update_request
from iib.workers.tasks.build import (
    _cleanup,
    _copy_files_from_image,
)
from iib.workers.tasks.build_regenerate_bundle import (
    _adjust_operator_bundle,
    get_related_bundle_images,
    write_related_bundles_file,
)
from iib.workers.config import get_worker_config
from iib.workers.tasks.celery import app
from iib.workers.tasks.utils import (
    get_resolved_image,
    podman_pull,
    request_logger,
    set_registry_auths,
    get_bundle_metadata,
)
from iib.workers.tasks.iib_static_types import UpdateRequestPayload


__all__ = ['handle_recursive_related_bundles_request']

yaml = ruamel.yaml.YAML()
# IMPORTANT: ruamel will introduce a line break if the yaml line is longer than yaml.width.
# Unfortunately, this causes issues for JSON values nested within a YAML file, e.g.
# metadata.annotations."alm-examples" in a CSV file.
# The default value is 80. Set it to a more forgiving higher number to avoid issues
yaml.width = 200
# ruamel will also cause issues when normalizing a YAML object that contains
# a nested JSON object when it does not preserve quotes. Thus, it produces
# invalid YAML. Let's prevent this from happening at all.
yaml.preserve_quotes = True
log = logging.getLogger(__name__)


@app.task
@request_logger
def handle_recursive_related_bundles_request(
    payload: RecursiveRelatedBundlesPydanticModel,
    request_id: int,
) -> None:
    """
    Coordinate the work needed to find recursive related bundles of the operator bundle image.

    :param str parent_bundle_image: the pull specification of the bundle image to whose related
        bundles are to be found.
    :param str organization: the name of the organization the bundle should be regenerated for.
    :param int request_id: the ID of the IIB build request
    :param dict registry_auths: Provide the dockerconfig.json for authentication to private
      registries, defaults to ``None``.
    :raises IIBError: if the recursive related bundles build fails.
    """
    _cleanup()

    set_request_state(request_id, 'in_progress', 'Resolving parent_bundle_image')

    with set_registry_auths(payload.registry_auths):
        parent_bundle_image_resolved = get_resolved_image(payload.parent_bundle_image)

        update_payload: UpdateRequestPayload = {
            'parent_bundle_image_resolved': parent_bundle_image_resolved,
            'state': 'in_progress',
            'state_reason': (
                f'Finding recursive related bundles for the bundle: {payload.parent_bundle_image}'
            ),
        }
        update_request(request_id, update_payload)

        recursive_related_bundles = [parent_bundle_image_resolved]
        current_level_related_bundles = [parent_bundle_image_resolved]
        total_related_bundles = 0
        conf = get_worker_config()
        traversal_completed = False
        while not traversal_completed:
            temp_current_level_related_bundles = copy.deepcopy(current_level_related_bundles)
            current_level_related_bundles = []
            for bundle in temp_current_level_related_bundles:
                children_related_bundles = process_parent_bundle_image(
                    bundle, request_id, payload.organization
                )
                current_level_related_bundles.extend(children_related_bundles)

                total_related_bundles += len(children_related_bundles)
                if total_related_bundles >= conf['iib_max_recursive_related_bundles']:
                    raise IIBError('Max number of related bundles exceeded. Potential DOS attack!')

            recursive_related_bundles.extend(current_level_related_bundles)
            if not current_level_related_bundles:
                traversal_completed = True

    update_payload = {
        'state': 'in_progress',
        'state_reason': 'Writing recursive related bundles to a file',
    }
    update_request(
        request_id,
        update_payload,
        exc_msg='Failed setting the bundle image on the request',
    )
    # Reverse the list while writing because we did a top to bottom level traversal of a tree.
    # The return value should be a bottom to top level traversal.
    write_related_bundles_file(
        recursive_related_bundles[::-1],
        request_id,
        conf['iib_request_recursive_related_bundles_dir'],
        'recursive_related_bundles',
    )

    update_payload = {
        'state': 'complete',
        'state_reason': 'The request completed successfully',
    }
    _cleanup()
    update_request(
        request_id,
        update_payload,
        exc_msg='Failed setting the bundle image on the request',
    )


def process_parent_bundle_image(
    bundle_image_resolved: str, request_id: int, organization: Optional[str] = None
) -> List[str]:
    """
    Apply required customization and get children bundles (aka related bundles) for a bundle image.

    :param str bundle_image_resolved: the pull specification of the bundle image to whose children
        bundles are to be found.
    :param int request_id: the ID of the IIB build request
    :param str organization: the name of the organization the to apply customizations on.
    :rtype: list
    :return: the list of all children bundles for a parent bundle image
    :raises IIBError: if fails to process the parent bundle image.
    """
    # Pull the bundle_image to ensure steps later on don't fail due to registry timeouts
    podman_pull(bundle_image_resolved)

    with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
        manifests_path = os.path.join(temp_dir, 'manifests')
        _copy_files_from_image(bundle_image_resolved, '/manifests', manifests_path)
        metadata_path = os.path.join(temp_dir, 'metadata')
        _copy_files_from_image(bundle_image_resolved, '/metadata', metadata_path)
        if organization:
            _adjust_operator_bundle(
                manifests_path,
                metadata_path,
                request_id,
                organization,
                recursive_related_bundles=True,
            )

        try:
            operator_manifest = OperatorManifest.from_directory(manifests_path)
        except (ruamel.yaml.YAMLError, ruamel.yaml.constructor.DuplicateKeyError) as e:
            error = f'The Operator Manifest is not in a valid YAML format: {e}'
            log.exception(error)
            raise IIBError(error)

        bundle_metadata = get_bundle_metadata(operator_manifest, pinned_by_iib=False)
        return get_related_bundle_images(bundle_metadata)
