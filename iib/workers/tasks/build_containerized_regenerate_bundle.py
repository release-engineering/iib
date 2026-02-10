# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import tempfile
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional

import ruamel.yaml

from iib.common.common_utils import get_binary_versions
from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state, update_request
from iib.workers.config import get_worker_config
from iib.workers.tasks.build_regenerate_bundle import (
    _adjust_operator_bundle,
    _get_package_annotations,
)
from iib.workers.tasks.celery import app
from iib.workers.tasks.containerized_utils import (
    extract_files_from_image_non_privileged,
    git_commit_and_create_mr_or_push,
    monitor_pipeline_and_extract_image,
    replicate_image_to_tagged_destinations,
    cleanup_on_failure,
    cleanup_merge_request_if_exists,
)
from iib.workers.tasks.git_utils import (
    clone_git_repo,
    get_git_token,
)
from iib.workers.tasks.utils import (
    get_image_arches,
    get_image_labels,
    get_resolved_image,
    request_logger,
    set_registry_auths,
)
from iib.workers.tasks.iib_static_types import UpdateRequestPayload


__all__ = ['handle_containerized_regenerate_bundle_request']

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
@instrument_tracing(
    span_name="workers.tasks.build.handle_containerized_regenerate_bundle_request",
    attributes=get_binary_versions(),
)
def handle_containerized_regenerate_bundle_request(
    from_bundle_image: str,
    organization: str,
    request_id: int,
    registry_auths: Optional[Dict[str, Any]] = None,
    bundle_replacements: Optional[Dict[str, str]] = None,
    index_to_gitlab_push_map: Optional[Dict[str, str]] = None,
    binary_image_less_arches_allowed_versions: Optional[List[str]] = None,
    regenerate_bundle_repo_key: str = 'regenerate-bundle',
) -> None:
    """
    Coordinate the work needed to regenerate the operator bundle image using containerized workflow.

    :param str from_bundle_image: the pull specification of the bundle image to be regenerated.
    :param str organization: the name of the organization the bundle should be regenerated for.
    :param int request_id: the ID of the IIB build request.
    :param dict registry_auths: Provide the dockerconfig.json for authentication to private
      registries, defaults to ``None``.
    :param dict bundle_replacements: Dictionary mapping from original bundle pullspecs to rebuilt
      bundle pullspecs.
    :param dict index_to_gitlab_push_map: the dict mapping index images (keys) to GitLab repos
      (values) in order to push their catalogs into GitLab.
    :param str regenerate_bundle_repo_key: the key to look up the actual repo URL from
      index_to_gitlab_push_map, defaults to ``regenerate-bundle``.
    :param list binary_image_less_arches_allowed_versions: list of versions of the binary image
        that are allowed to build for less arches. Defaults to ``None``.
    :raises IIBError: if the regenerate bundle image build fails.
    """
    bundle_replacements = bundle_replacements or {}

    set_request_state(request_id, 'in_progress', 'Resolving from_bundle_image')

    mr_details: Optional[Dict[str, str]] = None
    bundle_git_repo: Optional[str] = None
    last_commit_sha: Optional[str] = None
    output_pull_spec: Optional[str] = None

    with set_registry_auths(registry_auths):
        from_bundle_image_resolved = get_resolved_image(from_bundle_image)

        arches = get_image_arches(from_bundle_image_resolved)
        if not arches:
            raise IIBError(
                'No arches were found in the resolved from_bundle_image '
                f'{from_bundle_image_resolved}'
            )

        pinned_by_iib_label = (
            get_image_labels(from_bundle_image_resolved).get('com.redhat.iib.pinned') or 'false'
        )
        pinned_by_iib = yaml.load(pinned_by_iib_label)

        arches_str = ', '.join(sorted(arches))
        log.debug('Set to regenerate the bundle image for the following arches: %s', arches_str)

        payload: UpdateRequestPayload = {
            'from_bundle_image_resolved': from_bundle_image_resolved,
            'state': 'in_progress',
            'state_reason': f'Regenerating the bundle image for the following arches: {arches_str}',
        }
        exc_msg = 'Failed setting the resolved "from_bundle_image" on the request'
        update_request(request_id, payload, exc_msg=exc_msg)

        with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
            bundle_git_repo = (
                index_to_gitlab_push_map.get(regenerate_bundle_repo_key)
                if index_to_gitlab_push_map
                else None
            )
            if not bundle_git_repo:
                raise IIBError(f"Repository not found for key: {regenerate_bundle_repo_key}")
            # Get Git token
            token_name, git_token = get_git_token(bundle_git_repo)

            # Clone Git repository
            set_request_state(request_id, 'in_progress', 'Cloning Git repository')
            # Use regenerate_bundle_repo_key as branch name for bundle regeneration
            branch = regenerate_bundle_repo_key
            local_git_repo_path = Path(temp_dir) / 'git' / branch
            local_git_repo_path.mkdir(parents=True, exist_ok=True)
            clone_git_repo(bundle_git_repo, branch, token_name, git_token, str(local_git_repo_path))

            # Extract bundle contents
            set_request_state(request_id, 'in_progress', 'Extracting bundle contents')
            manifests_path = local_git_repo_path / 'manifests'
            extract_files_from_image_non_privileged(
                from_bundle_image_resolved, '/manifests', str(manifests_path)
            )
            metadata_path = local_git_repo_path / 'metadata'
            extract_files_from_image_non_privileged(
                from_bundle_image_resolved, '/metadata', str(metadata_path)
            )

            # Apply bundle modifications
            set_request_state(request_id, 'in_progress', 'Modifying bundle manifests')
            new_labels = _adjust_operator_bundle(
                str(manifests_path),
                str(metadata_path),
                request_id,
                organization=organization,
                pinned_by_iib=pinned_by_iib,
                bundle_replacements=bundle_replacements,
            )

            # Get package name for metadata
            annotations_yaml = _get_package_annotations(str(metadata_path))
            package_name = annotations_yaml['annotations'][
                'operators.operatorframework.io.bundle.package.v1'
            ]

            # Create Dockerfile with labels
            set_request_state(request_id, 'in_progress', 'Creating Dockerfile')
            dockerfile_path = local_git_repo_path / 'Dockerfile'
            with open(dockerfile_path, 'w') as dockerfile:
                dockerfile.write(
                    textwrap.dedent(
                        f"""\
                            FROM {from_bundle_image_resolved}
                            COPY ./manifests /manifests
                            COPY ./metadata /metadata
                        """
                    )
                )
                # Add labels directly in Dockerfile
                for name, value in new_labels.items():
                    dockerfile.write(f'LABEL {name}={value}\n')

            # Write build metadata (without distribution_scope, ocp_version, and opm_version)
            set_request_state(request_id, 'in_progress', 'Writing build metadata')
            metadata = {
                'request_id': request_id,
                'arches': sorted(list(arches)),
                'organization': organization,
                'package_name': package_name,
            }
            metadata_path_file = local_git_repo_path / '.iib-build-metadata.json'
            with open(metadata_path_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            log.info('Written build metadata to %s', metadata_path_file)

            try:
                # Commit changes and create MR to trigger Konflux pipeline
                # Bundle regeneration is always a throw-away request (no overwrite)
                mr_details, last_commit_sha = git_commit_and_create_mr_or_push(
                    request_id=request_id,
                    local_git_repo_path=str(local_git_repo_path),
                    index_git_repo=bundle_git_repo,
                    branch=branch,
                    commit_message=(
                        f"IIB: Regenerate bundle for request {request_id}\n\n"
                        f"Organization: {organization}\n"
                        f"Package: {package_name}"
                    ),
                    overwrite_from_index=False,  # Always use MR for bundle regeneration
                )

                # Wait for Konflux pipeline and extract built image URL
                image_url = monitor_pipeline_and_extract_image(
                    request_id=request_id,
                    last_commit_sha=last_commit_sha,
                )

                # Copy built bundle to all output pull specs
                output_pull_specs = replicate_image_to_tagged_destinations(
                    request_id=request_id,
                    image_url=image_url,
                    build_tags=None,  # No additional tags for bundle regeneration
                )

                # Use the first output_pull_spec as the primary one
                if not output_pull_specs:
                    raise IIBError(
                        "No output pull specs were generated. "
                        "This should not happen if the pipeline completed successfully."
                    )
                output_pull_spec = output_pull_specs[0]

                # Apply output registry replacement if configured
                conf = get_worker_config()
                if conf.get('iib_index_image_output_registry'):
                    old_output_pull_spec = output_pull_spec
                    output_pull_spec = output_pull_spec.replace(
                        conf['iib_registry'], conf['iib_index_image_output_registry'], 1
                    )
                    log.info(
                        'Changed the bundle_image pull specification from %s to %s',
                        old_output_pull_spec,
                        output_pull_spec,
                    )

                # Close MR if it was opened
                cleanup_merge_request_if_exists(mr_details, bundle_git_repo)

                # Update request with final output
                payload = {
                    'arches': list(arches),
                    'bundle_image': output_pull_spec,
                    'state': 'complete',
                    'state_reason': 'The request completed successfully',
                }
                update_request(
                    request_id, payload, exc_msg='Failed setting the bundle image on the request'
                )

            except Exception as e:
                cleanup_on_failure(
                    mr_details=mr_details,
                    last_commit_sha=last_commit_sha,
                    index_git_repo=bundle_git_repo,
                    overwrite_from_index=False,  # Bundle regeneration never overwrites
                    request_id=request_id,
                    from_index='',  # No from_index for bundle regeneration
                    index_repo_map={},
                    original_index_db_digest=None,  # No index.db for bundle regeneration
                    reason=f"error: {e}",
                )
                raise IIBError(f"Failed to regenerate bundle: {e}")
