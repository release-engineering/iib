# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import tempfile
import textwrap

from operator_manifest.operator import ImageName, OperatorManifest
import ruamel.yaml

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state, update_request
from iib.workers.tasks.build import (
    _cleanup,
    get_image_label,
    _build_image,
    _push_image,
    _create_and_push_manifest_list,
    _copy_files_from_image,
)
from iib.workers.config import get_worker_config
from iib.workers.tasks.celery import app
from iib.workers.tasks.utils import (
    get_resolved_image,
    podman_pull,
    request_logger,
    set_registry_auths,
    get_image_arches,
)


__all__ = ['handle_regenerate_bundle_request']

yaml = ruamel.yaml.YAML()
# IMPORTANT: ruamel will introduce a line break if the yaml line is longer than yaml.width.
# Unfortunately, this causes issues for JSON values nested within a YAML file, e.g.
# metadata.annotations."alm-examples" in a CSV file.
# The default value is 80. Set it to a more forgiving higher number to avoid issues
yaml.width = 200
log = logging.getLogger(__name__)


@app.task
@request_logger
def handle_regenerate_bundle_request(
    from_bundle_image, organization, request_id, registry_auths=None
):
    """
    Coordinate the work needed to regenerate the operator bundle image.

    :param str from_bundle_image: the pull specification of the bundle image to be regenerated.
    :param str organization: the name of the organization the bundle should be regenerated for.
    :param int request_id: the ID of the IIB build request
    :param dict registry_auths: Provide the dockerconfig.json for authentication to private
      registries, defaults to ``None``.
    :raises IIBError: if the regenerate bundle image build fails.
    """
    _cleanup()

    set_request_state(request_id, 'in_progress', 'Resolving from_bundle_image')

    with set_registry_auths(registry_auths):
        from_bundle_image_resolved = get_resolved_image(from_bundle_image)

        arches = get_image_arches(from_bundle_image_resolved)
        if not arches:
            raise IIBError(
                'No arches were found in the resolved from_bundle_image '
                f'{from_bundle_image_resolved}'
            )

        pinned_by_iib = yaml.load(
            get_image_label(from_bundle_image_resolved, 'com.redhat.iib.pinned') or 'false'
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
            metadata_path = os.path.join(temp_dir, 'metadata')
            _copy_files_from_image(from_bundle_image_resolved, '/metadata', metadata_path)
            new_labels = _adjust_operator_bundle(
                manifests_path, metadata_path, organization, pinned_by_iib
            )

            with open(os.path.join(temp_dir, 'Dockerfile'), 'w') as dockerfile:
                dockerfile.write(
                    textwrap.dedent(
                        f"""\
                            FROM {from_bundle_image_resolved}
                            COPY ./manifests /manifests
                            COPY ./metadata /metadata
                        """
                    )
                )
                for name, value in new_labels.items():
                    dockerfile.write(f'LABEL {name}={value}\n')

            for arch in sorted(arches):
                _build_image(temp_dir, 'Dockerfile', request_id, arch)
                _push_image(request_id, arch)

    set_request_state(request_id, 'in_progress', 'Creating the manifest list')
    output_pull_spec = _create_and_push_manifest_list(request_id, arches)

    conf = get_worker_config()
    if conf['iib_index_image_output_registry']:
        old_output_pull_spec = output_pull_spec
        output_pull_spec = output_pull_spec.replace(
            conf['iib_registry'], conf['iib_index_image_output_registry'], 1
        )
        log.info(
            'Changed the bundle_image pull specification from %s to %s',
            old_output_pull_spec,
            output_pull_spec,
        )

    payload = {
        'arches': list(arches),
        'bundle_image': output_pull_spec,
        'state': 'complete',
        'state_reason': 'The request completed successfully',
    }
    update_request(request_id, payload, exc_msg='Failed setting the bundle image on the request')


def _apply_package_name_suffix(metadata_path, package_name_suffix):
    """
    Add the package name suffix if configured for this organization.

    This adds the suffix to the value of
    ``annotations['operators.operatorframework.io.bundle.package.v1']`` in
    ``metadata/annotations.yaml``.

    The final package name value is returned as part of the tuple.

    :param str metadata_path: the path to the bundle's metadata directory.
    :param str package_name_suffix: the suffix to be added to the package name.
    :return: a tuple with the package name and a dictionary of labels to set on the bundle.
    :rtype: tuple(str, dict)
    """
    annotations_yaml = _get_package_annotations(metadata_path)
    package_label = 'operators.operatorframework.io.bundle.package.v1'
    package_annotation = annotations_yaml['annotations'][package_label]

    if package_annotation.endswith(package_name_suffix):
        log.debug('No modifications are needed on %s in metadata/annotations.yaml', package_label)
        return package_annotation, {}

    annotations_yaml['annotations'][package_label] = f'{package_annotation}{package_name_suffix}'

    with open(os.path.join(metadata_path, 'annotations.yaml'), 'w') as f:
        yaml.dump(annotations_yaml, f)

    log.info(
        'Modified %s in metadata/annotations.yaml from %s to %s',
        package_label,
        package_annotation,
        annotations_yaml['annotations'][package_label],
    )

    return (
        annotations_yaml['annotations'][package_label],
        {package_label: annotations_yaml['annotations'][package_label]},
    )


def _adjust_operator_bundle(manifests_path, metadata_path, organization=None, pinned_by_iib=False):
    """
    Apply modifications to the operator manifests at the given location.

    For any container image pull spec found in the Operator CSV files, replace floating
    tags with pinned digests, e.g. `image:latest` becomes `image@sha256:...`.

    If spec.relatedImages is not set, it will be set with the pinned digests. If it is set but
    there are also RELATED_IMAGE_* environment variables set, an exception will be raised.

    This method relies on the OperatorManifest class to properly identify and apply the
    modifications as needed.

    :param str manifests_path: the full path to the directory containing the operator manifests.
    :param str metadata_path: the full path to the directory containing the bundle metadata files.
    :param str organization: the organization this bundle is for. If no organization is provided,
        no custom behavior will be applied.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    :raises IIBError: if the operator manifest has invalid entries
    :return: a dictionary of labels to set on the bundle
    :rtype: dict
    """
    try:
        operator_manifest = OperatorManifest.from_directory(manifests_path)
    except (ruamel.yaml.YAMLError, ruamel.yaml.constructor.DuplicateKeyError) as e:
        error = f'The Operator Manifest is not in a valid YAML format: {e}'
        log.exception(error)
        raise IIBError(error)

    conf = get_worker_config()
    organization_customizations = conf['iib_organization_customizations'].get(organization, [])
    if not organization_customizations:
        organization_customizations = [
            {'type': 'package_name_suffix'},
            {'type': 'registry_replacements'},
            {'type': 'csv_annotations'},
        ]

    annotations_yaml = _get_package_annotations(metadata_path)
    package_name = annotations_yaml['annotations'][
        'operators.operatorframework.io.bundle.package.v1'
    ]
    labels = {}
    # Perform the customizations in order
    for customization in organization_customizations:
        customization_type = customization['type']
        if customization_type == 'package_name_suffix':
            package_name_suffix = customization.get('suffix')
            if package_name_suffix:
                log.info('Applying package_name_suffix : %s', package_name_suffix)
                package_name, labels = _apply_package_name_suffix(
                    metadata_path, package_name_suffix
                )
        elif customization_type == 'registry_replacements':
            registry_replacements = customization.get('replacements', {})
            log.info('Resolving image pull specs and applying registry replacements')
            bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
            _resolve_image_pull_specs(bundle_metadata, labels, pinned_by_iib, registry_replacements)
        elif customization_type == 'csv_annotations' and organization:
            org_csv_annotations = customization.get('annotations')
            if org_csv_annotations:
                log.info('Applying csv annotations for organization %s', organization)
                _adjust_csv_annotations(operator_manifest.files, package_name, org_csv_annotations)

    return labels


def _get_bundle_metadata(operator_manifest, pinned_by_iib):
    """
    Get bundle metadata i.e. CSV's and all relatedImages pull specifications.

    :param operator_manifest.operator.OperatorManifest operator_manifest: the operator manifest
        object.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    :raises IIBError: if the operator manifest has invalid entries
    :return: a dictionary of CSV's and relatedImages pull specifications
    :rtype: dict
    """
    bundle_metadata = {'found_pullspecs': set(), 'operator_csvs': []}
    for operator_csv in operator_manifest.files:
        if pinned_by_iib:
            # If the bundle image has already been previously pinned by IIB, the relatedImages
            # section will be populated and there may be related image environment variables.
            # However, we still want to process the image to apply any of the other possible
            # changes.
            log.info('Skipping pinning because related images have already been pinned by IIB')
        elif operator_csv.has_related_images():
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

        bundle_metadata['operator_csvs'].append(operator_csv)

        for pullspec in operator_csv.get_pullspecs():
            bundle_metadata['found_pullspecs'].add(pullspec)
    return bundle_metadata


def _get_package_annotations(metadata_path):
    """
    Get valid annotations yaml of the bundle.

    :param str metadata_path: the path to the bundle's metadata directory.
    :raises IIBError: if the annotations.yaml has invalid entries.
    :return: a dictionary of the bundle annotations.yaml file.
    :rtype: dict
    """
    annotations_yaml_path = os.path.join(metadata_path, 'annotations.yaml')
    if not os.path.exists(annotations_yaml_path):
        raise IIBError('metadata/annotations.yaml does not exist in the bundle')

    with open(annotations_yaml_path, 'r') as f:
        try:
            annotations_yaml = yaml.load(f)
        except ruamel.yaml.YAMLError:
            error = 'metadata/annotations.yaml is not valid YAML'
            log.exception(error)
            raise IIBError(error)

    if not isinstance(annotations_yaml.get('annotations', {}), dict):
        raise IIBError('The value of metadata/annotations.yaml must be a dictionary')

    package_label = 'operators.operatorframework.io.bundle.package.v1'
    package_annotation = annotations_yaml.get('annotations', {}).get(package_label)
    if not package_annotation:
        raise IIBError(f'{package_label} is not set in metadata/annotations.yaml')

    if not isinstance(package_annotation, str):
        raise IIBError(f'The value of {package_label} in metadata/annotations.yaml is not a string')

    return annotations_yaml


def _resolve_image_pull_specs(bundle_metadata, labels, pinned_by_iib, registry_replacements):
    """
    Resolve image pull specifications to container image digests.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :param dict labels: the dictionary of labels to be set on the bundle image
    :param str registry_replacements: the customization dictionary which specifies replacement of
        registry in the pull specifications.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    """
    # Resolve pull specs to container image digests
    replacement_pullspecs = {}
    for pullspec in bundle_metadata['found_pullspecs']:
        replacement_needed = False
        new_pullspec = ImageName.parse(pullspec.to_str())

        if not pinned_by_iib:
            # Resolve the image only if it has not already been processed by IIB. This
            # helps making sure the pullspec is valid
            resolved_image = ImageName.parse(get_resolved_image(pullspec.to_str()))

            # If the tag is in the format "<algorithm>:<checksum>", the image is already pinned.
            # Otherwise, always pin it to a digest.
            if ':' not in ImageName.parse(pullspec).tag:
                log.debug('%s will be pinned to %s', pullspec, resolved_image.to_str())
                new_pullspec = resolved_image
                replacement_needed = True
                labels['com.redhat.iib.pinned'] = 'true'

        # Apply registry modifications
        new_registry = registry_replacements.get(new_pullspec.registry)
        if new_registry:
            replacement_needed = True
            new_pullspec.registry = new_registry

        if replacement_needed:
            log.debug('%s will be replaced with %s', pullspec, new_pullspec.to_str())
            replacement_pullspecs[pullspec] = new_pullspec

    # Apply modifications to the operator bundle image metadata
    for operator_csv in bundle_metadata['operator_csvs']:
        csv_file_name = os.path.basename(operator_csv.path)
        log.info('Replacing the pull specifications on %s', csv_file_name)
        operator_csv.replace_pullspecs_everywhere(replacement_pullspecs)

        log.info('Setting spec.relatedImages on %s', csv_file_name)
        operator_csv.set_related_images()

        operator_csv.dump()


def _adjust_csv_annotations(operator_csvs, package_name, org_csv_annotations):
    """
    Annotate ClusterServiceVersion objects based on an organization configuration.

    :param list operator_csvs: the list of ``OperatorCSV`` objects to examine.
    :param str package_name: the operator package name.
    :param str org_csv_annotations: the dict of annotations customization for an organization.
    """
    for operator_csv in operator_csvs:
        log.debug(
            'Processing the ClusterServiceVersion file %s', os.path.basename(operator_csv.path)
        )
        csv_annotations = operator_csv.data.setdefault('metadata', {}).setdefault('annotations', {})
        for annotation, value_template in org_csv_annotations.items():
            value = value_template.format(package_name=package_name)
            csv_annotations[annotation] = value

        operator_csv.dump()
