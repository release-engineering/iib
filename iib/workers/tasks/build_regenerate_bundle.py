# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import os
import tempfile
import textwrap
from typing import Any, Dict, List, Optional, Set, Tuple

from operator_manifest.operator import ImageName, OperatorManifest, OperatorCSV
import ruamel.yaml

from iib.exceptions import IIBError
from iib.workers.s3_utils import upload_file_to_s3_bucket
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
    get_image_labels,
    get_resolved_image,
    podman_pull,
    request_logger,
    set_registry_auths,
    get_image_arches,
)
from iib.workers.tasks.iib_static_types import BundleMetadata, UpdateRequestPayload


__all__ = ['handle_regenerate_bundle_request']

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
def handle_regenerate_bundle_request(
    from_bundle_image: str,
    organization: str,
    request_id: int,
    registry_auths: Optional[Dict[str, Any]] = None,
    bundle_replacements: Optional[Dict[str, str]] = {},
) -> None:
    """
    Coordinate the work needed to regenerate the operator bundle image.

    :param str from_bundle_image: the pull specification of the bundle image to be regenerated.
    :param str organization: the name of the organization the bundle should be regenerated for.
    :param int request_id: the ID of the IIB build request.
    :param dict registry_auths: Provide the dockerconfig.json for authentication to private
      registries, defaults to ``None``.
    :param dict bundle_replacements: Dictionary mapping from original bundle pullspecs to rebuilt
      bundle pullspecs.
    :raises IIBError: if the regenerate bundle image build fails.
    """
    _cleanup()

    set_request_state(request_id, 'in_progress', 'Resolving from_bundle_image')

    with set_registry_auths(registry_auths):
        from_bundle_image_resolved = get_resolved_image(from_bundle_image)

        arches: Set[str] = get_image_arches(from_bundle_image_resolved)
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

        payload: UpdateRequestPayload = {
            'from_bundle_image_resolved': from_bundle_image_resolved,
            'state': 'in_progress',
            'state_reason': f'Regenerating the bundle image for the following arches: {arches_str}',
        }
        exc_msg = 'Failed setting the resolved "from_bundle_image" on the request'
        update_request(request_id, payload, exc_msg=exc_msg)

        # Pull the from_bundle_image to ensure steps later on don't fail due to registry timeouts
        podman_pull(from_bundle_image_resolved)

        with tempfile.TemporaryDirectory(prefix=f'iib-{request_id}-') as temp_dir:
            manifests_path = os.path.join(temp_dir, 'manifests')
            _copy_files_from_image(from_bundle_image_resolved, '/manifests', manifests_path)
            metadata_path = os.path.join(temp_dir, 'metadata')
            _copy_files_from_image(from_bundle_image_resolved, '/metadata', metadata_path)
            new_labels = _adjust_operator_bundle(
                manifests_path,
                metadata_path,
                request_id,
                organization=organization,
                pinned_by_iib=pinned_by_iib,
                bundle_replacements=bundle_replacements,
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
    output_pull_spec = _create_and_push_manifest_list(request_id, arches, [])

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
    _cleanup()
    update_request(request_id, payload, exc_msg='Failed setting the bundle image on the request')


def _apply_package_name_suffix(
    metadata_path: str,
    package_name_suffix: str,
) -> Tuple[str, Dict[str, str]]:
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


def _adjust_operator_bundle(
    manifests_path: str,
    metadata_path: str,
    request_id: int,
    organization: Optional[str] = None,
    pinned_by_iib: bool = False,
    recursive_related_bundles: bool = False,
    bundle_replacements: Optional[Dict[str, str]] = {},
) -> Dict[str, str]:
    """
    Apply modifications to the operator manifests at the given location.

    For any container image pull spec found in the Operator CSV files, replace floating
    tags with pinned digests, e.g. `image:latest` becomes `image@sha256:...`.

    If spec.relatedImages is not set, it will be set with the pinned digests. If it is set but
    there are also RELATED_IMAGE_* environment variables set, the relatedImages will be regenerated
    and the digests will be pinned again.

    This method relies on the OperatorManifest class to properly identify and apply the
    modifications as needed.

    :param str manifests_path: the full path to the directory containing the operator manifests.
    :param str metadata_path: the full path to the directory containing the bundle metadata files.
    :param int request_id: the ID of the IIB build request.
    :param str organization: the organization this bundle is for. If no organization is provided,
        no custom behavior will be applied.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    :param bool recursive_related_bundles: whether or not the call is from a
        recursive_related_bundles request.
    :param dict bundle_replacements: mapping between original pullspecs and rebuilt bundles,
        allowing the updating of digests if any bundles have been rebuilt.
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
            {'type': 'resolve_image_pullspecs'},
            {'type': 'related_bundles'},
            {'type': 'package_name_suffix'},
            {'type': 'registry_replacements'},
            {'type': 'perform_bundle_replacements'},
            {'type': 'image_name_from_labels'},
            {'type': 'csv_annotations'},
            {'type': 'enclose_repo'},
        ]

    annotations_yaml = _get_package_annotations(metadata_path)
    package_name = annotations_yaml['annotations'][
        'operators.operatorframework.io.bundle.package.v1'
    ]
    labels: Dict[str, str] = {}

    # Perform the customizations in order
    for customization in organization_customizations:
        customization_type = customization['type']
        if customization_type == 'package_name_suffix':
            package_name_suffix = customization.get('suffix')
            if package_name_suffix:
                log.info('Applying package_name_suffix : %s', package_name_suffix)
                package_name, package_labels = _apply_package_name_suffix(
                    metadata_path, package_name_suffix
                )
                labels = {**labels, **package_labels}
        elif customization_type == 'registry_replacements':
            registry_replacements = customization.get('replacements', {})
            if registry_replacements:
                log.info('Applying registry replacements')
                bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
                _apply_registry_replacements(bundle_metadata, registry_replacements)
        elif customization_type == 'csv_annotations' and organization:
            org_csv_annotations = customization.get('annotations')
            if org_csv_annotations:
                log.info('Applying csv annotations for organization %s', organization)
                _adjust_csv_annotations(operator_manifest.files, package_name, org_csv_annotations)
        elif customization_type == 'image_name_from_labels':
            org_image_name_template = customization.get('template', '')
            if org_image_name_template:
                bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
                _replace_image_name_from_labels(bundle_metadata, org_image_name_template)
        elif customization_type == 'enclose_repo':
            org_enclose_repo_namespace = customization.get('namespace')
            org_enclose_repo_glue = customization.get('enclosure_glue')
            if org_enclose_repo_namespace and org_enclose_repo_glue:
                log.info(
                    'Applying enclose_repo customization with namespace %s and enclosure_glue %s'
                    ' for organizaton %s',
                    org_enclose_repo_namespace,
                    org_enclose_repo_glue,
                    organization,
                )
                bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
                _apply_repo_enclosure(
                    bundle_metadata, org_enclose_repo_namespace, org_enclose_repo_glue
                )
        elif customization_type == 'related_bundles':
            # When recursive_related_bundles is set to True, the call is from a
            # recureive_related_bundles request. Product teams have customizations
            # in the order so that when this customization is specified, the bundles
            # are accessible. For the recursive_related_bundles request, we want the
            # the images to be in an accessible state. So apply all customizations
            # that were specified before this one and return.
            if recursive_related_bundles:
                log.debug(
                    'Stopping before performing `related_bundles` modification since '
                    'recursive_related_bundles set to True. No further '
                    'customizations will be performed for organization '
                    f'{organization}. Finding recursive_related_bundles'
                )
                return labels
            log.info('Applying related_bundles customization')
            bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
            related_bundle_images = get_related_bundle_images(bundle_metadata=bundle_metadata)
            write_related_bundles_file(
                related_bundle_images,
                request_id,
                conf['iib_request_related_bundles_dir'],
                'related_bundles',
            )
        elif customization_type == 'resolve_image_pullspecs':
            log.info('Resolving image pull specs')
            bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
            _resolve_image_pull_specs(bundle_metadata, labels, pinned_by_iib)
        elif customization_type == 'perform_bundle_replacements':
            log.info('Performing bundle replacements')
            bundle_metadata = _get_bundle_metadata(operator_manifest, pinned_by_iib)
            replacement_pullspecs = {}
            if bundle_replacements:
                for old, new in bundle_replacements.items():
                    if _is_bundle_image(old):
                        replacement_pullspecs[ImageName.parse(old)] = ImageName.parse(new)
                _replace_csv_pullspecs(bundle_metadata, replacement_pullspecs)

    return labels


def _get_bundle_metadata(
    operator_manifest: OperatorManifest,
    pinned_by_iib: bool,
) -> BundleMetadata:
    """
    Get bundle metadata i.e. CSV's and all relatedImages pull specifications.

    If the bundle is already pinned by IIB, it will be pinned again and the relatedImages will
    be regenerated.

    :param operator_manifest.operator.OperatorManifest operator_manifest: the operator manifest
        object.
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    :raises IIBError: if the operator manifest has invalid entries
    :return: a dictionary of CSV's and relatedImages pull specifications
    :rtype: dict
    """
    bundle_metadata: BundleMetadata = {'found_pullspecs': set(), 'operator_csvs': []}
    for operator_csv in operator_manifest.files:
        if pinned_by_iib:
            # If the bundle image has already been previously pinned by IIB, the relatedImages
            # section will be populated and there may be related image environment variables.
            # This behavior is now valid and the images will be pinned again and the relatedImages
            # will be regenerated.
            log.info(
                'Bundle has been pinned by IIB. '
                'Pinning will be done again and relatedImages will be regenerated'
            )

        bundle_metadata['operator_csvs'].append(operator_csv)

        for pullspec in operator_csv.get_pullspecs():
            bundle_metadata['found_pullspecs'].add(pullspec)
    return bundle_metadata


def _get_package_annotations(metadata_path: str) -> Dict[str, Any]:
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


def _resolve_image_pull_specs(
    bundle_metadata: BundleMetadata,
    labels: Dict[str, str],
    pinned_by_iib: bool,
) -> None:
    """
    Resolve image pull specifications to container image digests.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :param dict labels: the dictionary of labels to be set on the bundle image
    :param bool pinned_by_iib: whether or not the bundle image has already been processed by
        IIB to perform image pinning of related images.
    """
    # Resolve pull specs to container image digests
    replacement_pullspecs = {}
    for pullspec in bundle_metadata['found_pullspecs']:
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
                labels['com.redhat.iib.pinned'] = 'true'
                replacement_pullspecs[pullspec] = new_pullspec

    if replacement_pullspecs:
        _replace_csv_pullspecs(bundle_metadata, replacement_pullspecs)


def _apply_registry_replacements(
    bundle_metadata: BundleMetadata,
    registry_replacements: Dict[str, Any],
) -> None:
    """
    Apply registry replacements from the config customizations.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :param dict registry_replacements: the customization dictionary which specifies replacement of
        registry in the pull specifications.
    """
    replacement_pullspecs = {}
    for pullspec in bundle_metadata['found_pullspecs']:
        new_pullspec = ImageName.parse(pullspec.to_str())
        # Apply registry modifications
        new_registry = registry_replacements.get(new_pullspec.registry)
        if new_registry:
            new_pullspec.registry = new_registry
            replacement_pullspecs[pullspec] = new_pullspec

    if replacement_pullspecs:
        _replace_csv_pullspecs(bundle_metadata, replacement_pullspecs)


def _replace_image_name_from_labels(
    bundle_metadata: BundleMetadata,
    replacement_template: str,
) -> None:
    """
    Replace repo/image-name in the CSV pull specs with values from their labels.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :param str replacement_template: the template specifying which label values to use for
        replacement
    """
    replacement_pullspecs = {}

    for pullspec in bundle_metadata['found_pullspecs']:
        new_pullspec = ImageName.parse(pullspec.to_str())
        pullspec_labels = get_image_labels(pullspec.to_str())
        try:
            modified_namespace_repo = replacement_template.format(**pullspec_labels)
        except KeyError:
            raise IIBError(
                f'Pull spec {pullspec.to_str()} is missing one or more label(s)'
                f' required in the image_name_from_labels {replacement_template}.'
                f' Available labels: {", ".join(list(pullspec_labels.keys()))}'
            )

        namespace_repo_list: List[Optional[str]] = []
        namespace_repo_list.extend(modified_namespace_repo.split('/', 1))
        if len(namespace_repo_list) == 1:
            namespace_repo_list.insert(0, None)

        new_pullspec.namespace, new_pullspec.repo = namespace_repo_list
        replacement_pullspecs[pullspec] = new_pullspec

    # Related images have already been set when resolving pull_specs.
    _replace_csv_pullspecs(bundle_metadata, replacement_pullspecs)


def _replace_csv_pullspecs(
    bundle_metadata: BundleMetadata,
    replacement_pullspecs: Dict[ImageName, ImageName],
) -> None:
    """
    Replace pull specs in operator CSV files.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :param dict replacement_pullspecs: the dictionary mapping existing pull specs to the new
        pull specs that will replace the existing pull specs in the operator CSVs.
    """
    # Log the pullspecs replacement
    for old_pullspec, new_pullspec in replacement_pullspecs.items():
        log.debug(
            '%s will be replaced with %s in the bundle CSVs',
            old_pullspec,
            new_pullspec.to_str(),
        )

    # Apply modifications to the operator bundle image metadata
    for operator_csv in bundle_metadata['operator_csvs']:
        csv_file_name = os.path.basename(operator_csv.path)
        log.info('Replacing the pull specifications on %s', csv_file_name)
        operator_csv.replace_pullspecs_everywhere(replacement_pullspecs)

        # Only set related images if they haven't been set already. Or else the OperatorManifest
        # library will set it twice instead of replacing the old one. It's not required to be
        # called everytime because replace_pullspecs_everywhere replaces the pull_specs even
        # in the relatedImages part if they are set.
        if not operator_csv.has_related_images():
            log.info('Setting spec.relatedImages on %s', csv_file_name)
            operator_csv.set_related_images()

        operator_csv.dump()


def _adjust_csv_annotations(
    operator_csvs: List[OperatorCSV],
    package_name: str,
    org_csv_annotations: Dict[str, Any],
) -> None:
    """
    Annotate ClusterServiceVersion objects based on an organization configuration.

    :param list operator_csvs: the list of ``OperatorCSV`` objects to examine.
    :param str package_name: the operator package name.
    :param dict org_csv_annotations: the dict of annotations customization for an organization.
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


def _apply_repo_enclosure(
    bundle_metadata: BundleMetadata,
    org_enclose_repo_namespace: str,
    org_enclose_repo_glue: str,
) -> None:
    """
    Apply repo_enclosure customization to the bundle image.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :param str org_enclose_repo_namespace: the string sprecifying the namespace of the modified
        pull specs in the CSV files
    :param str org_enclose_repo_glue: the string specifying the enclosure glue to be applied
        to modify the pull specs in the CSV files
    """
    replacement_pullspecs = {}
    for pullspec in bundle_metadata['found_pullspecs']:
        new_pullspec = ImageName.parse(pullspec.to_str())

        repo_parts = new_pullspec.repo.split('/')
        if new_pullspec.namespace and org_enclose_repo_namespace != new_pullspec.namespace:
            repo_parts.insert(0, new_pullspec.namespace)

        new_pullspec.namespace = org_enclose_repo_namespace
        new_pullspec.repo = org_enclose_repo_glue.join(repo_parts)
        replacement_pullspecs[pullspec] = new_pullspec

    _replace_csv_pullspecs(bundle_metadata, replacement_pullspecs)


def get_related_bundle_images(bundle_metadata: BundleMetadata) -> List[str]:
    """
    Get related bundle images from bundle metadata.

    :param dict bundle_metadata: the dictionary of CSV's and relatedImages pull specifications
    :rtype: list
    :return: a list of related bundles
    """
    related_bundle_images = []
    for related_pullspec_obj in bundle_metadata['found_pullspecs']:
        related_pullspec = related_pullspec_obj.to_str()
        if _is_bundle_image(related_pullspec):
            related_bundle_images.append(related_pullspec)
    return related_bundle_images


def _is_bundle_image(image_pullspec: str) -> bool:
    """
    Determine whether a specific image pullspec is for a bundle image.

    :param str image_pullspec: the string of the image pullspec to test
    :rtype: bool
    :return: whether the image is considered a bundle image
    """
    return yaml.load(
        get_image_label(image_pullspec, 'com.redhat.delivery.operator.bundle') or 'false'
    )


def write_related_bundles_file(
    related_bundle_images: List[str], request_id: int, local_directory: str, s3_file_identifier: str
) -> None:
    """
    Get bundle images in the CSV files of the bundle being regenerated and store them in a file.

    :param list related_bundle_images: the list of pull specifications of related bundle images
    :param int request_id: the ID of the IIB build request
    :param str local_directory: the directory in which the file should be stored locally
    :param str s3_file_identifier: the identifier to be used for sub-directory and file name on the
        s3 bucket.
    """
    worker_config = get_worker_config()
    related_bundles_file = os.path.join(local_directory, f'{request_id}_{s3_file_identifier}.json')

    log.debug('Writing related bundle images %s to %s', related_bundle_images, related_bundles_file)
    with open(related_bundles_file, 'w') as output_file:
        json.dump(related_bundle_images, output_file)

    if worker_config['iib_aws_s3_bucket_name']:
        upload_file_to_s3_bucket(
            related_bundles_file, s3_file_identifier, f'{request_id}_{s3_file_identifier}.json'
        )
