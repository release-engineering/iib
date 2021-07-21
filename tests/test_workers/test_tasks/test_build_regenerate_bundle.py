# SPDX-License-Identifier: GPL-3.0-or-later
import textwrap
from unittest import mock
from unittest.mock import call, MagicMock

from operator_manifest.operator import OperatorManifest
import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_regenerate_bundle


# Re-use the yaml instance to ensure configuration is also used in tests
yaml = build_regenerate_bundle.yaml


@pytest.mark.parametrize(
    'pinned_by_iib_label, pinned_by_iib_bool',
    (('true', True), ('True', True), (None, False), ('false', False), ('False', False)),
)
@pytest.mark.parametrize(
    'iib_index_image_output_registry, expected_bundle_image',
    ((None, 'quay.io/iib:99'), ('dagobah.domain.local', 'dagobah.domain.local/iib:99')),
)
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_label')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._cleanup')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.podman_pull')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_arches')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._copy_files_from_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._build_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._push_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.set_request_state')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_worker_config')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.update_request')
def test_handle_regenerate_bundle_request(
    mock_ur,
    mock_gwc,
    mock_capml,
    mock_srs,
    mock_pi,
    mock_bi,
    mock_aob,
    mock_cffi,
    mock_gia,
    mock_temp_dir,
    mock_pp,
    mock_gri,
    mock_cleanup,
    mock_gil,
    iib_index_image_output_registry,
    expected_bundle_image,
    pinned_by_iib_label,
    pinned_by_iib_bool,
    tmpdir,
):
    arches = ['amd64', 's390x']
    from_bundle_image = 'bundle-image:latest'
    from_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    bundle_image = 'quay.io/iib:99'
    organization = 'acme'
    request_id = 99

    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)
    mock_gri.return_value = from_bundle_image_resolved
    mock_gia.return_value = list(arches)
    mock_aob.return_value = {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'}
    mock_capml.return_value = bundle_image
    mock_gwc.return_value = {
        'iib_index_image_output_registry': iib_index_image_output_registry,
        'iib_registry': 'quay.io',
    }
    mock_gil.return_value = pinned_by_iib_label

    build_regenerate_bundle.handle_regenerate_bundle_request(
        from_bundle_image, organization, request_id
    )

    mock_cleanup.assert_called_once()

    mock_gri.assert_called_once()
    mock_gri.assert_called_with('bundle-image:latest')

    mock_pp.assert_called_once_with(from_bundle_image_resolved)

    mock_gia.assert_called_once()
    mock_gia.assert_called_with('bundle-image@sha256:abcdef')

    assert mock_cffi.call_count == 2
    mock_cffi.assert_has_calls(
        (
            mock.call('bundle-image@sha256:abcdef', '/manifests', mock.ANY),
            mock.call('bundle-image@sha256:abcdef', '/metadata', mock.ANY),
        )
    )

    mock_aob.assert_called_once_with(
        str(tmpdir.join('manifests')), str(tmpdir.join('metadata')), 'acme', pinned_by_iib_bool
    )

    assert mock_bi.call_count == len(arches)
    assert mock_pi.call_count == len(arches)
    for arch in arches:
        mock_bi.assert_any_call(mock.ANY, 'Dockerfile', request_id, arch)
        mock_pi.assert_any_call(request_id, arch)

    assert mock_srs.call_count == 2
    mock_srs.assert_has_calls(
        [
            mock.call(request_id, 'in_progress', 'Resolving from_bundle_image'),
            mock.call(request_id, 'in_progress', 'Creating the manifest list'),
        ]
    )

    mock_capml.assert_called_once_with(request_id, list(arches))

    assert mock_ur.call_count == 2
    mock_ur.assert_has_calls(
        [
            mock.call(
                request_id,
                {
                    'from_bundle_image_resolved': from_bundle_image_resolved,
                    'state': 'in_progress',
                    'state_reason': (
                        'Regenerating the bundle image for the following arches: amd64, s390x'
                    ),
                },
                exc_msg='Failed setting the resolved "from_bundle_image" on the request',
            ),
            mock.call(
                request_id,
                {
                    'arches': list(arches),
                    'bundle_image': expected_bundle_image,
                    'state': 'complete',
                    'state_reason': 'The request completed successfully',
                },
                exc_msg='Failed setting the bundle image on the request',
            ),
        ]
    )

    with open(tmpdir.join('Dockerfile'), 'r') as f:
        dockerfile = f.read()

    expected_dockerfile = textwrap.dedent(
        '''\
        FROM bundle-image@sha256:abcdef
        COPY ./manifests /manifests
        COPY ./metadata /metadata
        LABEL operators.operatorframework.io.bundle.package.v1=amqstreams-cmp
        '''
    )

    assert dockerfile == expected_dockerfile


@mock.patch('iib.workers.tasks.build_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._apply_package_name_suffix')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._adjust_csv_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_labels')
def test_adjust_operator_bundle_unordered(
    mock_gil, mock_aca, mock_gri, mock_apns, mock_gpa, tmpdir
):
    manager = MagicMock()
    manager.attach_mock(mock_gpa, 'mock_gpa')
    manager.attach_mock(mock_apns, 'mock_apns')
    manager.attach_mock(mock_gri, 'mock_gri')
    manager.attach_mock(mock_aca, 'mock_aca')
    manager.attach_mock(mock_gil, 'mock_gil')

    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'amqstreams'}
    }
    mock_apns.return_value = (
        'amqstreams',
        {},
    )
    manifests_dir = tmpdir.mkdir('manifests')
    metadata_dir = tmpdir.mkdir('metadata')
    csv1 = manifests_dir.join('1.clusterserviceversion.yaml')
    csv2 = manifests_dir.join('2.clusterserviceversion.yaml')
    csv3 = manifests_dir.join('3.clusterserviceversion.yaml')

    # NOTE: The OperatorManifest class is capable of modifying pull specs found in
    # various locations within the CSV file. Since IIB relies on this class to do
    # such modifications, this test only verifies that at least one of the locations
    # is being handled properly. This is to ensure IIB is using OperatorManifest
    # correctly.
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/operator/image{ref}
        """
    )
    image_digest = '654321'
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/operator/image{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            ref=':v1',
            related_name=f'image-{image_digest}-annotation',
            related_ref='@sha256:749327',
        )
    )
    csv2.write(csv_template.format(registry='quay.io', ref='@sha256:654321'))
    csv3.write(csv_template.format(registry='registry.access.company.com', ref=':v2'))

    def get_resolved_image(image):
        return {
            'quay.io/operator/image:v2': 'quay.io/operator/image@sha256:654321',
            'quay.io/operator/image@sha256:654321': 'quay.io/operator/image@sha256:654321',
            'registry.access.company.com/operator/image:v2': (
                'registry.access.company.com/operator/image@sha256:654321'
            ),
        }[image]

    mock_gri.side_effect = get_resolved_image

    labels = build_regenerate_bundle._adjust_operator_bundle(
        str(manifests_dir), str(metadata_dir), 'company-unknown'
    )

    assert labels == {
        'com.redhat.iib.pinned': 'true',
    }
    # Verify that the relatedImages are not modified if they were already set and that images were
    # not pinned
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        ref=':v1',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:749327',
    )
    assert csv2.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        ref='@sha256:654321',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    assert csv3.read_text('utf-8') == csv_related_images_template.format(
        registry='registry.access.company.com',
        ref='@sha256:654321',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    mock_aca.assert_not_called()
    mock_apns.assert_not_called()
    mock_gil.assert_not_called()

    expected_calls = [
        call.mock_gpa(mock.ANY),
        call.mock_gri(mock.ANY),
        call.mock_gri(mock.ANY),
    ]
    assert manager.mock_calls == expected_calls


@mock.patch('iib.workers.tasks.build_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._apply_package_name_suffix')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._adjust_csv_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_labels')
def test_adjust_operator_bundle_ordered(mock_gil, mock_aca, mock_gri, mock_apns, mock_gpa, tmpdir):
    manager = MagicMock()
    manager.attach_mock(mock_gpa, 'mock_gpa')
    manager.attach_mock(mock_apns, 'mock_apns')
    manager.attach_mock(mock_gri, 'mock_gri')
    manager.attach_mock(mock_aca, 'mock_aca')
    manager.attach_mock(mock_gil, 'mock_gil')

    annotations = {
        'marketplace.company.io/remote-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
        ),
        'marketplace.company.io/support-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/support'
        ),
    }

    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'amqstreams'}
    }
    mock_apns.return_value = (
        'amqstreams',
        {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'},
    )
    mock_gil.return_value = {'name': 'namespace/reponame', 'version': 'rhel-8'}
    manifests_dir = tmpdir.mkdir('manifests')
    metadata_dir = tmpdir.mkdir('metadata')
    csv1 = manifests_dir.join('1.clusterserviceversion.yaml')
    csv2 = manifests_dir.join('2.clusterserviceversion.yaml')
    csv3 = manifests_dir.join('3.clusterserviceversion.yaml')

    # NOTE: The OperatorManifest class is capable of modifying pull specs found in
    # various locations within the CSV file. Since IIB relies on this class to do
    # such modifications, this test only verifies that at least one of the locations
    # is being handled properly. This is to ensure IIB is using OperatorManifest
    # correctly.
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/{operator}{image}{ref}
        """
    )
    image_digest = '654321'
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/{operator}{image}{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            operator='operator',
            image='/image',
            ref=':v1',
            related_name=f'image-{image_digest}-annotation',
            related_ref='@sha256:749327',
        )
    )
    csv2.write(
        csv_template.format(
            registry='quay.io', operator='operator', image='/image', ref='@sha256:654321'
        )
    )
    csv3.write(
        csv_template.format(
            registry='registry.access.company.com', operator='operator', image='/image', ref=':v2'
        )
    )

    def get_resolved_image(image):
        return {
            'quay.io/operator/imagel:v2': 'quay.io/operator/image@sha256:654321',
            'quay.io/operator/image@sha256:654321': 'quay.io/operator/image@sha256:654321',
            'registry.access.company.com/operator/image:v2': (
                'registry.access.company.com/operator/image@sha256:654321'
            ),
        }[image]

    mock_gri.side_effect = get_resolved_image

    labels = build_regenerate_bundle._adjust_operator_bundle(
        str(manifests_dir), str(metadata_dir), 'company-marketplace'
    )

    assert labels == {
        'com.redhat.iib.pinned': 'true',
        'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp',
    }
    # Verify that the relatedImages are not modified if they were already set and that images were
    # not pinned
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        operator='namespace/reponame',
        image='-rhel-8-final',
        ref=':v1',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:749327',
    )
    assert csv2.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        operator='namespace/reponame',
        image='-rhel-8-final',
        ref='@sha256:654321',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    assert csv3.read_text('utf-8') == csv_related_images_template.format(
        registry='registry.marketplace.company.com',
        operator='namespace/reponame',
        image='-rhel-8-final',
        ref='@sha256:654321',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    mock_aca.assert_called_once_with(mock.ANY, 'amqstreams', annotations)
    assert mock_gil.call_count == 4

    expected_calls = [
        call.mock_gpa(mock.ANY),
        call.mock_gri(mock.ANY),
        call.mock_gri(mock.ANY),
        call.mock_aca(mock.ANY, 'amqstreams', annotations),
        call.mock_apns(mock.ANY, '-cmp'),
        call.mock_gil(mock.ANY),
        call.mock_gil(mock.ANY),
        call.mock_gil(mock.ANY),
        call.mock_gil(mock.ANY),
    ]
    assert manager.mock_calls == expected_calls


@mock.patch('iib.workers.tasks.build_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._apply_package_name_suffix')
def test_adjust_operator_bundle_invalid_related_images(mock_apns, mock_gpa, tmpdir):
    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'amqstreams'}
    }
    mock_apns.return_value = ('amqstreams', {})
    manifests_dir = tmpdir.mkdir('manifests')
    metadata_dir = tmpdir.mkdir('metadata')
    csv = manifests_dir.join('csv.yaml')
    csv.write(
        textwrap.dedent(
            """\
            apiVersion: operators.example.com/v1
            kind: ClusterServiceVersion
            metadata:
              name: amqstreams.v1.0.0
              namespace: placeholder
              annotations:
                containerImage: quay.io/operator/image:v1
            spec:
              install:
                spec:
                  deployments:
                  - spec:
                      template:
                        spec:
                          containers:
                          - name: image-annotation
                            image: quay.io/operator/image:v1
                            env:
                            - name: RELATED_IMAGE_SOMETHING
                              value: quay.io/operator/image@sha256:749327
              relatedImages:
              - name: image-annotation
                image: quay.io/operator/image@sha256:749327
            """
        )
    )

    expected = (
        r'The ClusterServiceVersion file csv.yaml has entries in spec.relatedImages and one or '
        r'more containers have RELATED_IMAGE_\* environment variables set. This is not allowed for '
        r'bundles regenerated with IIB.'
    )
    with pytest.raises(IIBError, match=expected):
        build_regenerate_bundle._adjust_operator_bundle(str(manifests_dir), str(metadata_dir))


@mock.patch('iib.workers.tasks.build_regenerate_bundle._apply_package_name_suffix')
def test_adjust_operator_bundle_invalid_yaml_file(mock_apns, tmpdir):
    mock_apns.return_value = ('amqstreams', {})
    manifests_dir = tmpdir.mkdir('manifests')
    metadata_dir = tmpdir.mkdir('metadata')
    csv = manifests_dir.join('csv.yaml')
    csv.write(
        textwrap.dedent(
            """\
            apiVersion: operators.example.com/v1
            kind: ClusterServiceVersion
            metadata:
              @\n name: amqstreams.v1.0.0
            """
        )
    )

    expected = r'The Operator Manifest is not in a valid YAML format'

    with pytest.raises(IIBError, match=expected):
        build_regenerate_bundle._adjust_operator_bundle(str(manifests_dir), str(metadata_dir))


@mock.patch('iib.workers.tasks.build_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._apply_package_name_suffix')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._adjust_csv_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_labels')
def test_adjust_operator_bundle_already_pinned_by_iib(
    mock_gil, mock_aca, mock_gri, mock_apns, mock_gpa, tmpdir
):
    annotations = {
        'marketplace.company.io/remote-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
        ),
        'marketplace.company.io/support-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/support'
        ),
    }
    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'amqstreams'}
    }
    mock_apns.return_value = (
        'amqstreams',
        {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'},
    )
    mock_gil.return_value = {'name': 'namespace/reponame', 'version': 'rhel-8'}
    manifests_dir = tmpdir.mkdir('manifests')
    metadata_dir = tmpdir.mkdir('metadata')
    csv1 = manifests_dir.join('2.clusterserviceversion.yaml')
    csv2 = manifests_dir.join('3.clusterserviceversion.yaml')

    # NOTE: The OperatorManifest class is capable of modifying pull specs found in
    # various locations within the CSV file. Since IIB relies on this class to do
    # such modifications, this test only verifies that at least one of the locations
    # is being handled properly. This is to ensure IIB is using OperatorManifest
    # correctly.
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/{operator}{image}{ref}
        """
    )
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/{operator}{image}{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            ref='@sha256:654321',
            related_name='image-654321-annotation',
            related_ref='@sha256:654321',
            operator='operator',
            image='/image',
        )
    )
    csv2.write(
        csv_related_images_template.format(
            # This registry for the company-marketplace will be replaced based on
            # worker configuration.
            registry='registry.access.company.com',
            ref='@sha256:765432',
            related_name=f'operator/image-765432-annotation',
            related_ref='@sha256:765432',
            operator='operator',
            image='/image',
        )
    )

    labels = build_regenerate_bundle._adjust_operator_bundle(
        str(manifests_dir), str(metadata_dir), 'company-marketplace', pinned_by_iib=True
    )

    # The com.redhat.iib.pinned label is not explicitly set, but inherited from the original image
    assert labels == {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'}
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        ref='@sha256:654321',
        related_name=f'image-654321-annotation',
        related_ref='@sha256:654321',
        operator='namespace/reponame',
        image='-rhel-8-final',
    )
    assert csv2.read_text('utf-8') == csv_related_images_template.format(
        registry='registry.marketplace.company.com',
        ref='@sha256:765432',
        related_name=f'operator/image-765432-annotation',
        related_ref='@sha256:765432',
        operator='namespace/reponame',
        image='-rhel-8-final',
    )
    mock_aca.assert_called_once_with(mock.ANY, 'amqstreams', annotations)
    mock_gri.assert_not_called()


@pytest.mark.parametrize(
    'package_name_suffix, package, expected_package, expected_labels',
    (
        (
            '-cmp',
            'amq-streams',
            'amq-streams-cmp',
            {'operators.operatorframework.io.bundle.package.v1': 'amq-streams-cmp'},
        ),
        ('-cmp', 'amq-streams-cmp', 'amq-streams-cmp', {}),
    ),
)
def test_apply_package_name_suffix(
    package_name_suffix, package, expected_package, expected_labels, tmpdir
):
    metadata_dir = tmpdir.mkdir('metadata')
    annotations_yaml = metadata_dir.join('annotations.yaml')
    annotations_yaml.write(
        textwrap.dedent(
            f'''\
            annotations:
              operators.operatorframework.io.bundle.channel.default.v1: stable
              operators.operatorframework.io.bundle.channels.v1: stable
              operators.operatorframework.io.bundle.manifests.v1: manifests/
              operators.operatorframework.io.bundle.mediatype.v1: registry+v1
              operators.operatorframework.io.bundle.metadata.v1: metadata/
              operators.operatorframework.io.bundle.package.v1: {package}
            '''
        )
    )

    package_name, labels = build_regenerate_bundle._apply_package_name_suffix(
        str(metadata_dir), package_name_suffix
    )

    assert package_name == expected_package
    assert labels == expected_labels
    with open(annotations_yaml, 'r') as f:
        annotations_yaml_content = yaml.load(f)
    annotation_key = 'operators.operatorframework.io.bundle.package.v1'
    assert annotations_yaml_content['annotations'][annotation_key] == expected_package


def test_annotations_with_preserved_quotes(tmpdir):
    metadata_dir = tmpdir.mkdir('metadata')
    annotations_yaml = metadata_dir.join('annotations.yaml')
    annotations_yaml.write(
        textwrap.dedent(
            '''\
            annotations:
              operators.operatorframework.io.bundle.package.v1: amq-streams
              spam: "spam:maps"
            '''
        )
    )

    build_regenerate_bundle._apply_package_name_suffix(str(metadata_dir), '-cmp')
    annotations_yaml_content = annotations_yaml.read()
    assert annotations_yaml_content == textwrap.dedent(
        '''\
        annotations:
          operators.operatorframework.io.bundle.package.v1: amq-streams-cmp
          spam: "spam:maps"
        '''
    )


def test_apply_package_name_suffix_missing_annotations_yaml(tmpdir):
    metadata_dir = tmpdir.mkdir('metadata')

    expected = 'metadata/annotations.yaml does not exist in the bundle'
    with pytest.raises(IIBError, match=expected):
        build_regenerate_bundle._apply_package_name_suffix(str(metadata_dir), '-cmp')


@pytest.mark.parametrize(
    'annotations, expected_error',
    (
        (
            {'annotations': 'The greatest teacher, failure is.'},
            'The value of metadata/annotations.yaml must be a dictionary',
        ),
        (
            {'annotations': {'Yoda': 'You must unlearn what you have learned.'}},
            (
                'operators.operatorframework.io.bundle.package.v1 is not set in '
                'metadata/annotations.yaml'
            ),
        ),
        (
            {'annotations': {'operators.operatorframework.io.bundle.package.v1': 3}},
            (
                'The value of operators.operatorframework.io.bundle.package.v1 in '
                'metadata/annotations.yaml is not a string'
            ),
        ),
    ),
)
def test_apply_package_name_suffix_invalid_annotations_yaml(annotations, expected_error, tmpdir):
    metadata_dir = tmpdir.mkdir('metadata')
    annotations_yaml = metadata_dir.join('annotations.yaml')
    with open(str(annotations_yaml), 'w') as f:
        yaml.dump(annotations, f)

    with pytest.raises(IIBError, match=expected_error):
        build_regenerate_bundle._apply_package_name_suffix(str(metadata_dir), '-msd')


def test_apply_package_name_suffix_invalid_yaml(tmpdir):
    metadata_dir = tmpdir.mkdir('metadata')
    metadata_dir.join('annotations.yaml').write('"This is why you fail." - Yoda')

    expected = 'metadata/annotations.yaml is not valid YAML'
    with pytest.raises(IIBError, match=expected):
        build_regenerate_bundle._apply_package_name_suffix(str(metadata_dir), '-vk')


def test_adjust_csv_annotations(tmpdir):
    manifests_dir = tmpdir.mkdir('manifests')
    manifests_dir.join('backup.crd.yaml').write(
        'apiVersion: apiextensions.k8s.io/v1beta1\nkind: CustomResourceDefinition'
    )
    annotations = {
        'marketplace.company.io/remote-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
        ),
        'marketplace.company.io/support-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/support'
        ),
    }
    csv = manifests_dir.join('mig-operator.v1.1.1.clusterserviceversion.yaml')
    csv.write('apiVersion: operators.coreos.com/v1alpha1\nkind: ClusterServiceVersion')

    operator_manifest = OperatorManifest.from_directory(str(manifests_dir))
    build_regenerate_bundle._adjust_csv_annotations(
        operator_manifest.files, 'amqp-streams', annotations
    )

    with open(csv, 'r') as f:
        csv_content = yaml.load(f)

    assert csv_content == {
        'apiVersion': 'operators.coreos.com/v1alpha1',
        'kind': 'ClusterServiceVersion',
        'metadata': {
            'annotations': {
                'marketplace.company.io/remote-workflow': (
                    'https://marketplace.company.com/en-us/operators/amqp-streams/pricing'
                ),
                'marketplace.company.io/support-workflow': (
                    'https://marketplace.company.com/en-us/operators/amqp-streams/support'
                ),
            }
        },
    }


@pytest.mark.parametrize('name_label', ('namespace/reponame', 'reponame-only', 'foo/bar/some'))
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_labels')
def test_replace_image_name_from_labels(mock_gil, name_label, tmpdir):
    manifests_dir = tmpdir.mkdir('manifests')
    csv1 = manifests_dir.join('1.clusterserviceversion.yaml')
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/{operator}{image}{ref}
        """
    )
    image_digest = '654321'
    mock_gil.return_value = {'name': name_label, 'version': 'rhel-8'}
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/{operator}{image}{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            operator='operator',
            image='/image',
            ref=':v1',
            related_name=f'image-{image_digest}-annotation',
            related_ref='@sha256:749327',
        )
    )
    operator_manifest = OperatorManifest.from_directory(str(manifests_dir))
    bundle_metadata = build_regenerate_bundle._get_bundle_metadata(
        operator_manifest, False, perform_sanity_checks=False
    )
    build_regenerate_bundle._replace_image_name_from_labels(
        bundle_metadata, '{name}-original-{version}'
    )
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        ref=':v1',
        related_name='image-654321-annotation',
        related_ref='@sha256:749327',
        operator=name_label,
        image='-original-rhel-8',
    )
    assert mock_gil.call_count == 2


@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_labels')
def test_replace_image_name_from_labels_invalid_labels(mock_gil, tmpdir):
    manifests_dir = tmpdir.mkdir('manifests')
    csv1 = manifests_dir.join('1.clusterserviceversion.yaml')
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/{operator}{image}{ref}
        """
    )
    image_digest = '654321'
    mock_gil.return_value = {'name': 'namespace/reponame', 'version': 'rhel-8'}
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/{operator}{image}{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            operator='operator',
            image='/image',
            ref=':v1',
            related_name=f'image-{image_digest}-annotation',
            related_ref='@sha256:749327',
        )
    )
    operator_manifest = OperatorManifest.from_directory(str(manifests_dir))
    bundle_metadata = build_regenerate_bundle._get_bundle_metadata(
        operator_manifest, False, perform_sanity_checks=False
    )
    expected = (
        r' is missing one or more label\(s\) required in the '
        r'image_name_from_labels {name}-original-{unknown_label}. Available labels: name, version'
    )
    with pytest.raises(IIBError, match=expected):
        build_regenerate_bundle._replace_image_name_from_labels(
            bundle_metadata, '{name}-original-{unknown_label}'
        )


@pytest.mark.parametrize(
    'original_image, eclosure_namespace, expected_image',
    (
        ('/image', 'company-pending', 'operator----image'),
        ('/image/foo/bar', 'operator', 'image----foo----bar'),
        ('/image', 'operator', 'image'),
        ('-image', 'company-managed', 'operator-image'),
    ),
)
def test_apply_repo_enclosure(original_image, eclosure_namespace, expected_image, tmpdir):
    manifests_dir = tmpdir.mkdir('manifests')
    csv1 = manifests_dir.join('1.clusterserviceversion.yaml')
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/{operator}{image}{ref}
        """
    )
    image_digest = '654321'
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/{operator}{image}{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            operator='operator',
            image=original_image,
            ref=':v1',
            related_name=f'image-{image_digest}-annotation',
            related_ref='@sha256:749327',
        )
    )
    operator_manifest = OperatorManifest.from_directory(str(manifests_dir))
    bundle_metadata = build_regenerate_bundle._get_bundle_metadata(
        operator_manifest, False, perform_sanity_checks=False
    )
    build_regenerate_bundle._apply_repo_enclosure(bundle_metadata, eclosure_namespace, '----')
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        ref=':v1',
        related_name='image-654321-annotation',
        related_ref='@sha256:749327',
        operator=f'{eclosure_namespace}/',
        image=expected_image,
    )


@mock.patch('iib.workers.tasks.build_regenerate_bundle._get_package_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._apply_package_name_suffix')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_resolved_image')
@mock.patch('iib.workers.tasks.build_regenerate_bundle._adjust_csv_annotations')
@mock.patch('iib.workers.tasks.build_regenerate_bundle.get_image_labels')
def test_adjust_operator_bundle_duplicate_customizations_ordered(
    mock_gil, mock_aca, mock_gri, mock_apns, mock_gpa, tmpdir
):
    manager = MagicMock()
    manager.attach_mock(mock_gpa, 'mock_gpa')
    manager.attach_mock(mock_apns, 'mock_apns')
    manager.attach_mock(mock_gri, 'mock_gri')
    manager.attach_mock(mock_aca, 'mock_aca')
    manager.attach_mock(mock_gil, 'mock_gil')

    annotations = {
        'marketplace.company.io/remote-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/pricing'
        ),
        'marketplace.company.io/support-workflow': (
            'https://marketplace.company.com/en-us/operators/{package_name}/support'
        ),
    }

    mock_gpa.return_value = {
        'annotations': {'operators.operatorframework.io.bundle.package.v1': 'amqstreams'}
    }
    mock_apns.return_value = (
        'amqstreams',
        {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'},
    )
    mock_gil.return_value = {'name': 'namespace/reponame', 'version': 'rhel-8'}
    manifests_dir = tmpdir.mkdir('manifests')
    metadata_dir = tmpdir.mkdir('metadata')
    csv1 = manifests_dir.join('1.clusterserviceversion.yaml')
    csv2 = manifests_dir.join('2.clusterserviceversion.yaml')
    csv3 = manifests_dir.join('3.clusterserviceversion.yaml')

    # NOTE: The OperatorManifest class is capable of modifying pull specs found in
    # various locations within the CSV file. Since IIB relies on this class to do
    # such modifications, this test only verifies that at least one of the locations
    # is being handled properly. This is to ensure IIB is using OperatorManifest
    # correctly.
    csv_template = textwrap.dedent(
        """\
        apiVersion: operators.example.com/v1
        kind: ClusterServiceVersion
        metadata:
          name: amqstreams.v1.0.0
          namespace: placeholder
          annotations:
            containerImage: {registry}/{operator}{image}{ref}
        """
    )
    image_digest = '654321'
    csv_related_images_template = csv_template + textwrap.dedent(
        """\
        spec:
          relatedImages:
          - name: {related_name}
            image: {registry}/{operator}{image}{related_ref}
        """
    )
    csv1.write(
        csv_related_images_template.format(
            registry='quay.io',
            operator='operator',
            image='/image',
            ref=':v1',
            related_name=f'image-{image_digest}-annotation',
            related_ref='@sha256:749327',
        )
    )
    csv2.write(
        csv_template.format(
            registry='quay.io', operator='operator', image='/image', ref='@sha256:654321'
        )
    )
    csv3.write(
        csv_template.format(
            registry='registry.access.company.com', operator='operator', image='/image', ref=':v2'
        )
    )

    def get_resolved_image(image):
        return {
            'quay.io/operator/image:v2': 'quay.io/operator/image@sha256:654321',
            'quay.io/operator/image@sha256:654321': 'quay.io/operator/image@sha256:654321',
            'registry.access.company.com/operator/image:v2': (
                'registry.access.company.com/operator/image@sha256:654321'
            ),
        }[image]

    mock_gri.side_effect = get_resolved_image

    labels = build_regenerate_bundle._adjust_operator_bundle(
        str(manifests_dir), str(metadata_dir), 'company-managed'
    )

    assert labels == {
        'com.redhat.iib.pinned': 'true',
        'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp',
    }
    # Verify that the relatedImages are not modified if they were already set and that images were
    # not pinned
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quaaay.com',
        operator='company-pending/',
        image='namespace----reponame-rhel-8',
        ref=':v1',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:749327',
    )
    assert csv2.read_text('utf-8') == csv_related_images_template.format(
        registry='quaaay.com',
        operator='company-pending/',
        image='namespace----reponame-rhel-8',
        ref='@sha256:654321',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    assert csv3.read_text('utf-8') == csv_related_images_template.format(
        registry='quaaay.com',
        operator='company-pending/',
        image='namespace----reponame-rhel-8',
        ref='@sha256:654321',
        related_name=f'image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    mock_aca.assert_called_once_with(mock.ANY, 'amqstreams', annotations)
    assert mock_gil.call_count == 3

    expected_calls = [
        call.mock_gpa(mock.ANY),
        call.mock_gri(mock.ANY),
        call.mock_gri(mock.ANY),
        call.mock_apns(mock.ANY, '-cmp'),
        call.mock_aca(mock.ANY, 'amqstreams', annotations),
        call.mock_gil(mock.ANY),
        call.mock_gil(mock.ANY),
        call.mock_gil(mock.ANY),
    ]
    assert manager.mock_calls == expected_calls
