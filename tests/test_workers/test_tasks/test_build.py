# SPDX-License-Identifier: GPL-3.0-or-later
import copy
import os
import re
import textwrap
from unittest import mock

from operator_manifest.operator import OperatorManifest
import pytest
import ruamel.yaml

from iib.exceptions import IIBError
from iib.workers.tasks import build


yaml = ruamel.yaml.YAML()


@mock.patch('iib.workers.tasks.build.run_cmd')
def test_build_image(mock_run_cmd):
    build._build_image('/some/dir', 'some.Dockerfile', 3, 'amd64')

    mock_run_cmd.assert_called_once()
    build_args = mock_run_cmd.call_args[0][0]
    assert build_args[0:2] == ['buildah', 'bud']
    assert '/some/dir/some.Dockerfile' in build_args


@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.reset_docker_config')
def test_cleanup(mock_rdc, mock_run_cmd):
    build._cleanup()

    mock_run_cmd.assert_called_once()
    rmi_args = mock_run_cmd.call_args[0][0]
    assert rmi_args[0:2] == ['podman', 'rmi']
    mock_rdc.assert_called_once_with()


@mock.patch('iib.workers.tasks.build.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_create_and_push_manifest_list(mock_run_cmd, mock_td, tmp_path):
    mock_td.return_value.__enter__.return_value = tmp_path

    build._create_and_push_manifest_list(3, {'amd64', 's390x'})

    expected_manifest = textwrap.dedent(
        '''\
        image: registry:8443/iib-build:3
        manifests:
        - image: registry:8443/iib-build:3-amd64
          platform:
            architecture: amd64
            os: linux
        - image: registry:8443/iib-build:3-s390x
          platform:
            architecture: s390x
            os: linux
        '''
    )
    manifest = os.path.join(tmp_path, 'manifest.yaml')
    with open(manifest, 'r') as manifest_f:
        assert manifest_f.read() == expected_manifest
    mock_run_cmd.assert_called_once()
    manifest_tool_args = mock_run_cmd.call_args[0][0]
    assert manifest_tool_args[0] == 'manifest-tool'
    assert manifest in manifest_tool_args


@pytest.mark.parametrize(
    'iib_index_image_output_registry, from_index, overwrite, expected',
    (
        (None, None, False, '{default}'),
        (
            'registry-proxy.domain.local',
            None,
            False,
            'registry-proxy.domain.local/{default_no_registry}',
        ),
        (None, 'quay.io/ns/iib:v4.5', True, 'quay.io/ns/iib:v4.5'),
        (None, 'quay.io/ns/iib:v5.4', True, 'quay.io/ns/iib:v5.4'),
    ),
)
@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build._overwrite_from_index')
@mock.patch('iib.workers.tasks.build.update_request')
def test_update_index_image_pull_spec(
    mock_ur, mock_ofi, mock_gwc, iib_index_image_output_registry, from_index, overwrite, expected
):
    default_no_registry = 'namespace/some-image:3'
    default = f'quay.io/{default_no_registry}'
    expected_pull_spec = expected.format(default=default, default_no_registry=default_no_registry)
    request_id = 2
    arches = {'amd64'}
    overwrite_token = 'username:password'
    mock_gwc.return_value = {
        'iib_index_image_output_registry': iib_index_image_output_registry,
        'iib_registry': 'quay.io',
    }
    build._update_index_image_pull_spec(
        default, request_id, arches, from_index, overwrite, overwrite_token
    )

    mock_ur.assert_called_once()
    update_request_payload = mock_ur.call_args[0][1]
    assert update_request_payload.keys() == {'arches', 'index_image'}
    assert update_request_payload['index_image'] == expected_pull_spec
    if overwrite:
        mock_ofi.assert_called_once_with(request_id, default, from_index, overwrite_token)
    else:
        mock_ofi.assert_not_called()


@pytest.mark.parametrize(
    'pull_spec, expected',
    (
        ('quay.io/ns/repo:latest', 'quay.io/ns/repo'),
        ('quay.io/ns/repo@sha256:123456', 'quay.io/ns/repo'),
    ),
)
def test_get_container_image_name(pull_spec, expected):
    assert build._get_container_image_name(pull_spec) == expected


@pytest.mark.parametrize('request_id, arch', ((1, 'amd64'), (5, 's390x')))
def test_get_local_pull_spec(request_id, arch):
    rv = build._get_local_pull_spec(request_id, arch)

    assert re.match(f'.+:{request_id}-{arch}', rv)


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_image_arches(mock_si):
    mock_si.return_value = {
        'mediaType': 'application/vnd.docker.distribution.manifest.list.v2+json',
        'manifests': [
            {'platform': {'architecture': 'amd64'}},
            {'platform': {'architecture': 's390x'}},
        ],
    }
    rv = build._get_image_arches('image:latest')
    assert rv == {'amd64', 's390x'}


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_image_arches_manifest(mock_si):
    mock_si.side_effect = [
        {'mediaType': 'application/vnd.docker.distribution.manifest.v2+json'},
        {'architecture': 'amd64'},
    ]
    rv = build._get_image_arches('image:latest')
    assert rv == {'amd64'}


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_image_arches_not_manifest_list(mock_si):
    mock_si.return_value = {'mediaType': 'application/vnd.docker.distribution.notmanifest.v2+json'}
    with pytest.raises(IIBError, match='.+is neither a v2 manifest list nor a v2 manifest'):
        build._get_image_arches('image:latest')


@pytest.mark.parametrize('label, expected', (('some_label', 'value'), ('not_there', None)))
@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_label(mock_si, label, expected):
    mock_si.return_value = {'config': {'Labels': {'some_label': 'value'}}}
    assert build.get_image_label('some-image:latest', label) == expected


@pytest.mark.parametrize(
    'pull_spec, expected',
    (
        (
            'quay.io/ns/image:8',
            (
                'quay.io/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1c7e82'
                '8c568e'
            ),
        ),
        (
            'quay.io:443/ns/image:8',
            (
                'quay.io:443/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1'
                'c7e828c568e'
            ),
        ),
        (
            (
                'quay.io/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1c7e8'
                '28c568e'
            ),
            (
                'quay.io/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1c7e82'
                '8c568e'
            ),
        ),
    ),
)
@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_resolved_image(mock_si, pull_spec, expected):
    mock_si.return_value = textwrap.dedent(
        '''
        {
           "schemaVersion": 2,
           "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
           "config": {
              "mediaType": "application/vnd.docker.container.image.v1+json",
              "size": 5545,
              "digest": "sha256:720713e1a4410985aacd7008719efd13d8a32e76d08d34fca202a60ff43e516d"
           },
           "layers": [
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 76275160,
                 "digest": "sha256:a3ac36470b00df382448e79f7a749aa6833e4ac9cc90e3391f778820db9fa407"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 1598,
                 "digest": "sha256:82a8f4ea76cb6f833c5f179b3e6eda9f2267ed8ac7d1bf652f88ac3e9cc453d1"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 3500790,
                 "digest": "sha256:e1a6856f83e7ab214d6a8200d5fd22f2311e794c91c59eae3fd49699cbc4a14e"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 8236572,
                 "digest": "sha256:c82b363416dcd84a2f1c292c3a85b21cbf01f5f2ee7f8b88f4dcfffe53ce549d"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 92298818,
                 "digest": "sha256:8befc59eb9f1a3f40d3de0eccca8762c95800322c3a83fe40bbc0273df394ac1"
              }
           ]
        }
        '''  # noqa: E501
    ).strip('\n')
    rv = build._get_resolved_image(pull_spec)
    assert rv == expected


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_resolved_image_manifest_list(mock_si):
    mock_si.return_value = (
        r'{"manifests":[{"digest":"sha256:9e0c275e0bcb495773b10a18e499985d782810e47b4fce076422acb4b'
        r'c3da3dd","mediaType":"application\/vnd.docker.distribution.manifest.v2+json","platform":'
        r'{"architecture":"amd64","os":"linux"},"size":529},{"digest":"sha256:85313b812ad747dd19cf1'
        r'8078795b576cc4ae9cd2ca2ccccd7b5c12722b2effd","mediaType":"application\/vnd.docker.distrib'
        r'ution.manifest.v2+json","platform":{"architecture":"arm64","os":"linux","variant":"v8"},"'
        r'size":529},{"digest":"sha256:567785922b920b35aee6a217f70433fd437b335ad45054743c960d1aaa14'
        r'3dcd","mediaType":"application\/vnd.docker.distribution.manifest.v2+json","platform":{"ar'
        r'chitecture":"ppc64le","os":"linux"},"size":529}],"mediaType":"application\/vnd.docker.dis'
        r'tribution.manifest.list.v2+json","schemaVersion":2}'
    )
    rv = build._get_resolved_image('docker.io/library/centos:8')
    assert rv == (
        'docker.io/library/centos@sha256:fe8d824220415eed5477b63addf40fb06c3b049404242b31982106ac'
        '204f6700'
    )
    mock_si.assert_called_once_with(
        'docker://docker.io/library/centos:8', '--raw', return_json=False
    )


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_resolved_image_schema_1(mock_si):
    image_manifest_schema_1 = textwrap.dedent(
        """\
        {
           "schemaVersion": 1,
           "name": "repository/name",
           "tag": "1.0.0",
           "architecture": "amd64",
           "fsLayers": [],
           "history": [],
           "signatures": [
              {
                 "header": {},
                 "signature": "text-that-changes-per-request",
                 "protected": "spam"
              }
           ]
        }
        """
    )

    skopeo_output = {
        "Name": "registry.example.com/repository/name",
        "Tag": "1.0.0",
        "Digest": "sha256:aa6680b35f45cf0fd6fb5f417159257ba410a47b8fa20d37b4c7fcd4a564b3fb",
        "RepoTags": ["1.0.0", "latest"],
        "Created": "2019-12-04T06:41:46.3149046Z",
        "DockerVersion": "19.03.2",
        "Labels": {},
        "Architecture": "amd64",
        "Os": "linux",
        "Layers": [],
        "Env": [],
    }

    mock_si.side_effect = [image_manifest_schema_1, skopeo_output]
    rv = build._get_resolved_image('registry.example.com/repository/name:1.0.0')
    assert rv == (
        'registry.example.com/repository/name@sha256:aa6680b35f45cf0fd6fb5f417159257ba410a47b8fa2'
        '0d37b4c7fcd4a564b3fb'
    )

    mock_si.assert_has_calls(
        [
            mock.call(
                'docker://registry.example.com/repository/name:1.0.0', '--raw', return_json=False
            ),
            mock.call('docker://registry.example.com/repository/name:1.0.0'),
        ]
    )


@pytest.mark.parametrize(
    'skopeo_inspect_rv, expected_response',
    (
        (
            {
                'mediaType': 'application/vnd.docker.distribution.manifest.list.v2+json',
                'manifests': [
                    {'platform': {'architecture': 'amd64'}, 'digest': 'arch_digest'},
                    {'platform': {'architecture': 's390x'}, 'digest': 'different_arch_digest'},
                ],
            },
            ['some_bundle@arch_digest'],
        ),
        (
            {
                'mediaType': 'application/vnd.docker.distribution.manifest.v2+json',
                'schemaVersion': 2,
            },
            ['some_bundle@manifest_digest'],
        ),
    ),
)
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_resolved_bundles_success(mock_si, mock_gri, skopeo_inspect_rv, expected_response):
    mock_si.return_value = skopeo_inspect_rv
    mock_gri.return_value = 'some_bundle@manifest_digest'
    response = build._get_resolved_bundles(['some_bundle:1.2'])
    if skopeo_inspect_rv['mediaType'] == 'application/vnd.docker.distribution.manifest.v2+json':
        mock_gri.assert_called_once()
    else:
        mock_gri.assert_not_called()
    assert response == expected_response


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_resolved_bundles_failure(mock_si):
    skopeo_inspect_rv = {
        'mediaType': 'application/vnd.docker.distribution.notmanifest.v2+json',
        'schemaVersion': 1,
    }
    mock_si.return_value = skopeo_inspect_rv
    with pytest.raises(
        IIBError, match='.+ and schema version 1 is not supported by IIB.',
    ):
        build._get_resolved_bundles(['some_bundle@some_sha'])


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_opm_index_add(mock_run_cmd, mock_srt, from_index, bundles):
    build._opm_index_add('/tmp/somedir', bundles, 'binary-image:latest', from_index, 'user:pass')

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'add']
    if bundles:
        assert ','.join(bundles) in opm_args
    else:
        assert '""' in opm_args
    if from_index:
        assert '--from-index' in opm_args
        assert from_index in opm_args
    else:
        assert '--from-index' not in opm_args
    mock_srt.assert_called_once_with('user:pass', from_index)


@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_opm_index_rm(mock_run_cmd, mock_srt):
    operators = ['operator_1', 'operator_2']
    build._opm_index_rm(
        '/tmp/somedir', operators, 'binary-image:latest', 'some_index:latest', 'user:pass'
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'rm']
    assert ','.join(operators) in opm_args
    assert 'some_index:latest' in opm_args
    mock_srt.assert_called_once_with('user:pass', 'some_index:latest')


@pytest.mark.parametrize(
    'output_pull_spec, from_index, overwrite_from_index_token, oci_export_expected',
    (
        ('quay.io/ns/repo:1', 'quay.io/user_ns/repo:v1', 'user:pass', True),
        ('quay.io/ns/repo:1', 'docker.io/user_ns/repo:v1', 'user:pass', False),
        ('quay.io/ns/repo:1', 'quay.io/user_ns/repo:v1', None, False),
    ),
)
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build._skopeo_copy')
@mock.patch('iib.workers.tasks.build.set_registry_token')
def test_overwrite_from_index(
    mock_srt,
    mock_sc,
    mock_td,
    mock_srs,
    output_pull_spec,
    from_index,
    overwrite_from_index_token,
    oci_export_expected,
):
    mock_td.return_value.name = '/tmp/iib-12345'
    build._overwrite_from_index(1, output_pull_spec, from_index, overwrite_from_index_token)

    if oci_export_expected:
        oci_pull_spec = f'oci:{mock_td.return_value.name}'
        mock_sc.assert_has_calls(
            (
                mock.call(
                    f'docker://{output_pull_spec}', oci_pull_spec, copy_all=True, exc_msg=mock.ANY,
                ),
                mock.call(oci_pull_spec, f'docker://{from_index}', copy_all=True, exc_msg=mock.ANY),
            )
        )
        mock_td.return_value.cleanup.assert_called_once_with()
    else:
        mock_sc.assert_called_once_with(
            f'docker://{output_pull_spec}',
            f'docker://{from_index}',
            copy_all=True,
            exc_msg=mock.ANY,
        )
        mock_td.return_value.cleanup.assert_not_called()

    mock_srt.assert_called_once()


@pytest.mark.parametrize(
    'add_arches, from_index, from_index_arches, bundles, expected_bundle_mapping',
    (
        ([], 'some-index:latest', {'amd64'}, None, {}),
        (['amd64', 's390x'], None, set(), None, {}),
        (['amd64'], 'some-index:latest', {'amd64'}, None, {}),
        (
            ['amd64'],
            None,
            set(),
            ['quay.io/some-bundle:v1', 'quay.io/some-bundle2:v1'],
            {
                'some-bundle': ['quay.io/some-bundle:v1'],
                'some-bundle2': ['quay.io/some-bundle2:v1'],
            },
        ),
        (
            ['amd64'],
            'some-index:latest',
            set(),
            ['quay.io/some-bundle:v1', 'quay.io/some-bundle2:v1'],
            {
                'some-bundle': ['quay.io/some-bundle:v1'],
                'some-bundle2': ['quay.io/some-bundle2:v1'],
            },
        ),
    ),
)
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._get_image_arches')
@mock.patch('iib.workers.tasks.build.get_image_label')
@mock.patch('iib.workers.tasks.build.update_request')
def test_prepare_request_for_build(
    mock_ur,
    mock_gil,
    mock_gia,
    mock_gri,
    mock_srs,
    add_arches,
    from_index,
    from_index_arches,
    bundles,
    expected_bundle_mapping,
):
    binary_image_resolved = 'binary-image@sha256:abcdef'
    from_index_resolved = None
    expected_arches = set(add_arches) | from_index_arches
    expected_payload_keys = {'binary_image_resolved', 'state', 'state_reason'}
    gil_side_effect = []
    ocp_version = 'v4.5'
    if expected_bundle_mapping:
        expected_payload_keys.add('bundle_mapping')
    if from_index:
        from_index_name = from_index.split(':', 1)[0]
        from_index_resolved = f'{from_index_name}@sha256:bcdefg'
        mock_gri.side_effect = [binary_image_resolved, from_index_resolved]
        mock_gia.side_effect = [expected_arches, from_index_arches]
        expected_payload_keys.add('from_index_resolved')
        gil_side_effect = ['v4.6']
        ocp_version = 'v4.6'
    else:
        mock_gri.side_effect = [binary_image_resolved]
        mock_gia.side_effect = [expected_arches]
        gil_side_effect = []

    if bundles:
        bundle_side_effects = [bundle.rsplit('/', 1)[1].split(':', 1)[0] for bundle in bundles]
        gil_side_effect.extend(bundle_side_effects)

    mock_gil.side_effect = gil_side_effect
    rv = build._prepare_request_for_build(
        'binary-image:latest', 1, from_index, None, add_arches, bundles
    )

    assert rv == {
        'arches': expected_arches,
        'binary_image_resolved': binary_image_resolved,
        'bundle_mapping': expected_bundle_mapping,
        'from_index_resolved': from_index_resolved,
        'ocp_version': ocp_version,
        'source_from_index_resolved': None,
        'source_ocp_version': 'v4.5',
        'target_index_resolved': None,
        'target_ocp_version': 'v4.6',
    }


@pytest.mark.parametrize('bundle_mapping', (True, False))
@pytest.mark.parametrize('from_index_resolved', (True, False))
@mock.patch('iib.workers.tasks.build.update_request')
def test_update_index_image_build_state(
    mock_ur, bundle_mapping, from_index_resolved,
):
    prebuild_info = {
        'arches': ['amd64', 's390x'],
        'binary_image_resolved': 'binary-image@sha256:12345',
        'extra': 'ignored',
    }

    if bundle_mapping:
        prebuild_info['bundle_mapping'] = {
            'some-bundle': ['quay.io/some-bundle:v1'],
            'some-bundle2': ['quay.io/some-bundle2:v1'],
        }

    if from_index_resolved:
        prebuild_info['from_index_resolved'] = 'from-index-image@sha256:abcde'

    expected_payload = copy.deepcopy(prebuild_info)
    del expected_payload['arches']
    del expected_payload['extra']
    expected_payload['state'] = 'in_progress'
    expected_payload['state_reason'] = mock.ANY

    request_id = 1
    build._update_index_image_build_state(request_id, prebuild_info)
    mock_ur.assert_called_once_with(request_id, expected_payload, mock.ANY)


@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._get_image_arches')
def test_prepare_request_for_build_no_arches(mock_gia, mock_gri, mock_srs):
    mock_gia.side_effect = [{'amd64'}]

    with pytest.raises(IIBError, match='No arches.+'):
        build._prepare_request_for_build('binary-image:latest', 1)


@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._get_image_arches')
def test_prepare_request_for_build_binary_image_no_arch(mock_gia, mock_gri, mock_srs):
    mock_gia.side_effect = [{'amd64'}]

    expected = 'The binary image is not available for the following arches.+'
    with pytest.raises(IIBError, match=expected):
        build._prepare_request_for_build('binary-image:latest', 1, add_arches=['s390x'])


@pytest.mark.parametrize('schema_version', (1, 2))
@mock.patch('iib.workers.tasks.build._get_local_pull_spec')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.skopeo_inspect')
@mock.patch('iib.workers.tasks.build._skopeo_copy')
def test_push_image(mock_sc, mock_si, mock_run_cmd, mock_glps, schema_version):
    mock_glps.return_value = 'source:tag'
    mock_si.return_value = {'schemaVersion': schema_version}

    build._push_image(3, 'amd64')

    push_args = mock_run_cmd.mock_calls[0][1][0]
    assert push_args[0:2] == ['podman', 'push']
    assert 'source:tag' in push_args
    destination = 'docker://registry:8443/iib-build:3-amd64'
    assert destination in push_args

    if schema_version == 1:
        mock_sc.assert_called_once()
    else:
        mock_run_cmd.assert_called_once()


@pytest.mark.parametrize('copy_all', (False, True))
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_skopeo_copy(mock_run_cmd, copy_all):
    destination = 'some_destination'
    build._skopeo_copy(destination, destination, copy_all=copy_all)
    skopeo_args = mock_run_cmd.mock_calls[0][1][0]
    if copy_all:
        expected = [
            'skopeo',
            '--command-timeout',
            '300s',
            'copy',
            '--format',
            'v2s2',
            '--all',
            destination,
            destination,
        ]
    else:
        expected = [
            'skopeo',
            '--command-timeout',
            '300s',
            'copy',
            '--format',
            'v2s2',
            destination,
            destination,
        ]
    assert skopeo_args == expected
    mock_run_cmd.assert_called_once()


@mock.patch('iib.workers.tasks.build.run_cmd')
def test_skopeo_copy_fail_max_retries(mock_run_cmd):
    match_str = 'Something went wrong'
    mock_run_cmd.side_effect = IIBError(match_str)
    destination = 'some_destination'
    with pytest.raises(IIBError, match=match_str):
        build._skopeo_copy(destination, destination)
        assert mock_run_cmd.call_count == 5


@pytest.mark.parametrize('force_backport', (True, False))
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._verify_labels')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build._opm_index_add')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build.export_legacy_packages')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.get_legacy_support_packages')
@mock.patch('iib.workers.tasks.build.validate_legacy_params_and_config')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build._get_resolved_bundles')
@mock.patch('iib.workers.tasks.build._add_ocp_label_to_index')
@mock.patch('iib.workers.tasks.build._get_present_bundles')
@mock.patch('iib.workers.tasks.build._get_missing_bundles')
@mock.patch('iib.workers.tasks.build.set_registry_token')
def test_handle_add_request(
    mock_srt,
    mock_gmb,
    mock_gpb,
    mock_aolti,
    mock_grb,
    mock_gb,
    mock_vlpc,
    mock_glsp,
    mock_capml,
    mock_srs,
    mock_elp,
    mock_uiips,
    mock_vii,
    mock_pi,
    mock_bi,
    mock_oia,
    mock_uiibs,
    mock_prfb,
    mock_vl,
    mock_cleanup,
    force_backport,
):
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
        'ocp_version': 'v4.5',
    }
    mock_grb.return_value = ['some-bundle@sha']
    legacy_packages = {'some_package'}
    mock_glsp.return_value = legacy_packages
    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec

    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    build.handle_add_request(
        bundles,
        'binary-image:latest',
        3,
        'from-index:latest',
        ['s390x'],
        cnr_token,
        organization,
        force_backport,
        False,
        None,
        greenwave_config,
    )

    mock_cleanup.assert_called_once()
    mock_vl.assert_called_once()
    mock_prfb.assert_called_once()
    mock_gb.assert_called_once()
    mock_aolti.assert_called_once()
    mock_glsp.assert_called_once_with(['some-bundle@sha'], 3, 'v4.5', force_backport=force_backport)

    filter_args = mock_gmb.call_args[0]
    assert ['some-bundle@sha'] in filter_args
    mock_oia.assert_called_once()
    mock_srt.assert_called_once()

    assert mock_bi.call_count == len(arches)
    assert mock_pi.call_count == len(arches)

    mock_elp.assert_called_once()
    export_args = mock_elp.call_args[0]
    assert legacy_packages in export_args
    assert cnr_token in export_args
    assert organization in export_args

    mock_uiips.assert_called_once()
    mock_vii.assert_called_once()
    mock_capml.assert_called_once()
    assert mock_srs.call_count == 4


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build._verify_labels')
@mock.patch('iib.workers.tasks.build._get_resolved_bundles')
def test_handle_add_request_gating_failure(mock_grb, mock_vl, mock_gb, mock_srs, mock_cleanup):
    error_msg = 'Gating failure!'
    mock_gb.side_effect = IIBError(error_msg)
    mock_grb.return_value = ['some-bundle@sha']
    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    with pytest.raises(IIBError, match=error_msg):
        build.handle_add_request(
            bundles,
            'binary-image:latest',
            3,
            'from-index:latest',
            ['s390x'],
            cnr_token,
            organization,
            None,
            False,
            None,
            greenwave_config,
        )
    mock_cleanup.assert_called_once_with()
    mock_srs.assert_called_once()
    mock_vl.assert_called_once()
    mock_gb.assert_called_once_with(['some-bundle@sha'], greenwave_config)


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_bundles')
def test_handle_add_request_bundle_resolution_failure(mock_grb, mock_srs, mock_cleanup):
    error_msg = 'Bundle Resolution failure!'
    mock_grb.side_effect = IIBError(error_msg)
    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    with pytest.raises(IIBError, match=error_msg):
        build.handle_add_request(
            bundles,
            'binary-image:latest',
            3,
            'from-index:latest',
            ['s390x'],
            cnr_token,
            organization,
            False,
            None,
            greenwave_config,
        )
    mock_cleanup.assert_called_once_with()
    mock_srs.assert_called_once()
    mock_grb.assert_called_once_with(bundles)


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._verify_labels')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build._opm_index_add')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build.export_legacy_packages')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.get_legacy_support_packages')
@mock.patch('iib.workers.tasks.build.validate_legacy_params_and_config')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build._get_resolved_bundles')
@mock.patch('iib.workers.tasks.build._add_ocp_label_to_index')
@mock.patch('iib.workers.tasks.build._get_present_bundles')
@mock.patch('iib.workers.tasks.build._get_missing_bundles')
@mock.patch('iib.workers.tasks.build.set_registry_token')
def test_handle_add_request_backport_failure_no_overwrite(
    mock_srt,
    mock_gmb,
    mock_gpb,
    mock_aolti,
    mock_grb,
    mock_gb,
    mock_vlpc,
    mock_glsp,
    mock_capml,
    mock_srs,
    mock_elp,
    mock_uiips,
    mock_vii,
    mock_pi,
    mock_bi,
    mock_oia,
    mock_uiibs,
    mock_prfb,
    mock_vl,
    mock_cleanup,
):
    error_msg = 'Backport failure!'
    mock_elp.side_effect = IIBError(error_msg)
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
        'ocp_version': 'v4.6',
    }
    mock_grb.return_value = ['some-bundle@sha']
    legacy_packages = {'some_package'}
    mock_glsp.return_value = legacy_packages
    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec

    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    with pytest.raises(IIBError, match=error_msg):
        build.handle_add_request(
            bundles,
            'binary-image:latest',
            3,
            'from-index:latest',
            ['s390x'],
            cnr_token,
            organization,
            False,
            None,
            greenwave_config,
        )
    mock_elp.assert_called_once()
    mock_uiips.assert_not_called()


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build._opm_index_rm')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build._add_ocp_label_to_index')
def test_handle_rm_request(
    mock_aolti,
    mock_uiips,
    mock_capml,
    mock_srs,
    mock_vii,
    mock_pi,
    mock_bi,
    mock_uiibs,
    mock_oir,
    mock_prfb,
    mock_cleanup,
):
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
        'ocp_version': 'v4.6',
    }
    build.handle_rm_request(['some-operator'], 'binary-image:latest', 3, 'from-index:latest')

    mock_cleanup.assert_called_once()
    mock_prfb.assert_called_once()
    mock_oir.assert_called_once()
    mock_aolti.assert_called_once()
    assert mock_bi.call_count == len(arches)
    assert mock_pi.call_count == len(arches)
    mock_vii.assert_called_once()
    assert mock_srs.call_count == 2
    mock_capml.assert_called_once()
    mock_uiips.assert_called_once()
    assert mock_srs.call_args[0][1] == 'complete'


@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
def test_verify_index_image_failure(mock_gri, mock_srt):
    mock_gri.return_value = 'image:works'
    match_str = (
        'The supplied from_index image changed during the IIB request.'
        ' Please resubmit the request.'
    )
    with pytest.raises(IIBError, match=match_str):
        build._verify_index_image('image:doesnt_work', 'unresolved_image', 'user:pass')

    mock_srt.assert_called_once_with('user:pass', 'unresolved_image')


@pytest.mark.parametrize(
    'iib_required_labels', ({'com.redhat.delivery.operator.bundle': 'true'}, {})
)
@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build.get_image_labels')
def test_verify_labels(mock_gil, mock_gwc, iib_required_labels):
    mock_gwc.return_value = {'iib_required_labels': iib_required_labels}
    mock_gil.return_value = {'com.redhat.delivery.operator.bundle': 'true'}
    build._verify_labels(['some-bundle:v1.0'])

    if iib_required_labels:
        mock_gil.assert_called_once()
    else:
        mock_gil.assert_not_called()


@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build.get_image_labels')
def test_verify_labels_fails(mock_gil, mock_gwc):
    mock_gwc.return_value = {'iib_required_labels': {'com.redhat.delivery.operator.bundle': 'true'}}
    mock_gil.return_value = {'lunch': 'pizza'}
    with pytest.raises(IIBError, match='som'):
        build._verify_labels(['some-bundle:v1.0'])


@pytest.mark.parametrize(
    'pinned_by_iib_label, pinned_by_iib_bool',
    (('true', True), ('True', True), (None, False), ('false', False), ('False', False)),
)
@pytest.mark.parametrize(
    'iib_index_image_output_registry, expected_bundle_image',
    ((None, 'quay.io/iib:99'), ('dagobah.domain.local', 'dagobah.domain.local/iib:99')),
)
@mock.patch('iib.workers.tasks.build.get_image_label')
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build.podman_pull')
@mock.patch('iib.workers.tasks.build.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build._get_image_arches')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build.update_request')
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

    build.handle_regenerate_bundle_request(from_bundle_image, organization, request_id)

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


@pytest.mark.parametrize('fail_rm', (True, False))
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_copy_files_from_image(mock_run_cmd, fail_rm):
    image = 'bundle-image:latest'
    src_path = '/manifests'
    dest_path = '/destination/path/manifests'

    container_id = 'df2ff736efeaff598330a128b3dc4875caf254d9f416cefd86ec009b74d1488b'

    side_effect = [f'{container_id}\n', '']
    if fail_rm:
        side_effect.append(IIBError('Uh oh! Something went wrong.'))
    else:
        side_effect.append('')
    mock_run_cmd.side_effect = side_effect

    build._copy_files_from_image(image, src_path, dest_path)

    mock_run_cmd.assert_has_calls(
        [
            mock.call(['podman', 'create', image, 'unused'], exc_msg=mock.ANY),
            mock.call(['podman', 'cp', f'{container_id}:{src_path}', dest_path], exc_msg=mock.ANY),
            mock.call(['podman', 'rm', container_id], exc_msg=mock.ANY),
        ]
    )


@mock.patch('iib.workers.tasks.build._apply_package_name_suffix')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._adjust_csv_annotations')
def test_adjust_operator_bundle(mock_aca, mock_gri, mock_apns, tmpdir):
    mock_apns.return_value = (
        'amqstreams',
        {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'},
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

    def _get_resolved_image(image):
        return {
            'quay.io/operator/image:v2': 'quay.io/operator/image@sha256:654321',
            'quay.io/operator/image@sha256:654321': 'quay.io/operator/image@sha256:654321',
            'registry.access.company.com/operator/image:v2': (
                'registry.access.company.com/operator/image@sha256:654321'
            ),
        }[image]

    mock_gri.side_effect = _get_resolved_image

    labels = build._adjust_operator_bundle(
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
        registry='registry.marketplace.company.com/cm',
        ref='@sha256:654321',
        related_name=f'operator/image-{image_digest}-annotation',
        related_ref='@sha256:654321',
    )
    mock_aca.assert_called_once_with(mock.ANY, 'amqstreams', 'company-marketplace')


@mock.patch('iib.workers.tasks.build._apply_package_name_suffix')
def test_adjust_operator_bundle_invalid_related_images(mock_apns, tmpdir):
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
        build._adjust_operator_bundle(str(manifests_dir), str(metadata_dir))


@mock.patch('iib.workers.tasks.build._apply_package_name_suffix')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._adjust_csv_annotations')
def test_adjust_operator_bundle_already_pinned_by_iib(mock_aca, mock_gri, mock_apns, tmpdir):
    mock_apns.return_value = (
        'amqstreams',
        {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'},
    )
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
            containerImage: {registry}/operator/image{ref}
        """
    )
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
            ref='@sha256:654321',
            related_name='image-654321-annotation',
            related_ref='@sha256:654321',
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
        )
    )

    labels = build._adjust_operator_bundle(
        str(manifests_dir), str(metadata_dir), 'company-marketplace', pinned_by_iib=True
    )

    # The com.redhat.iib.pinned label is not explicitly set, but inherited from the original image
    assert labels == {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'}
    assert csv1.read_text('utf-8') == csv_related_images_template.format(
        registry='quay.io',
        ref='@sha256:654321',
        related_name=f'image-654321-annotation',
        related_ref='@sha256:654321',
    )
    assert csv2.read_text('utf-8') == csv_related_images_template.format(
        registry='registry.marketplace.company.com/cm',
        ref='@sha256:765432',
        related_name=f'operator/image-765432-annotation',
        related_ref='@sha256:765432',
    )
    mock_aca.assert_called_once_with(mock.ANY, 'amqstreams', 'company-marketplace')
    mock_gri.assert_not_called()


@pytest.mark.parametrize(
    'organization, package, expected_package, expected_labels',
    (
        (
            'company-marketplace',
            'amq-streams',
            'amq-streams-cmp',
            {'operators.operatorframework.io.bundle.package.v1': 'amq-streams-cmp'},
        ),
        (None, 'amq-streams', 'amq-streams', {}),
        ('company-marketplace', 'amq-streams-cmp', 'amq-streams-cmp', {}),
        ('non-existent', 'amq-streams', 'amq-streams', {}),
    ),
)
def test_apply_package_name_suffix(
    organization, package, expected_package, expected_labels, tmpdir
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

    package_name, labels = build._apply_package_name_suffix(str(metadata_dir), organization)

    assert package_name == expected_package
    assert labels == expected_labels
    with open(annotations_yaml, 'r') as f:
        annotations_yaml_content = yaml.load(f)
    annotation_key = 'operators.operatorframework.io.bundle.package.v1'
    assert annotations_yaml_content['annotations'][annotation_key] == expected_package


def test_apply_package_name_suffix_missing_annotations_yaml(tmpdir):
    metadata_dir = tmpdir.mkdir('metadata')

    expected = 'metadata/annotations.yaml does not exist in the bundle'
    with pytest.raises(IIBError, match=expected):
        build._apply_package_name_suffix(str(metadata_dir))


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
        build._apply_package_name_suffix(str(metadata_dir))


def test_apply_package_name_suffix_invalid_yaml(tmpdir):
    metadata_dir = tmpdir.mkdir('metadata')
    metadata_dir.join('annotations.yaml').write('"This is why you fail." - Yoda')

    expected = 'metadata/annotations/yaml is not valid YAML'
    with pytest.raises(IIBError, match=expected):
        build._apply_package_name_suffix(str(metadata_dir))


def test_adjust_csv_annotations(tmpdir):
    manifests_dir = tmpdir.mkdir('manifests')
    manifests_dir.join('backup.crd.yaml').write(
        'apiVersion: apiextensions.k8s.io/v1beta1\nkind: CustomResourceDefinition'
    )
    csv = manifests_dir.join('mig-operator.v1.1.1.clusterserviceversion.yaml')
    csv.write('apiVersion: operators.coreos.com/v1alpha1\nkind: ClusterServiceVersion')

    operator_manifest = OperatorManifest.from_directory(str(manifests_dir))
    build._adjust_csv_annotations(operator_manifest.files, 'amqp-streams', 'company-marketplace')

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


@mock.patch('iib.workers.tasks.build.yaml.dump')
def test_adjust_csv_annotations_no_customizations(mock_yaml_dump, tmpdir):
    manifests_dir = tmpdir.mkdir('manifests')
    manifests_dir.join('backup.crd.yaml').write(
        'apiVersion: apiextensions.k8s.io/v1beta1\nkind: CustomResourceDefinition'
    )
    csv = manifests_dir.join('mig-operator.v1.1.1.clusterserviceversion.yaml')
    csv.write('apiVersion: operators.coreos.com/v1alpha1\nkind: ClusterServiceVersion')

    operator_manifest = OperatorManifest.from_directory(str(manifests_dir))
    build._adjust_csv_annotations(operator_manifest.files, 'amqp-streams', 'mos-eisley')

    mock_yaml_dump.assert_not_called()


def test_add_ocp_label_to_index(tmpdir):
    operator_dir = tmpdir.mkdir('operator')
    dockerfile_txt = textwrap.dedent(
        '''\
        FROM scratch

        COPY manifests /manifests/
        COPY metadata/annotations.yaml /metadata/annotations.yaml

        LABEL operators.operatorframework.io.bundle.mediatype.v1=registry+v1
        LABEL operators.operatorframework.io.bundle.manifests.v1=manifests/
        LABEL operators.operatorframework.io.bundle.metadata.v1=metadata/
        LABEL operators.operatorframework.io.bundle.package.v1=my-operator
        LABEL operators.operatorframework.io.bundle.channels.v1=release-v1.1
        LABEL operators.operatorframework.io.bundle.channel.default.v1=release-v1.1

        # This last block are standard Red Hat container labels
        LABEL \
        com.redhat.component="my-operator-bundle-container" \
        version="v1.1" \
        name="my-operator-bundle" \
        License="ASL 2.0" \
        io.k8s.display-name="my-operator bundle" \
        io.k8s.description="demo of bundle" \
        summary="demo of bundle" \
        maintainer="John Doe <jdoe@redhat.com>"
        '''
    )
    operator_dir.join('Dockerfile').write(dockerfile_txt)

    build._add_ocp_label_to_index('v4.5', operator_dir, 'Dockerfile')

    expected = dockerfile_txt + '\nLABEL com.redhat.index.delivery.version="v4.5"\n'
    assert operator_dir.join('Dockerfile').read_text('utf-8') == expected


def test_get_missing_bundles_no_match():
    assert build._get_missing_bundles(
        [
            {
                'packageName': 'bundle1',
                'version': 'v1.0',
                'bundlePath': 'quay.io/pkg/pkg1@sha256:987654',
            },
            {
                'packageName': 'bundle2',
                'version': 'v2.0',
                'bundlePath': 'quay.io/pkg/pkg2@sha256:111111',
            },
        ],
        ['quay.io/ns/repo@sha256:123456'],
    ) == ['quay.io/ns/repo@sha256:123456']


def test_get_missing_bundles_match_hash():
    assert (
        build._get_missing_bundles(
            [
                {
                    'packageName': 'bundle1',
                    'version': 'v1.0',
                    'bundlePath': 'quay.io/pkg/pkg1@sha256:987654',
                },
                {
                    'packageName': 'bundle2',
                    'version': 'v2.0',
                    'bundlePath': 'quay.io/pkg/pkg2@sha256:111111',
                },
            ],
            ['quay.io/pkg/pkg1@sha256:987654'],
        )
        == []
    )


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_present_bundles(mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, tmpdir):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_gil.return_value = 'some-path'
    mock_run_cmd.side_effect = [
        'api.Registry.ListBundles',
        '{"packageName": "package1", "version": "v1.0"\n}'
        '\n{\n"packageName": "package2", "version": "v2.0"}',
    ]
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.return_value = 'address already in use'
    my_mock.poll.side_effect = [1, None]
    assert build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir)) == [
        {'packageName': 'package1', 'version': 'v1.0'},
        {'packageName': 'package2', 'version': 'v2.0'},
    ]
    assert mock_run_cmd.call_count == 2


@mock.patch('time.time')
@mock.patch('os.remove')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_present_bundles_grpc_not_initialize(
    mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, mock_remove, mock_time, tmpdir,
):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_run_cmd.side_effect = ['', '', '', '', ''] * 4
    mock_time.side_effect = list(range(1, 80))
    mock_gil.return_value = 'some-path'
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    with pytest.raises(IIBError, match='Index registry has not been initialized after 5 tries'):
        build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir))
    assert mock_run_cmd.call_count == 20


@mock.patch('time.time')
@mock.patch('os.remove')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_present_bundles_grpc_delayed_initialize(
    mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, mock_remove, mock_time, tmpdir,
):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_time.side_effect = [i * 0.5 for i in range(1, 80)]
    mock_gil.return_value = 'some-path'
    mock_run_cmd.side_effect = [
        '',
        '',
        '',
        '',
        '',
        '',
        'api.Registry.ListBundles',
        '{"packageName": "package1", "version": "v1.0"\n}'
        '\n{\n"packageName": "package2", "version": "v2.0"}',
    ]
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    assert build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir)) == [
        {'packageName': 'package1', 'version': 'v1.0'},
        {'packageName': 'package2', 'version': 'v2.0'},
    ]
    assert mock_run_cmd.call_count == 8


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_serve_image_registry(mock_run_cmd, mock_popen, mock_sleep, tmpdir):
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.side_effect = [
        'address already in use',
        'address already in use',
    ]
    mock_run_cmd.return_value = 'api.Registry.ListBundles'
    my_mock.poll.side_effect = [1, 1, None]
    port, _ = build._serve_index_registry('some_path.db')
    assert port == 50053
    assert my_mock.poll.call_count == 3


@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
def test_serve_image_registry_no_ports(mock_popen, mock_sleep, mock_config, tmpdir):
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.side_effect = [
        'address already in use',
        'address already in use',
        'address already in use',
    ]
    my_mock.poll.side_effect = [1, 1, 1, None]
    mock_config.return_value = {
        'iib_grpc_start_port': 50051,
        'iib_grpc_init_wait_time': 1,
        'iib_grpc_max_port_tries': 3,
        'iib_grpc_max_tries': 3,
    }
    with pytest.raises(IIBError, match='No free port has been found after 3 attempts.'):
        build._serve_index_registry('some_path.db')
    assert my_mock.poll.call_count == 3
