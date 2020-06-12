# SPDX-License-Identifier: GPL-3.0-or-later
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
def test_cleanup(mock_run_cmd):
    build._cleanup()

    mock_run_cmd.assert_called_once()
    rmi_args = mock_run_cmd.call_args[0][0]
    assert rmi_args[0:2] == ['podman', 'rmi']


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
    'iib_index_image_output_registry, from_index, overwrite, overwrite_token, expected',
    (
        (None, None, False, None, '{default}'),
        (
            'registry-proxy.domain.local',
            None,
            False,
            None,
            'registry-proxy.domain.local/{default_no_registry}',
        ),
        (None, 'quay.io/ns/iib:v4.5', True, None, 'quay.io/ns/iib:v4.5'),
        (None, 'quay.io/ns/iib:v5.4', True, 'username:password', 'quay.io/ns/iib:v5.4'),
    ),
)
@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build._skopeo_copy')
@mock.patch('iib.workers.tasks.build.update_request')
@mock.patch('iib.workers.tasks.build.set_request_state')
def test_update_index_image_pull_spec(
    mock_srs,
    mock_ur,
    mock_sc,
    mock_gwc,
    iib_index_image_output_registry,
    from_index,
    overwrite,
    overwrite_token,
    expected,
):
    default_no_registry = 'namespace/some-image:3'
    default = f'quay.io/{default_no_registry}'
    expected_pull_spec = expected.format(default=default, default_no_registry=default_no_registry)
    request_id = 2
    arches = {'amd64'}
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
    if overwrite_token:
        mock_sc.assert_called_once_with(
            f'docker://{default}',
            f'docker://{expected_pull_spec}',
            copy_all=True,
            dest_token='username:password',
            exc_msg=mock.ANY,
        )
    elif overwrite:
        mock_sc.assert_called_once_with(
            f'docker://{default}',
            f'docker://{expected_pull_spec}',
            copy_all=True,
            dest_token=None,
            exc_msg=mock.ANY,
        )
        mock_srs.assert_called_once()
    else:
        mock_sc.assert_not_called()
        mock_srs.assert_not_called()


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
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_opm_index_add(mock_run_cmd, from_index):
    bundles = ['bundle:1.2', 'bundle:1.3']
    build._opm_index_add('/tmp/somedir', bundles, 'binary-image:latest', from_index=from_index)

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'add']
    assert ','.join(bundles) in opm_args
    if from_index:
        assert '--from-index' in opm_args
        assert from_index in opm_args
    else:
        assert '--from-index' not in opm_args


@mock.patch('iib.workers.tasks.build.run_cmd')
def test_opm_index_rm(mock_run_cmd):
    operators = ['operator_1', 'operator_2']
    build._opm_index_rm('/tmp/somedir', operators, 'binary-image:latest', 'some_index:latest')

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'rm']
    assert ','.join(operators) in opm_args
    assert 'some_index:latest' in opm_args


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
    if expected_bundle_mapping:
        expected_payload_keys.add('bundle_mapping')
    if from_index:
        from_index_name = from_index.split(':', 1)[0]
        from_index_resolved = f'{from_index_name}@sha256:bcdefg'
        mock_gri.side_effect = [binary_image_resolved, from_index_resolved]
        mock_gia.side_effect = [expected_arches, from_index_arches]
        expected_payload_keys.add('from_index_resolved')
    else:
        mock_gri.side_effect = [binary_image_resolved]
        mock_gia.side_effect = [expected_arches]

    if bundles:
        mock_gil.side_effect = [bundle.rsplit('/', 1)[1].split(':', 1)[0] for bundle in bundles]

    rv = build._prepare_request_for_build('binary-image:latest', 1, from_index, add_arches, bundles)
    assert rv == {
        'arches': expected_arches,
        'binary_image_resolved': binary_image_resolved,
        'from_index_resolved': from_index_resolved,
    }
    mock_ur.assert_called_once()
    update_request_payload = mock_ur.call_args[0][1]
    if expected_bundle_mapping:
        assert update_request_payload['bundle_mapping'] == expected_bundle_mapping
    else:
        assert 'bundle_mapping' not in update_request_payload

    assert update_request_payload.keys() == expected_payload_keys


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


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._verify_labels')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
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
def test_handle_add_request(
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
    mock_prfb,
    mock_vl,
    mock_cleanup,
):
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
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
        False,
        None,
        greenwave_config,
    )

    mock_cleanup.assert_called_once()
    mock_vl.assert_called_once()
    mock_prfb.assert_called_once()
    mock_gb.assert_called_once()

    add_args = mock_oia.call_args[0]
    assert ['some-bundle@sha'] in add_args
    mock_oia.assert_called_once()

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
    assert mock_srs.call_count == 3


@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build._verify_labels')
@mock.patch('iib.workers.tasks.build._get_resolved_bundles')
def test_handle_add_request_gating_failure(mock_grb, mock_vl, mock_gb, mock_srs):
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
            False,
            None,
            greenwave_config,
        )
    mock_srs.assert_called_once()
    mock_vl.assert_called_once()
    mock_gb.assert_called_once_with(['some-bundle@sha'], greenwave_config)


@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_bundles')
def test_handle_add_request_bundle_resolution_failure(mock_grb, mock_srs):
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
    mock_srs.assert_called_once()
    mock_grb.assert_called_once_with(bundles)


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._opm_index_rm')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
def test_handle_rm_request(
    mock_uiips, mock_capml, mock_srs, mock_vii, mock_pi, mock_bi, mock_oir, mock_prfb, mock_cleanup
):
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
    }
    build.handle_rm_request(['some-operator'], 'binary-image:latest', 3, 'from-index:latest')

    mock_cleanup.assert_called_once()
    mock_prfb.assert_called_once()
    mock_oir.assert_called_once()
    assert mock_bi.call_count == len(arches)
    assert mock_pi.call_count == len(arches)
    mock_vii.assert_called_once()
    assert mock_srs.call_count == 2
    mock_capml.assert_called_once()
    mock_uiips.assert_called_once()
    assert mock_srs.call_args[0][1] == 'complete'


@mock.patch('iib.workers.tasks.build._get_resolved_image')
def test_verify_index_image_failure(mock_ri):
    mock_ri.return_value = 'image:works'
    match_str = (
        'The supplied from_index image changed during the IIB request.'
        ' Please resubmit the request.'
    )
    with pytest.raises(IIBError, match=match_str):
        build._verify_index_image('image:doesnt_work', 'unresolved_image')


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
    'iib_index_image_output_registry, expected_bundle_image',
    ((None, 'quay.io/iib:99'), ('dagobah.domain.local', 'dagobah.domain.local/iib:99')),
)
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
    iib_index_image_output_registry,
    expected_bundle_image,
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

    mock_aob.assert_called_once()

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

    assert labels == {'operators.operatorframework.io.bundle.package.v1': 'amqstreams-cmp'}
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
