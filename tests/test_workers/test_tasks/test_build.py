# SPDX-License-Identifier: GPL-3.0-or-later
import os
import re
import textwrap
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build


@mock.patch('iib.workers.tasks.build.run_cmd')
def test_build_image(mock_run_cmd):
    build._build_image('/some/dir', 3, 'amd64')

    mock_run_cmd.assert_called_once()
    build_args = mock_run_cmd.call_args[0][0]
    assert build_args[0:2] == ['buildah', 'bud']
    assert '/some/dir/index.Dockerfile' in build_args


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


@mock.patch('iib.workers.tasks.build.update_request')
def test_finish_request_post_build(mock_ur):
    output_pull_spec = 'quay.io/namespace/some-image:3'
    request_id = 2
    arches = {'amd64'}
    build._finish_request_post_build(output_pull_spec, request_id, arches)

    mock_ur.assert_called_once()
    update_request_payload = mock_ur.call_args[0][1]
    assert update_request_payload.keys() == {'arches', 'index_image', 'state', 'state_reason'}
    assert update_request_payload['index_image'] == output_pull_spec


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
        {'Architecture': 'amd64'},
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
    mock_si.return_value = {'Labels': {'some_label': 'value'}}
    assert build.get_image_label('some-image:latest', label) == expected


@mock.patch('iib.workers.tasks.build.skopeo_inspect')
def test_get_resolved_image(mock_si):
    mock_si.return_value = {'Digest': 'sha256:abcdefg', 'Name': 'some-image'}
    rv = build._get_resolved_image('some-image')
    assert rv == 'some-image@sha256:abcdefg'


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
    expected_payload_keys = {'binary_image_resolved', 'bundle_mapping', 'state', 'state_reason'}
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
    assert update_request_payload['bundle_mapping'] == expected_bundle_mapping
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


@mock.patch('iib.workers.tasks.build._get_local_pull_spec')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_push_image(mock_run_cmd, mock_glps):
    mock_glps.return_value = 'source:tag'

    build._push_image(3, 'amd64')

    mock_run_cmd.assert_called_once()
    push_args = mock_run_cmd.call_args[0][0]
    assert push_args[0:2] == ['podman', 'push']
    assert 'source:tag' in push_args
    assert 'docker://registry:8443/iib-build:3-amd64' in push_args


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._verify_labels')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._opm_index_add')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._finish_request_post_build')
@mock.patch('iib.workers.tasks.build.opm_index_export')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.get_legacy_support_packages')
@mock.patch('iib.workers.tasks.build.validate_legacy_params_and_config')
def test_handle_add_request(
    mock_vlpc,
    mock_glsp,
    mock_capml,
    mock_srs,
    mock_oie,
    mock_frpb,
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
    legacy_packages = {'some_package'}
    mock_glsp.return_value = legacy_packages
    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec

    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    build.handle_add_request(
        bundles, 'binary-image:latest', 3, 'from-index:latest', ['s390x'], cnr_token, organization,
    )

    mock_cleanup.assert_called_once()
    mock_vl.assert_called_once()
    mock_prfb.assert_called_once()

    add_args = mock_oia.call_args[0]
    assert bundles in add_args
    mock_oia.assert_called_once()

    assert mock_bi.call_count == len(arches)
    assert mock_pi.call_count == len(arches)

    mock_oie.assert_called_once()
    export_args = mock_oie.call_args[0]
    assert legacy_packages in export_args
    assert cnr_token in export_args
    assert organization in export_args

    mock_frpb.assert_called_once()
    mock_vii.assert_called_once()
    mock_capml.assert_called_once()
    mock_srs.assert_called_once()


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._opm_index_rm')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build._finish_request_post_build')
def test_handle_rm_request(
    mock_frpb, mock_capml, mock_srs, mock_vii, mock_pi, mock_bi, mock_oir, mock_prfb, mock_cleanup
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
    mock_srs.assert_called_once()
    mock_capml.assert_called_once()
    mock_frpb.assert_called_once()


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
