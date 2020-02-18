# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import re
import textwrap
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build


@mock.patch('iib.workers.tasks.build._run_cmd')
def test_build_image(mock_run_cmd):
    build._build_image('/some/dir', 3)

    mock_run_cmd.assert_called_once()
    build_args = mock_run_cmd.call_args[0][0]
    assert build_args[0:2] == ['podman', 'build']
    assert '/some/dir/index.Dockerfile' in build_args


@mock.patch('iib.workers.tasks.build._run_cmd')
def test_cleanup(mock_run_cmd):
    build._cleanup()

    mock_run_cmd.assert_called_once()
    rmi_args = mock_run_cmd.call_args[0][0]
    assert rmi_args[0:2] == ['podman', 'rmi']


@mock.patch('iib.workers.tasks.build.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build._run_cmd')
def test_create_and_push_manifest_list(mock_run_cmd, mock_td, tmp_path):
    mock_td.return_value.__enter__.return_value = tmp_path

    build._create_and_push_manifest_list(3, {'amd64', 's390x'})

    expected_manifest = textwrap.dedent(
        '''\
        image: registry:8443/operator-registry-index:3
        manifests:
        - image: registry:8443/operator-registry-index:3-amd64
          platform:
            architecture: amd64
            os: linux
        - image: registry:8443/operator-registry-index:3-s390x
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


@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.update_request')
def test_finish_request_post_build(mock_ur, mock_capml, mock_srs):
    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec
    request_id = 2
    arches = {'amd64'}
    build._finish_request_post_build(request_id, arches)

    mock_srs.assert_called_once()
    mock_capml.assert_called_once_with(request_id, arches)
    mock_ur.assert_called_once()
    update_request_payload = mock_ur.call_args[0][1]
    assert update_request_payload.keys() == {'index_image', 'state', 'state_reason'}
    assert update_request_payload['index_image'] == output_pull_spec


def test_fix_opm_path(tmpdir):
    dockerfile = tmpdir.join('index.Dockerfile')
    dockerfile.write('FROM image as builder\nFROM scratch\nCOPY --from=builder /build/bin/opm /opm')

    build._fix_opm_path(str(tmpdir))

    assert dockerfile.read() == (
        'FROM image as builder\nFROM scratch\nCOPY --from=builder /bin/opm /opm'
    )


@pytest.mark.parametrize('request_id', (1, 5))
def test_get_local_pull_spec(request_id):
    rv = build._get_local_pull_spec(request_id)

    assert re.match(f'.+:{request_id}', rv)


@mock.patch('iib.workers.tasks.build._skopeo_inspect')
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


@mock.patch('iib.workers.tasks.build._skopeo_inspect')
def test_get_image_arches_manifest(mock_si):
    mock_si.side_effect = [
        {'mediaType': 'application/vnd.docker.distribution.manifest.v2+json'},
        {'Architecture': 'amd64'},
    ]
    rv = build._get_image_arches('image:latest')
    assert rv == {'amd64'}


@mock.patch('iib.workers.tasks.build._skopeo_inspect')
def test_get_image_arches_not_manifest_list(mock_si):
    mock_si.return_value = {'mediaType': 'application/vnd.docker.distribution.notmanifest.v2+json'}
    with pytest.raises(IIBError, match='.+is neither a v2 manifest list nor a v2 manifest'):
        build._get_image_arches('image:latest')


@mock.patch('iib.workers.tasks.build._skopeo_inspect')
def test_get_resolved_image(mock_si):
    mock_si.return_value = {'Digest': 'sha256:abcdefg', 'Name': 'some-image'}
    rv = build._get_resolved_image('some-image')
    assert rv == 'some-image@sha256:abcdefg'


@mock.patch('iib.workers.tasks.build.time.sleep')
@mock.patch('iib.workers.tasks.build.get_request')
def test_poll_request(mock_gr, mock_sleep):
    mock_gr.side_effect = [
        {'arches': [], 'state': 'in_progress'},
        {'arches': ['amd64'], 'state': 'in_progress'},
        {'arches': ['s390x'], 'state': 'in_progress'},
    ]

    assert build._poll_request(3, {'amd64', 's390x'}) is True
    mock_sleep.call_count == 3
    mock_gr.call_count == 3


@mock.patch('iib.workers.tasks.build.time.sleep')
@mock.patch('iib.workers.tasks.build.get_request')
def test_poll_request_request_failed(mock_gr, mock_sleep):
    mock_gr.side_effect = [
        {'arches': [], 'state': 'in_progress'},
        {'arches': [], 'state': 'failed'},
    ]

    assert build._poll_request(3, {'amd64', 's390x'}) is False
    mock_sleep.call_count == 2
    mock_gr.call_count == 2


@pytest.mark.parametrize(
    'add_arches, from_index, from_index_arches',
    (
        ([], 'some-index:latest', {'amd64'}),
        (['amd64', 's390x'], None, set()),
        (['s390x'], 'some-index:latest', {'amd64'}),
    ),
)
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._get_image_arches')
@mock.patch('iib.workers.tasks.build.update_request')
def test_prepare_request_for_build(
    mock_ur, mock_gia, mock_gri, mock_srs, add_arches, from_index, from_index_arches,
):
    binary_image_resolved = 'binary-image@sha256:abcdef'
    from_index_resolved = None
    expected_arches = set(add_arches) | from_index_arches
    expected_payload_keys = {'binary_image_resolved', 'state', 'state_reason'}
    if from_index:
        from_index_name = from_index.split(':', 1)[0]
        from_index_resolved = f'{from_index_name}@sha256:bcdefg'
        mock_gri.side_effect = [binary_image_resolved, from_index_resolved]
        mock_gia.side_effect = [expected_arches, from_index_arches]
        expected_payload_keys.add('from_index_resolved')
    else:
        mock_gri.side_effect = [binary_image_resolved]
        mock_gia.side_effect = [expected_arches]

    rv = build._prepare_request_for_build('binary-image:latest', 1, from_index, add_arches)
    assert rv == {
        'arches': expected_arches,
        'binary_image_resolved': binary_image_resolved,
        'from_index_resolved': from_index_resolved,
    }
    mock_ur.assert_called_once()
    update_request_payload = mock_ur.call_args[0][1]
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
def test_prepare_request_for_build_no_arch_worker(mock_gia, mock_gri, mock_srs):
    mock_gia.side_effect = [{'amd64', 'arm64'}]

    expected = 'Building for the following requested arches is not supported.+'
    with pytest.raises(IIBError, match=expected):
        build._prepare_request_for_build('binary-image:latest', 1, add_arches=['arm64'])


@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._get_resolved_image')
@mock.patch('iib.workers.tasks.build._get_image_arches')
def test_prepare_request_for_build_binary_image_no_arch(mock_gia, mock_gri, mock_srs):
    mock_gia.side_effect = [{'amd64'}]

    expected = 'The binary image is not available for the following arches.+'
    with pytest.raises(IIBError, match=expected):
        build._prepare_request_for_build('binary-image:latest', 1, add_arches=['s390x'])


@mock.patch('iib.workers.tasks.build._get_local_pull_spec')
@mock.patch('iib.workers.tasks.build._run_cmd')
def test_push_arch_image(mock_run_cmd, mock_glps):
    mock_glps.return_value = 'source:tag'

    build._push_arch_image(3)

    mock_run_cmd.assert_called_once()
    push_args = mock_run_cmd.call_args[0][0]
    assert push_args[0:2] == ['podman', 'push']
    assert 'source:tag' in push_args
    assert 'docker://registry:8443/operator-registry-index:3-amd64' in push_args


@pytest.mark.parametrize('use_creds', (True, False))
@mock.patch('iib.workers.tasks.build._run_cmd')
def test_skopeo_inspect(mock_run_cmd, use_creds):
    mock_run_cmd.return_value = '{"Name": "some-image"}'
    image = 'docker://some-image:latest'
    rv = build._skopeo_inspect(image, use_creds=use_creds)
    assert rv == {"Name": "some-image"}
    skopeo_args = mock_run_cmd.call_args[0][0]
    expected = ['skopeo', 'inspect', image]
    if use_creds:
        expected += ['--creds', 'iib:iibpassword']

    assert skopeo_args == expected


@pytest.mark.parametrize('request_succeeded', (True, False))
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build.opm_index_add')
@mock.patch('iib.workers.tasks.build._poll_request')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._finish_request_post_build')
def test_handle_add_request(
    mock_frpb, mock_vii, mock_pr, mock_oia, mock_prfb, request_succeeded,
):
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
    }
    mock_pr.return_value = request_succeeded

    build.handle_add_request(
        ['some-bundle:2.3-1'], 'binary-image:latest', 3, 'from-index:latest', ['s390x']
    )

    mock_prfb.assert_called_once()
    mock_oia.apply_async.call_count == 2
    # Verify opm_index_add was scheduled on the correct workers
    for i, arch in enumerate(sorted(arches)):
        assert mock_oia.apply_async.call_args_list[i][1]['queue'] == f'iib_{arch}'
        assert mock_oia.apply_async.call_args_list[i][1]['routing_key'] == f'iib_{arch}'
    mock_pr.assert_called_once()
    if request_succeeded:
        mock_frpb.assert_called_once()
        mock_vii.assert_called_once()
    else:
        mock_frpb.assert_not_called()
        mock_vii.assert_not_called()


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@mock.patch('iib.workers.tasks.build.get_request')
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._fix_opm_path')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_arch_image')
@mock.patch('iib.workers.tasks.build._run_cmd')
@mock.patch('iib.workers.tasks.build.update_request')
def test_opm_index_add(
    mock_ur, mock_run_cmd, mock_pai, mock_bi, mock_fop, mock_cleanup, mock_gr, from_index
):
    mock_gr.return_value = {'state': 'in_progress'}
    binary_images = ['bundle:1.2', 'bundle:1.3']
    build.opm_index_add(binary_images, 'binary-image:latest', 3, from_index=from_index)

    # This is only directly called once in the actual function
    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'add']
    assert ','.join(binary_images) in opm_args
    if from_index:
        assert '--from-index' in opm_args
        assert from_index in opm_args
    else:
        assert '--from-index' not in opm_args
    mock_gr.assert_called_once_with(3)
    mock_cleanup.assert_called_once()
    mock_fop.assert_called_once()
    mock_bi.assert_called_once()
    mock_pai.assert_called_once()
    mock_ur.assert_called_once()


@mock.patch('iib.workers.tasks.build.get_request')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._build_image')
def test_opm_index_add_already_failed(mock_bi, mock_srs, mock_gr):
    mock_gr.return_value = {'state': 'failed'}
    binary_images = ['bundle:1.2', 'bundle:1.3']
    build.opm_index_add(binary_images, 'binary-image:latest', 3)

    mock_srs.assert_called_once()
    mock_gr.assert_called_once_with(3)
    mock_bi.assert_not_called()


@mock.patch('iib.workers.tasks.build.subprocess.run')
def test_run_cmd(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 0
    mock_sub_run.return_value = mock_rv

    build._run_cmd(['echo', 'hello world'], {'cwd': '/some/path'})

    mock_sub_run.assert_called_once()


@pytest.mark.parametrize('exc_msg', (None, 'Houston, we have a problem!'))
@mock.patch('iib.workers.tasks.build.subprocess.run')
def test_run_cmd_failed(mock_sub_run, caplog, exc_msg):
    # When running tests that involve Flask before this test, the iib.workers loggers
    # are disabled. This is an ugly workaround.
    for logger in ('iib.workers', 'iib.workers.tasks', 'iib.workers.tasks.build'):
        logging.getLogger(logger).disabled = False

    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = 'some failure'
    mock_sub_run.return_value = mock_rv

    expected_exc = exc_msg or 'An unexpected error occurred'
    with pytest.raises(IIBError, match=expected_exc):
        build._run_cmd(['echo', 'iib:iibpassword'], exc_msg=exc_msg)

    mock_sub_run.assert_called_once()
    # Verify that the password is not logged
    assert '********' in caplog.text
    assert 'iib:iibpassword' not in caplog.text


@pytest.mark.parametrize('request_succeeded', (True, False))
@mock.patch('iib.workers.tasks.build._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build.opm_index_rm')
@mock.patch('iib.workers.tasks.build._poll_request')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._finish_request_post_build')
def test_handle_rm_request(
    mock_frpb, mock_vii, mock_pr, mock_oir, mock_prfb, request_succeeded,
):
    arches = {'amd64', 's390x'}
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
    }
    mock_pr.return_value = request_succeeded
    build.handle_rm_request(['some-operator'], 'binary-image:latest', 3, 'from-index:latest')

    mock_prfb.assert_called_once()
    mock_oir.apply_async.call_count == 2
    # Verify opm_index_add was scheduled on the correct workers
    for i, arch in enumerate(sorted(arches)):
        assert mock_oir.apply_async.call_args_list[i][1]['queue'] == f'iib_{arch}'
        assert mock_oir.apply_async.call_args_list[i][1]['routing_key'] == f'iib_{arch}'
    mock_pr.assert_called_once()
    if request_succeeded:
        mock_vii.assert_called_once()
        mock_frpb.assert_called_once()
    else:
        mock_vii.assert_not_called()
        mock_frpb.assert_not_called()


@mock.patch('iib.workers.tasks.build.get_request')
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._fix_opm_path')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_arch_image')
@mock.patch('iib.workers.tasks.build._run_cmd')
@mock.patch('iib.workers.tasks.build.update_request')
def test_opm_index_rm(mock_ur, mock_run_cmd, mock_pai, mock_bi, mock_fop, mock_cleanup, mock_gr):
    mock_gr.return_value = {'state': 'in_progress'}
    operators = ['operator_1', 'operator_2']
    build.opm_index_rm(operators, 'binary-image:latest', 3, 'some_index:latest')

    # This is only directly called once in the actual function
    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'rm']
    assert ','.join(operators) in opm_args
    assert 'some_index:latest' in opm_args
    mock_gr.assert_called_once_with(3)
    mock_cleanup.assert_called_once()
    mock_fop.assert_called_once()
    mock_bi.assert_called_once()
    mock_pai.assert_called_once()
    mock_ur.assert_called_once()


@mock.patch('iib.workers.tasks.build.get_request')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._build_image')
def test_opm_index_rm_already_failed(mock_bi, mock_srs, mock_gr):
    mock_gr.return_value = {'state': 'failed'}
    operators = ['operator_1', 'operator_2']
    build.opm_index_rm(operators, 'binary-image:latest', 3, 'from:index')

    mock_srs.assert_called_once()
    mock_gr.assert_called_once_with(3)
    mock_bi.assert_not_called()


@mock.patch('iib.workers.tasks.build._get_resolved_image')
def test_verify_index_image_failure(mock_ri):
    mock_ri.return_value = 'image:works'
    match_str = (
        'The supplied from_index image changed during the IIB request.'
        ' Please resubmit the request.'
    )
    with pytest.raises(IIBError, match=match_str):
        build._verify_index_image('image:doesnt_work', 'unresolved_image')
