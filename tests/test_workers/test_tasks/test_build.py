# SPDX-License-Identifier: GPL-3.0-or-later
import re
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


@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build._get_local_pull_spec')
@mock.patch('iib.workers.tasks.build._run_cmd')
def test_push_arch_image(mock_run_cmd, mock_glps, mock_gwc):
    mock_gwc.return_value = {
        'iib_arch': 'amd64',
        'iib_arch_image_push_template': (
            'docker://{registry}/operator-registry-index:{request_id}-{arch}'
        ),
        'iib_registry': 'registry',
        'iib_registry_credentials': 'username:password',
    }
    mock_glps.return_value = 'source:tag'

    build._push_arch_image(3)

    mock_run_cmd.assert_called_once()
    push_args = mock_run_cmd.call_args[0][0]
    assert push_args[0:2] == ['podman', 'push']
    assert 'source:tag' in push_args
    assert 'docker://registry/operator-registry-index:3-amd64' in push_args


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build._fix_opm_path')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_arch_image')
@mock.patch('iib.workers.tasks.build._run_cmd')
def test_opm_index_add(mock_run_cmd, mock_pai, mock_bi, mock_fop, mock_cleanup, from_index):
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
    mock_cleanup.assert_called_once()
    mock_fop.assert_called_once()
    mock_bi.assert_called_once()
    mock_pai.assert_called_once()


@mock.patch('iib.workers.tasks.build.subprocess.run')
def test_mock_run_cmd(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 0
    mock_sub_run.return_value = mock_rv

    build._run_cmd(['echo', 'hello world'], {'cwd': '/some/path'})

    mock_sub_run.assert_called_once()


@pytest.mark.parametrize('exc_msg', (None, 'Houston, we have a problem!'))
@mock.patch('iib.workers.tasks.build.subprocess.run')
def test_mock_run_cmd_failed(mock_sub_run, exc_msg):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = 'some failure'
    mock_sub_run.return_value = mock_rv

    expected_exc = exc_msg or 'An unexpected error occurred'
    with pytest.raises(IIBError, match=expected_exc):
        build._run_cmd(['echo', 'hello world'], exc_msg=exc_msg)

    mock_sub_run.assert_called_once()
