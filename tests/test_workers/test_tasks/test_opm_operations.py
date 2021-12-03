# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import opm_operations


@pytest.fixture()
def mock_config():
    with mock.patch('iib.workers.tasks.opm_operations.get_worker_config') as mc:
        mc.return_value = {
            'iib_grpc_start_port': 50051,
            'iib_grpc_init_wait_time': 1,
            'iib_grpc_max_port_tries': 3,
            'iib_grpc_max_tries': 3,
        }
        yield mc


@mock.patch('iib.workers.tasks.opm_operations.socket')
def test_get_free_port(mock_socket):
    mock_socket.socket().bind.side_effect = [OSError(), OSError(), '']
    mock_socket.poll.side_effect = [1, 1, None]
    port = opm_operations._get_free_port(50051, 50054)
    assert port == 50053


@mock.patch('iib.workers.tasks.opm_operations.socket')
def test_get_free_port_no_port(mock_socket):
    mock_socket.socket().bind.side_effect = OSError()
    with pytest.raises(IIBError, match='No free port has been found after 3 attempts.'):
        opm_operations._get_free_port(50051, 50054)


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.opm_operations.socket')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_registry_serve(mock_run_cmd, mock_socket, mock_popen, mock_sleep):
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    mock_socket.socket().bind.side_effect = [OSError(), OSError(), '']
    mock_socket.poll.side_effect = [1, 1, None]
    mock_run_cmd.return_value = 'api.Registry.ListBundles'
    port, _ = opm_operations.opm_registry_serve(db_path='some_path.db')
    assert port == 50053
    assert mock_socket.socket().bind.call_count == 3


@mock.patch('iib.workers.tasks.opm_operations._serve_cmd_at_port')
@mock.patch('iib.workers.tasks.opm_operations.socket')
def test_opm_registry_serve_no_ports(mock_socket, mock_scap, mock_config):
    mock_socket.socket().bind.side_effect = OSError('OSError: [Errno 98] Address already in use')
    with pytest.raises(IIBError, match='No free port has been found after 3 attempts.'):
        opm_operations.opm_registry_serve(db_path='some_path.db')
    mock_scap.assert_not_called()


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.opm_operations.socket')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_serve(mock_run_cmd, mock_socket, mock_popen, mock_sleep):
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    mock_socket.socket().bind.side_effect = [OSError(), OSError(), '']
    mock_socket.poll.side_effect = [1, 1, None]
    mock_run_cmd.return_value = 'api.Registry.ListBundles'
    port, _ = opm_operations.opm_serve(catalog_dir='/some/dir')
    assert port == 50053
    assert mock_socket.socket().bind.call_count == 3


@mock.patch('iib.workers.tasks.opm_operations._serve_cmd_at_port')
@mock.patch('iib.workers.tasks.opm_operations.socket')
def test_opm_serve_no_ports(mock_socket, mock_scap, mock_config):
    mock_socket.socket().bind.side_effect = OSError('OSError: [Errno 98] Address already in use')
    with pytest.raises(IIBError, match='No free port has been found after 3 attempts.'):
        opm_operations.opm_serve(catalog_dir='/some/dir')
    mock_scap.assert_not_called()


@pytest.mark.parametrize('is_fbc', (True, False))
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.opm_operations.socket')
@mock.patch('iib.workers.tasks.opm_operations.get_catalog_dir')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
def test_opm_serve_from_index(
    mock_ifbc, mock_run_cmd, mock_gid, mock_cd, mock_socket, mock_popen, mock_sleep, tmpdir, is_fbc
):
    my_mock = mock.MagicMock()
    mock_ifbc.return_value = is_fbc
    mock_gid.return_value = "some.db"
    mock_cd.return_value = "/some/path"
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    mock_socket.socket().bind.side_effect = [OSError(), OSError(), '']
    mock_socket.poll.side_effect = [1, 1, None]
    mock_run_cmd.return_value = 'api.Registry.ListBundles'
    port, _ = opm_operations.opm_serve_from_index(
        base_dir=tmpdir, from_index='docker://test_pull_spec:latest'
    )
    assert port == 50053
    assert mock_socket.socket().bind.call_count == 3


@mock.patch('time.time')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_serve_cmd_at_port_not_initialize(
    mock_run_cmd, mock_popen, mock_sleep, mock_time, tmpdir, mock_config
):
    mock_run_cmd.side_effect = ['', '', '', '', ''] * 4
    mock_time.side_effect = list(range(1, 80))
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None

    cmd = ['opm', 'registry', 'serve', '-p', '50051', '-d', '/tmp/dummy.db', '-t', '/dev/null']
    with pytest.raises(IIBError, match='Index registry has not been initialized after 5 tries'):
        opm_operations._serve_cmd_at_port(" ".join(cmd), '/tmp', 50051, 5, 3)
    assert mock_run_cmd.call_count == 20


@mock.patch('time.time')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_serve_cmd_at_port_delayed_initialize(
    mock_run_cmd, mock_popen, mock_sleep, mock_time, tmpdir, mock_config
):
    mock_time.side_effect = [i * 0.5 for i in range(1, 80)]
    mock_run_cmd.side_effect = [
        '',
        '',
        '',
        '',
        '',
        '',
        'api.Registry.ListBundles',
    ]

    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None

    cmd = ['opm', 'registry', 'serve', '-p', '50051', '-d', '/tmp/dummy.db', '-t', '/dev/null']
    opm_operations._serve_cmd_at_port(" ".join(cmd), '/tmp', 50051, 5, 3)
    assert mock_run_cmd.call_count == 7
