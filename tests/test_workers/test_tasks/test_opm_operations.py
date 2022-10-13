# SPDX-License-Identifier: GPL-3.0-or-later
import os.path
import pytest

from unittest import mock

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
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


@mock.patch('iib.workers.tasks.opm_operations.shutil.rmtree')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_migrate(
    mock_run_cmd,
    moch_srmtree,
    tmpdir,
):
    index_db_file = os.path.join(tmpdir, 'database/index.db')

    opm_operations.opm_migrate(index_db_file, tmpdir)
    moch_srmtree.assert_not_called()

    mock_run_cmd.assert_called_once_with(
        ['opm', 'migrate', index_db_file, os.path.join(tmpdir, 'catalog')],
        {'cwd': tmpdir},
        exc_msg='Failed to migrate index.db to file-based catalog',
    )


@pytest.mark.parametrize("dockerfile", (None, 'index.Dockerfile'))
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_generate_dockerfile(mock_run_cmd, tmpdir, dockerfile):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')

    def create_dockerfile(*args, **kwargs):
        with open(os.path.join(tmpdir, 'catalogs.Dockerfile'), 'a'):
            pass

    mock_run_cmd.side_effect = create_dockerfile

    opm_operations.opm_generate_dockerfile(
        fbc_dir, tmpdir, index_db_file, "some:image", dockerfile_name=dockerfile
    )

    df_name = dockerfile if dockerfile else f"{os.path.basename(fbc_dir)}.Dockerfile"

    mock_run_cmd.assert_called_once_with(
        ['opm', 'generate', 'dockerfile', fbc_dir, '--binary-image', 'some:image'],
        {'cwd': tmpdir},
        exc_msg='Failed to generate Dockerfile for file-based catalog',
    )

    df_path = os.path.join(tmpdir, df_name)
    with open(df_path, 'r') as f:
        assert any(line.find('/var/lib/iib/_hidden/do.not.edit.db') != -1 for line in f.readlines())


@pytest.mark.parametrize("set_index_db_file", (False, True))
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_generate_dockerfile_no_dockerfile(mock_run_cmd, tmpdir, set_index_db_file):
    index_db_file = os.path.join(tmpdir, 'database/index.db') if set_index_db_file else None
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    df_path = os.path.join(tmpdir, f"{os.path.basename(fbc_dir)}.Dockerfile")

    with pytest.raises(IIBError, match=f"Cannot find generated Dockerfile at {df_path}"):
        opm_operations.opm_generate_dockerfile(
            fbc_dir,
            tmpdir,
            index_db_file,
            "some:image",
        )

    mock_run_cmd.assert_called_once_with(
        ['opm', 'generate', 'dockerfile', fbc_dir, '--binary-image', 'some:image'],
        {'cwd': tmpdir},
        exc_msg='Failed to generate Dockerfile for file-based catalog',
    )


@pytest.mark.parametrize("set_index_db_file", (False, True))
@pytest.mark.parametrize("dockerfile", (None, 'index.Dockerfile'))
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_generate_dockerfile_exist(mock_run_cmd, tmpdir, dockerfile, set_index_db_file):
    index_db_file = os.path.join(tmpdir, 'database/index.db') if set_index_db_file else None
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    df_name = f"{os.path.basename(fbc_dir)}.Dockerfile" if not dockerfile else dockerfile
    df_path = os.path.join(tmpdir, df_name)

    # create Dockerfile for tests
    with open(df_path, 'a'):
        pass

    opm_operations.opm_generate_dockerfile(
        fbc_dir, tmpdir, index_db_file, "some:image", dockerfile_name=dockerfile
    )

    mock_run_cmd.assert_not_called()


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('overwrite_csv', (True, False))
@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@mock.patch('iib.workers.tasks.utils.set_registry_token')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_registry_add(
    mock_run_cmd, mock_srt, from_index, bundles, overwrite_csv, container_tool
):
    opm_operations._opm_registry_add(
        base_dir='/tmp/somedir',
        index_db='/tmp/somedir/some.db',
        bundles=bundles,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[:3] == ['opm', 'registry', 'add']
    if bundles:
        assert ','.join(bundles) in opm_args
    else:
        assert '""' in opm_args
    if overwrite_csv:
        assert '--overwrite-latest' in opm_args
    else:
        assert '--overwrite-latest' not in opm_args
    if container_tool:
        assert '--container-tool' in opm_args
        assert container_tool in opm_args
    else:
        assert '--container-tool' not in opm_args
    assert "--enable-alpha" in opm_args


@pytest.mark.parametrize('is_fbc', (True, False))
@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('overwrite_csv', (True, False))
@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@mock.patch('iib.workers.tasks.opm_operations.opm_generate_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_add')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
def test_opm_registry_add_fbc(
    mock_iifbc,
    mock_ghid,
    mock_gid,
    mock_ora,
    mock_om,
    mock_ogd,
    from_index,
    bundles,
    overwrite_csv,
    container_tool,
    is_fbc,
    tmpdir,
):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    mock_ghid.return_value = index_db_file
    mock_gid.return_value = index_db_file
    mock_om.return_value = fbc_dir
    mock_iifbc.return_value = is_fbc

    opm_operations.opm_registry_add_fbc(
        base_dir=tmpdir,
        bundles=bundles,
        binary_image="some:image",
        from_index=from_index,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
    )

    mock_ora.assert_called_once_with(
        base_dir=tmpdir,
        index_db=index_db_file,
        bundles=bundles,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
    )

    mock_om.assert_called_once_with(index_db=index_db_file, base_dir=tmpdir)
    mock_ogd.assert_called_once_with(
        fbc_dir=fbc_dir,
        base_dir=tmpdir,
        index_db=index_db_file,
        binary_image="some:image",
        dockerfile_name='index.Dockerfile',
    )


@pytest.mark.parametrize('operators', (['abc-operator', 'xyz-operator'], []))
@mock.patch('iib.workers.tasks.opm_operations.opm_generate_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_rm')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
@mock.patch('iib.workers.tasks.utils.set_registry_token')
def test_opm_registry_rm_fbc(
    mock_srt,
    mock_ghid,
    mock_orr,
    mock_om,
    mock_ogd,
    tmpdir,
    operators,
):
    from_index = 'some_index:latest'
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    mock_ghid.return_value = index_db_file
    mock_om.return_value = fbc_dir

    opm_operations.opm_registry_rm_fbc(
        tmpdir, from_index, operators, 'some:image', overwrite_from_index_token='some_token'
    )

    mock_orr.assert_called_once_with(
        index_db_file,
        operators,
        tmpdir,
    )

    mock_srt.assert_called_once_with('some_token', 'some_index:latest', append=True)
    mock_om.assert_called_once_with(index_db=index_db_file, base_dir=tmpdir)
    mock_ogd.assert_called_once_with(
        fbc_dir=fbc_dir,
        base_dir=tmpdir,
        index_db=index_db_file,
        binary_image='some:image',
        dockerfile_name='index.Dockerfile',
    )


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_registry_rm(mock_run_cmd):
    packages = ['abc-operator', 'xyz-operator']
    opm_operations._opm_registry_rm(
        '/tmp/somedir/some.db',
        packages,
        '/tmp/somedir',
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[:3] == ['opm', 'registry', 'rm']
    assert ','.join(packages) in opm_args


@pytest.mark.parametrize(
    'from_index, is_fbc', [('some-fbc-index:latest', True), ('some-sqlite-index:latest', False)]
)
@mock.patch('iib.workers.tasks.opm_operations.opm_generate_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations._opm_registry_rm')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
@mock.patch('iib.workers.tasks.build._get_index_database')
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
@mock.patch('iib.workers.tasks.opm_operations.set_request_state')
def test_opm_create_empty_fbc(
    mock_srs, mock_iif, mock_gid, mock_ghid, mock_orr, mock_om, mock_ogd, tmpdir, from_index, is_fbc
):

    operators = ['abc-operator', 'xyz-operator']
    mock_iif.return_value = is_fbc
    hidden_index_db_file = os.path.join(tmpdir, 'hidden/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    mock_ghid.return_value = hidden_index_db_file
    mock_om.return_value = fbc_dir

    index_db_file = os.path.join(tmpdir, 'database/index.db')
    mock_gid.return_value = index_db_file

    opm_operations.opm_create_empty_fbc(3, tmpdir, from_index, from_index, 'some:image', operators)

    if is_fbc:
        mock_orr.assert_called_once_with(
            index_db_path=hidden_index_db_file, operators=operators, base_dir=tmpdir
        )
        mock_ghid.assert_called_once()
    else:
        mock_gid.assert_called_once()
        mock_orr.assert_called_once_with(
            index_db_path=index_db_file, operators=operators, base_dir=tmpdir
        )


@pytest.mark.parametrize("from_index", (None, "image:latest"))
@pytest.mark.parametrize("db_exist", (True, False))
@mock.patch('iib.workers.tasks.opm_operations.is_image_fbc')
@mock.patch('iib.workers.tasks.opm_operations.get_hidden_index_database')
def test_get_or_create_temp_index_db_file(mock_ghid, mock_iifbc, db_exist, from_index, tmpdir):
    def create_index_db(*args, **kwargs):
        db_file = os.path.join(tmpdir, get_worker_config()['temp_index_db_path'])
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        with open(db_file, 'w'):
            pass
        return db_file

    mock_ghid.side_effect = create_index_db
    mock_iifbc.return_value = True

    if db_exist:
        create_index_db()

    index_db_file = opm_operations._get_or_create_temp_index_db_file(
        base_dir=tmpdir, from_index=from_index
    )
    assert os.path.isfile(index_db_file)


@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_opm_registry_deprecatetruncate(mock_run_cmd, bundles):
    index_db_file = '/tmp/test_file.db'
    cmd = [
        'opm',
        'registry',
        'deprecatetruncate',
        '--database',
        index_db_file,
        '--bundle-images',
        ','.join(bundles),
        '--allow-package-removal',
    ]

    opm_operations.opm_registry_deprecatetruncate(
        base_dir='/tmp',
        index_db=index_db_file,
        bundles=bundles,
    )

    mock_run_cmd.assert_called_once_with(
        cmd, {'cwd': '/tmp'}, exc_msg=f'Failed to deprecate the bundles on {index_db_file}'
    )


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@mock.patch('iib.workers.tasks.opm_operations.opm_generate_dockerfile')
@mock.patch('iib.workers.tasks.opm_operations.opm_migrate')
@mock.patch('iib.workers.tasks.opm_operations.opm_registry_deprecatetruncate')
@mock.patch('iib.workers.tasks.opm_operations._get_or_create_temp_index_db_file')
def test_deprecate_bundles_fbc(
    mock_gtidf,
    mock_ord,
    mock_om,
    mock_ogd,
    from_index,
    bundles,
    tmpdir,
):
    index_db_file = os.path.join(tmpdir, 'database/index.db')
    fbc_dir = os.path.join(tmpdir, 'catalogs')
    mock_gtidf.return_value = index_db_file
    mock_om.return_value = fbc_dir

    opm_operations.deprecate_bundles_fbc(
        bundles=bundles,
        base_dir=tmpdir,
        binary_image="some:image",
        from_index=from_index,
    )

    mock_ord.assert_called_once_with(base_dir=tmpdir, index_db=index_db_file, bundles=bundles)

    mock_om.assert_called_once_with(index_db_file, tmpdir)
    mock_ogd.assert_called_once_with(
        fbc_dir=fbc_dir,
        base_dir=tmpdir,
        index_db=index_db_file,
        binary_image="some:image",
        dockerfile_name='index.Dockerfile',
    )
