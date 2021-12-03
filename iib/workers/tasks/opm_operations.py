import logging
import os
import socket
import subprocess
import time

from retry import retry

from iib.exceptions import AddressAlreadyInUse, IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.fbc_utils import is_image_fbc, get_catalog_dir

log = logging.getLogger(__name__)


def _get_free_port(port_start, port_end):
    """
    Return free port that is safe to use for opm command.

    :param int port_start: port from which we should start trying to connect to
    :param int port_end: port on which we should stop trying
    :return: free port from given interval
    :rtype: int
    :raises IIBError: if all tried ports are in use
    """
    log.debug('Finding free port from interval [%d,%d)', port_start, port_end)
    for port in range(port_start, port_end):
        sock = socket.socket()
        try:
            sock.bind(('', port))
            sock.close()
            log.debug('Free port found: %d', port)
            return port

        except OSError:
            log.info('Port %d is in use, trying another.', port)
            port += 1
        sock.close()

    err_msg = f'No free port has been found after {port_end - port_start} attempts.'
    log.error(err_msg)
    raise IIBError(err_msg)


def _get_free_port_for_grpc():
    """Return free port for gRPC service from range set in IIB config."""
    log.debug('Finding free port for gRPC')
    conf = get_worker_config()
    port_start = conf['iib_grpc_start_port']
    port_end = port_start + conf['iib_grpc_max_port_tries']

    return _get_free_port(port_start, port_end)


def opm_serve_from_index(base_dir, from_index):
    """
    Locally start OPM registry service, which can be communicated with using gRPC queries.

    Due to IIB's paralellism, the service can run multiple times, which could lead to port
    binding conflicts. Resolution of port conflicts is handled in this function as well.

    :param str base_dir: base directory to create temporary files in.
    :param str from_index: index image to inspect.
    :return: tuple containing port number of the running service and the running Popen object.
    :rtype: (int, Popen)
    """
    from iib.workers.tasks.build import _get_index_database

    log.info('Serving data from image %s', from_index)
    if not is_image_fbc(from_index):
        db_path = _get_index_database(from_index, base_dir)
        return opm_registry_serve(db_path)

    catalog_dir = get_catalog_dir(from_index, base_dir)
    return opm_serve(catalog_dir)


@retry(exceptions=AddressAlreadyInUse, tries=get_worker_config().iib_grpc_max_tries, logger=log)
def opm_serve(catalog_dir):
    """
    Locally start OPM service, which can be communicated with using gRPC queries.

    Due to IIB's paralellism, the service can run multiple times, which could lead to port
    binding conflicts. Resolution of port conflicts is handled in this function as well.

    :param str catalog_dir: path to file-based catalog directory that should be served.
    :return: tuple containing port number of the running service and the running Popen object.
    :rtype: (int, Popen)
    """
    log.info('Serving data from file-based catalog %s', catalog_dir)

    port = _get_free_port_for_grpc()
    cmd = ['opm', 'serve', catalog_dir, '-p', str(port), '-t', '/dev/null']
    cwd = os.path.abspath(os.path.join(catalog_dir, os.path.pardir))
    return (
        port,
        _serve_cmd_at_port_defaults(cmd, cwd, port),
    )


@retry(exceptions=AddressAlreadyInUse, tries=get_worker_config().iib_grpc_max_tries, logger=log)
def opm_registry_serve(db_path):
    """
    Locally start OPM registry service, which can be communicated with using gRPC queries.

    Due to IIB's paralellism, the service can run multiple times, which could lead to port
    binding conflicts. Resolution of port conflicts is handled in this function as well.

    :param str db_path: path to index database containing the registry data.
    :return: tuple containing port number of the running service and the running Popen object.
    :rtype: (int, Popen)
    """
    log.info('Serving data from index.db %s', db_path)

    port = _get_free_port_for_grpc()
    cmd = ['opm', 'registry', 'serve', '-p', str(port), '-d', db_path, '-t', '/dev/null']
    cwd = os.path.dirname(db_path)
    return (
        port,
        _serve_cmd_at_port_defaults(cmd, cwd, port),
    )


def _serve_cmd_at_port_defaults(serve_cmd, cwd, port):
    """
    Call `_serve_cmd_at_port()` with default values from IIB config.

    :param list serve_cmd: opm command to be run (serve FBC or index.db).
    :param str cwd: path to folder which should be set as current working directory.
    :param str int port: port to start the service on.
    """
    log.debug('Run _serve_cmd_at_port with default loaded from IIB config.')
    conf = get_worker_config()
    return _serve_cmd_at_port(
        serve_cmd, cwd, port, conf['iib_grpc_max_tries'], conf['iib_grpc_init_wait_time']
    )


@retry(exceptions=IIBError, tries=2, logger=log)
def _serve_cmd_at_port(serve_cmd, cwd, port, max_tries, wait_time):
    """
    Start an opm service at a specified port.

    :param list serve_cmd: opm command to be run (serve FBC or index.db).
    :param str cwd: path to folder which should be set as current working directory.
    :param str int port: port to start the service on.
    :param max_tries: how many times to try to start the service before giving up.
    :param wait_time: time to wait before checking if the service is initialized.
    :return: object of the running Popen process.
    :rtype: subprocess.Popen
    :raises IIBError: if the process has failed to initialize too many times, or an unexpected
        error occurred.
    :raises AddressAlreadyInUse: if the specified port is already being used by another service.
    """
    from iib.workers.tasks.utils import run_cmd

    log.debug('Run command %s with up to %d retries', ' '.join(serve_cmd), max_tries)
    for _ in range(max_tries):
        rpc_proc = subprocess.Popen(
            serve_cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        start_time = time.time()
        while time.time() - start_time < wait_time:
            time.sleep(1)
            ret = rpc_proc.poll()
            # process has terminated
            if ret is not None:
                stderr = rpc_proc.stderr.read()
                if 'address already in use' in stderr:
                    raise AddressAlreadyInUse(f'Port {port} is already used by a different service')
                raise IIBError(f'Command "{" ".join(serve_cmd)}" has failed with error "{stderr}"')

            # query the service to see if it has started
            try:
                output = run_cmd(
                    ['grpcurl', '-plaintext', f'localhost:{port}', 'list', 'api.Registry']
                )
            except IIBError:
                output = ''

            if 'api.Registry.ListBundles' in output or 'api.Registry.ListPackages' in output:
                log.debug('Started the command "%s"', ' '.join(serve_cmd))
                log.info('Index registry service has been initialized.')
                return rpc_proc

        rpc_proc.kill()

    raise IIBError(f'Index registry has not been initialized after {max_tries} tries')
