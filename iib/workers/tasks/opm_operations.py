import logging
import os
import shutil
import socket
import subprocess
import time

from retry import retry

from iib.exceptions import AddressAlreadyInUse, IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.fbc_utils import is_image_fbc, get_catalog_dir, get_hidden_index_database

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


def opm_migrate(index_db, base_dir):
    """
    Migrate SQLite database to File-Based catalog using opm command.

    :param str index_db: path to SQLite index.db which should migrated to FBC.
    :param str base_dir: base directory where catalog should be created.
    :return: Returns path to directory containing file-based catalog.
    :rtype: str
    """
    from iib.workers.tasks.utils import run_cmd

    fbc_dir_path = os.path.join(base_dir, 'catalog')

    # It may happen that we need to regenerate file-based catalog
    # based on updated index.db therefore we have to remove the outdated catalog
    # to be able to generate new one
    if os.path.exists(fbc_dir_path):
        shutil.rmtree(fbc_dir_path)

    cmd = ['opm', 'migrate', index_db, fbc_dir_path]

    run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to migrate index.db to file-based catalog')
    log.info("Migration to file-based catalog was completed.")
    return fbc_dir_path


def opm_generate_dockerfile(fbc_dir, base_dir, index_db, binary_image, dockerfile_name=None):
    """
    Generate Dockerfile using opm command and adding index.db to hidden location.

    :param str fbc_dir: directory containing file-based catalog (JSON or YAML files).
    :param str base_dir: base directory where Dockerfile should be created.
    :param str index_db: path to SQLite index.db which should be put to hidden location in container
    :param str binary_image: pull spec of binary image in which to build catalog.
    :param str dockerfile_name: name of generated Dockerfile.
    :return: Returns path to generated Dockerfile
    :raises: IIBError when Dockerfile was not generated
    :rtype: str
    """
    from iib.workers.tasks.utils import run_cmd

    # we do not want to continue if Dockerfile already exists
    dockerfile_name_opm_default = f"{os.path.basename(fbc_dir)}.Dockerfile"
    tmp_dockerfile_name = dockerfile_name or dockerfile_name_opm_default
    dockerfile_path = os.path.join(base_dir, tmp_dockerfile_name)

    if os.path.isfile(dockerfile_path):
        log.info(
            "Skipping generation of Dockerfile. "
            "Dockerfile for file-based catalog already exists at %s",
            dockerfile_path,
        )
        return dockerfile_path

    cmd = [
        'opm',
        'alpha',
        'generate',
        'dockerfile',
        os.path.abspath(fbc_dir),
        '--binary-image',
        binary_image,
    ]

    log.info('Generating Dockerfile with binary image %s' % binary_image)
    run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to generate Dockerfile for file-based catalog')

    # check if opm command generated Dockerfile successfully
    dockerfile_path_opm_default = os.path.join(base_dir, dockerfile_name_opm_default)
    if not os.path.isfile(dockerfile_path_opm_default):
        error_msg = f"Cannot find generated Dockerfile at {dockerfile_path_opm_default}"
        log.error(error_msg)
        raise IIBError(error_msg)

    # we should rename Dockerfile generated by opm if `dockerfile_name` parameter is set
    if dockerfile_name:
        if os.path.exists(dockerfile_path):
            log.info('Rewriting Dockerfile %s with newly generated by opm.', dockerfile_path)
        os.rename(dockerfile_path_opm_default, dockerfile_path)

    db_path = get_worker_config()['hidden_index_db_path']
    rel_path_index_db = os.path.relpath(index_db, base_dir)
    with open(dockerfile_path, 'a') as f:
        f.write(f'\nADD {rel_path_index_db} {db_path}\n')

    log.info("Dockerfile was successfully generated.")
    return dockerfile_path


@retry(exceptions=IIBError, tries=2, logger=log)
def _opm_registry_add(
    base_dir, index_db, bundles, overwrite_csv=False, container_tool=None,
):
    """
    Add the input bundles to an operator index database.

    This only runs operations on index database.

    :param str base_dir: the base directory to generate the database and index.Dockerfile in.
    :param str index_db: relative path to SQLite index.db database file
    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :param bool overwrite_csv: a boolean determining if a bundle will be replaced if the CSV
        already exists.
    :param str container_tool: the container tool to be used to operate on the index image
    """
    from iib.workers.tasks.utils import run_cmd

    # The bundles are not resolved since these are stable tags, and references
    # to a bundle image using a digest fails when using the opm command.
    bundle_str = ','.join(bundles) or '""'

    cmd = [
        'opm',
        'registry',
        'add',
        '--database',
        index_db,
        # This enables substitutes-for functionality for rebuilds. See
        # https://github.com/operator-framework/enhancements/blob/master/enhancements/substitutes-for.md
        '--enable-alpha',
        '--bundle-images',
        bundle_str,
    ]

    if container_tool:
        cmd.append('--container-tool')
        cmd.append(container_tool)

    log.info('Generating the database file with the following bundle(s): %s', ', '.join(bundles))

    if overwrite_csv:
        log.info('Using force to add bundle(s) to index')
        cmd.extend(['--overwrite-latest'])

    run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to add the bundles to the index image')


@retry(exceptions=IIBError, tries=2, logger=log)
def opm_registry_add_fbc(
    base_dir, bundles, binary_image, from_index=None, overwrite_csv=False, container_tool=None,
):
    """
    Add the input bundles to an operator index.

    This only produces the index.Dockerfile file and does not build the container image.

    :param str base_dir: the base directory to generate the database and index.Dockerfile in.
    :param list bundles: a list of strings representing the pull specifications of the bundles to
        add to the index image being built.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from. This should point to a digest or stable tag.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param bool overwrite_csv: a boolean determining if a bundle will be replaced if the CSV
        already exists.
    :param str container_tool: the container tool to be used to operate on the index image
    """
    if from_index:
        log.info('Using the existing database from %s', from_index)
        index_db_file = get_hidden_index_database(from_index, base_dir)
    else:
        log.info('Creating new database file index.db')
        index_db_file = os.path.join(base_dir, get_worker_config()['temp_index_db_path'])
        # prepare path and create empty file as SQLite database
        index_db_dir = os.path.dirname(index_db_file)
        if not os.path.exists(index_db_dir):
            os.makedirs(index_db_dir, exist_ok=True)
        with open(index_db_file, 'w'):
            pass

    _opm_registry_add(
        base_dir=base_dir,
        index_db=index_db_file,
        bundles=bundles,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
    )

    fbc_dir = opm_migrate(index_db=index_db_file, base_dir=base_dir)
    # we should keep generating Dockerfile here
    # to have the same behavior as we run `opm index add` with '--generate' option
    opm_generate_dockerfile(
        fbc_dir=fbc_dir,
        base_dir=base_dir,
        index_db=index_db_file,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )


def _opm_registry_rm(index_db_path, operators, base_dir):
    """
    Generate and run the opm command to remove operator package from index db provided.

    :param str index_db_path: path where the input index image is temporarily copied
    :param list operators: list of operator packages to be removed
    :param base_dir: the base directory to generate the database and index.Dockerfile in.
    """
    from iib.workers.tasks.utils import run_cmd

    cmd = [
        'opm',
        'registry',
        'rm',
        '--database',
        index_db_path,
        '--packages',
        ','.join(operators),
    ]
    run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to remove operators from the index image')


@retry(exceptions=IIBError, tries=2, logger=log)
def opm_registry_rm_fbc(base_dir, from_index, operators, binary_image):
    """
    Remove operator/s from a File Based Catalog index image.

    This only produces the index.Dockerfile file and does not build the container image.

    :param str base_dir: the base directory to generate the database and index.Dockerfile in.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param list operators: a list of strings representing the packages of the operators to be
        removed from the output index image.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from. This should point to a digest or stable tag.
    """
    log.info('Removing %s from a FBC Image %s', operators, from_index)
    log.info('Using the existing database from %s', from_index)
    index_db_path = get_hidden_index_database(from_index=from_index, base_dir=base_dir)

    _opm_registry_rm(index_db_path, operators, base_dir)
    fbc_dir = opm_migrate(index_db=index_db_path, base_dir=base_dir)

    opm_generate_dockerfile(
        fbc_dir=fbc_dir,
        base_dir=base_dir,
        index_db=index_db_path,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )
