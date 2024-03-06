import logging
import os
import random
import shutil
import subprocess
import time
from typing import List, Optional, Tuple, Generator, Union

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
)

from iib.exceptions import AddressAlreadyInUse, IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.fbc_utils import (
    is_image_fbc,
    get_catalog_dir,
    get_hidden_index_database,
    extract_fbc_fragment,
)

log = logging.getLogger(__name__)


def _gen_port_for_grpc() -> Generator[int, None, None]:
    """
    Generate port for gRPC service from range set in IIB config.

    :raises: IIBError when all ports were already taken
    """
    conf = get_worker_config()
    port_start = conf['iib_grpc_start_port']
    port_end = port_start + conf['iib_grpc_max_port_tries']

    port_stack = list(range(port_start, port_end))
    random.shuffle(port_stack)

    log.debug('Get random ports from range [%d, %d)', port_start, port_end)

    while port_stack:
        yield port_stack.pop(0)

    # The port stack is empty - we tried out all ports from allowed range
    # therefore we will raise and IIB error
    err_msg = f'No free port has been found after {port_end - port_start} attempts.'
    log.error(err_msg)
    raise IIBError(err_msg)


def opm_serve_from_index(base_dir: str, from_index: str) -> Tuple[int, subprocess.Popen]:
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


def opm_serve(catalog_dir: str) -> Tuple[int, subprocess.Popen]:
    """
    Locally start OPM service, which can be communicated with using gRPC queries.

    Due to IIB's paralellism, the service can run multiple times, which could lead to port
    binding conflicts. Resolution of port conflicts is handled in this function as well.

    :param str catalog_dir: path to file-based catalog directory that should be served.
    :return: tuple containing port number of the running service and the running Popen object.
    :rtype: (int, Popen)
    """
    log.info('Serving data from file-based catalog %s', catalog_dir)

    for port in _gen_port_for_grpc():
        try:
            cmd = ['opm', 'serve', catalog_dir, '-p', str(port), '-t', '/dev/null']
            cwd = os.path.abspath(os.path.join(catalog_dir, os.path.pardir))
            result = (
                port,
                _serve_cmd_at_port_defaults(cmd, cwd, port),
            )
            break
        except AddressAlreadyInUse:
            log.debug('Port %s is already taken. Checking next one...', port)
            continue
    return result


def opm_registry_serve(db_path: str) -> Tuple[int, subprocess.Popen]:
    """
    Locally start OPM registry service, which can be communicated with using gRPC queries.

    Due to IIB's paralellism, the service can run multiple times, which could lead to port
    binding conflicts. Resolution of port conflicts is handled in this function as well.

    :param str db_path: path to index database containing the registry data.
    :return: tuple containing port number of the running service and the running Popen object.
    :rtype: (int, Popen)
    """
    log.info('Serving data from index.db %s', db_path)

    for port in _gen_port_for_grpc():
        try:
            cmd = ['opm', 'registry', 'serve', '-p', str(port), '-d', db_path, '-t', '/dev/null']
            cwd = os.path.dirname(db_path)
            result = (
                port,
                _serve_cmd_at_port_defaults(cmd, cwd, port),
            )
            break
        except AddressAlreadyInUse:
            log.debug('Port %s is already taken. Checking next one...', port)
            continue
    return result


def _serve_cmd_at_port_defaults(serve_cmd: List[str], cwd: str, port: int) -> subprocess.Popen:
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


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(2),
)
def _serve_cmd_at_port(
    serve_cmd: List[str],
    cwd: str,
    port: int,
    max_tries: int,
    wait_time: int,
) -> subprocess.Popen:
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
    from iib.workers.tasks.utils import run_cmd, terminate_process

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
            time.sleep(5)
            ret = rpc_proc.poll()
            # process has terminated
            if ret is not None:
                if not rpc_proc.stderr:
                    raise IIBError(
                        f'Command "{" ".join(serve_cmd)}" has failed, stderr was not captured'
                    )
                stderr_message = rpc_proc.stderr.read()
                if 'address already in use' in stderr_message:
                    raise AddressAlreadyInUse(f'Port {port} is already used by a different service')
                raise IIBError(
                    f'Command "{" ".join(serve_cmd)}" has failed with error "{stderr_message}"'
                )

            # query the service to see if it has started
            try:
                output = run_cmd(
                    ['grpcurl', '-plaintext', f'localhost:{port}', 'list', 'api.Registry']
                )
            except IIBError:
                output = ''

            if 'api.Registry.ListBundles' in output or 'api.Registry.ListPackages' in output:
                log.debug('Started the command "%s"; pid: %d', ' '.join(serve_cmd), rpc_proc.pid)
                log.info('Index registry service has been initialized.')
                return rpc_proc

        terminate_process(rpc_proc)

    raise IIBError(f'Index registry has not been initialized after {max_tries} tries')


def _get_or_create_temp_index_db_file(
    base_dir: str,
    from_index: Optional[str] = None,
    overwrite_from_index_token: Optional[str] = None,
    ignore_existing: bool = False,
) -> str:
    """
    Get path to temp index.db used for opm registry commands.

    If index.db does not exist it will be created, ether by copying from from_index
    or as creating empty index.db file if from_index is not set.

    :param str base_dir: base directory where index.db file will be located.
    :param str from_index: index image, from which we should copy index.
    :param bool ignore_existing: if set it forces to copy index.db from `from_index`.
       `from_index` must be set
    :return: Returns path to index.db located in base_dir.
    :rtype: str
    """
    from iib.workers.tasks.build import _get_index_database
    from iib.workers.tasks.utils import set_registry_token

    index_db_file = os.path.join(base_dir, get_worker_config()['temp_index_db_path'])

    if not ignore_existing and os.path.exists(index_db_file):
        log.debug('Temp index.db already exist for %s', from_index)
        return index_db_file

    log.info('Temp index.db does not exist yet for %s', from_index)
    if from_index:
        log.info('Using the existing database from %s', from_index)
        with set_registry_token(overwrite_from_index_token, from_index, append=True):
            if is_image_fbc(from_index):
                return get_hidden_index_database(from_index, base_dir)
            return _get_index_database(from_index, base_dir)

    log.info('Creating empty database file %s', index_db_file)
    index_db_dir = os.path.dirname(index_db_file)
    if not os.path.exists(index_db_dir):
        os.makedirs(index_db_dir, exist_ok=True)
    with open(index_db_file, 'w'):
        pass

    return index_db_file


def opm_registry_deprecatetruncate(base_dir: str, index_db: str, bundles: List[str]) -> None:
    """
    Deprecate bundles from index.db.

    :param str base_dir: base directory where operation files will be located.
    :param str index_db: path to index.db used with opm registry deprecatetruncate.
    :param list bundles: pull specifications of bundles to deprecate.
    """
    from iib.workers.tasks.utils import run_cmd

    log.debug(
        'Run opm registry deprecatetruncate on database %s and bundles %s',
        index_db,
        ' '.join(bundles),
    )

    cmd = [
        'opm',
        'registry',
        'deprecatetruncate',
        '--database',
        index_db,
        '--bundle-images',
        ','.join(bundles),
        '--allow-package-removal',
    ]

    run_cmd(cmd, {'cwd': base_dir}, exc_msg=f'Failed to deprecate the bundles on {index_db}')


def deprecate_bundles_fbc(
    bundles: List[str],
    base_dir: str,
    binary_image: str,
    from_index: str,
) -> None:
    """
    Deprecate the specified bundles from the FBC index image.

    Dockerfile is created only, no build is performed.

    :param list bundles: pull specifications of bundles to deprecate.
    :param str base_dir: base directory where operation files will be located.
    :param str binary_image: binary image to be used by the new index image.
    :param str from_index: index image, from which the bundles will be deprecated.
    """
    index_db_file = _get_or_create_temp_index_db_file(base_dir=base_dir, from_index=from_index)

    opm_registry_deprecatetruncate(
        base_dir=base_dir,
        index_db=index_db_file,
        bundles=bundles,
    )

    fbc_dir, _ = opm_migrate(index_db_file, base_dir)
    # we should keep generating Dockerfile here
    # to have the same behavior as we run `opm index deprecatetruncate` with '--generate' option
    opm_generate_dockerfile(
        fbc_dir=fbc_dir,
        base_dir=base_dir,
        index_db=index_db_file,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )


def opm_migrate(
    index_db: str, base_dir: str, generate_cache: bool = True
) -> Union[Tuple[str, str], Tuple[str, None]]:
    """
    Migrate SQLite database to File-Based catalog and generate cache using opm command.

    :param str index_db: path to SQLite index.db which should migrated to FBC.
    :param str base_dir: base directory where catalog should be created.
    :param bool generate_cache: if set cache will be generated
    :return: Returns paths to directories for containing file-based catalog and it's cache
    :rtype: str, str|None
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

    if generate_cache:
        # Remove outdated cache before generating new one
        local_cache_path = os.path.join(base_dir, 'cache')
        if os.path.exists(local_cache_path):
            shutil.rmtree(local_cache_path)
        generate_cache_locally(base_dir, fbc_dir_path, local_cache_path)
        return fbc_dir_path, local_cache_path

    return fbc_dir_path, None


def opm_generate_dockerfile(
    fbc_dir: str,
    base_dir: str,
    index_db: str,
    binary_image: str,
    dockerfile_name: Optional[str] = None,
) -> str:
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

    insert_cache_into_dockerfile(dockerfile_path)

    db_path = get_worker_config()['hidden_index_db_path']
    rel_path_index_db = os.path.relpath(index_db, base_dir)
    with open(dockerfile_path, 'a') as f:
        f.write(f'\nADD {rel_path_index_db} {db_path}\n')

    log.info("Dockerfile was successfully generated.")
    return dockerfile_path


def verify_cache_insertion_edit_dockerfile(file_list: list) -> None:
    """
    Verify Dockerfile edit to insert local cache was successful.

    :param list file_list: Generated dockerfile as a list.
    :raises: IIBError when the Dockerfile edit is unsuccessful.
    """
    copied_cache_found = False
    match_str = 'COPY --chown=1001:0 cache /tmp/cache'
    for line in file_list:
        if match_str in line:
            copied_cache_found = True
            break
    if not copied_cache_found:
        raise IIBError('Dockerfile edit to insert locally built cache failed.')


def insert_cache_into_dockerfile(dockerfile_path: str) -> None:
    """
    Insert built cache into the Dockerfile.

    :param str dockerfile_path: path to the generated Dockerfile.
    """
    with open(dockerfile_path, 'r') as f:
        file_data = f.read()

    file_data = file_data.replace(
        'RUN ["/bin/opm", "serve", "/configs", "--cache-dir=/tmp/cache", "--cache-only"]',
        'COPY --chown=1001:0 cache /tmp/cache',
    )

    with open(dockerfile_path, 'w') as f:
        f.write(file_data)

    with open(dockerfile_path, 'r') as f:
        verify_cache_insertion_edit_dockerfile(f.readlines())


def generate_cache_locally(base_dir: str, fbc_dir: str, local_cache_path: str) -> None:
    """
    Generate the cache for the index image locally before building it.

    :param str base_dir: base directory where cache should be created.
    :param str fbc_dir: directory containing file-based catalog (JSON or YAML files).
    :param str local_cache_path: path to the locally generated cache.
    :return: Returns path to generated cache
    :rtype: str
    :raises: IIBError when cache was not generated

    """
    from iib.workers.tasks.utils import run_cmd

    cmd = [
        'opm',
        'serve',
        os.path.abspath(fbc_dir),
        f'--cache-dir={local_cache_path}',
        '--cache-only',
        '--termination-log',
        '/dev/null',
    ]

    log.info('Generating cache for the file-based catalog')
    if os.path.exists(local_cache_path):
        shutil.rmtree(local_cache_path)
    run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to generate cache for file-based catalog')

    # Check if the opm command generated cache successfully
    if not os.path.isdir(local_cache_path):
        error_msg = f"Cannot find generated cache at {local_cache_path}"
        log.error(error_msg)
        raise IIBError(error_msg)


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(2),
)
def _opm_registry_add(
    base_dir: str,
    index_db: str,
    bundles: List[str],
    overwrite_csv: bool = False,
    container_tool: Optional[str] = None,
    graph_update_mode: Optional[str] = None,
) -> None:
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
    :param str graph_update_mode: Graph update mode that defines how channel graphs are updated
        in the index.
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

    if graph_update_mode:
        log.info('Using %s mode to update the channel graph in the index', graph_update_mode)
        cmd.extend(['--mode', graph_update_mode])

    log.info('Generating the database file with the following bundle(s): %s', ', '.join(bundles))

    if overwrite_csv:
        log.info('Using force to add bundle(s) to index')
        cmd.extend(['--overwrite-latest'])

    # after commit 643fc9499222107f52b7ba3f9f3969fa36812940 index.db backup was added
    # due to the opm bug https://issues.redhat.com/browse/OCPBUGS-30214
    index_db_backup = index_db + ".backup"
    shutil.copyfile(index_db, index_db_backup)

    try:
        run_cmd(cmd, {'cwd': base_dir}, exc_msg='Failed to add the bundles to the index image')
    except Exception as e:
        shutil.copyfile(index_db_backup, index_db)
        raise e
    finally:
        os.remove(index_db_backup)


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(2),
)
def opm_registry_add_fbc(
    base_dir: str,
    bundles: List[str],
    binary_image: str,
    from_index: Optional[str] = None,
    graph_update_mode: Optional[str] = None,
    overwrite_csv: bool = False,
    overwrite_from_index_token: Optional[str] = None,
    container_tool: Optional[str] = None,
) -> None:
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
    :param str graph_update_mode: Graph update mode that defines how channel graphs are updated
        in the index.
    :param bool overwrite_csv: a boolean determining if a bundle will be replaced if the CSV
        already exists.
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``source_from_index`` image. This is required to use ``overwrite_target_index``.
        The format of the token must be in the format "user:password".
    :param str container_tool: the container tool to be used to operate on the index image
    """
    index_db_file = _get_or_create_temp_index_db_file(
        base_dir=base_dir,
        from_index=from_index,
        overwrite_from_index_token=overwrite_from_index_token,
        ignore_existing=True,
    )

    _opm_registry_add(
        base_dir=base_dir,
        index_db=index_db_file,
        bundles=bundles,
        overwrite_csv=overwrite_csv,
        container_tool=container_tool,
        graph_update_mode=graph_update_mode,
    )

    fbc_dir, _ = opm_migrate(index_db=index_db_file, base_dir=base_dir)
    # we should keep generating Dockerfile here
    # to have the same behavior as we run `opm index add` with '--generate' option
    opm_generate_dockerfile(
        fbc_dir=fbc_dir,
        base_dir=base_dir,
        index_db=index_db_file,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )


def _opm_registry_rm(index_db_path: str, operators: List[str], base_dir: str) -> None:
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


@retry(
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
    retry=retry_if_exception_type(IIBError),
    stop=stop_after_attempt(2),
)
def opm_registry_rm_fbc(
    base_dir: str,
    from_index: str,
    operators: List[str],
    binary_image: str,
    overwrite_from_index_token: Optional[str] = None,
    generate_cache: bool = True,
) -> Tuple[str, Optional[str]]:
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
    :param str overwrite_from_index_token: the token used for overwriting the input
        ``from_index`` image. This is required to use ``overwrite_from_index``.
        The format of the token must be in the format "user:password".
    :param bool generate_cache: if set cache of migrated file-based catalog will be generated
        The format of the token must be in the format "user:password".

    :return: Returns paths to directories for containing file-based catalog and it's cache
    :rtype: str, str|None
    """
    from iib.workers.tasks.utils import set_registry_token

    log.info('Removing %s from a FBC Image %s', operators, from_index)
    log.info('Using the existing database from %s', from_index)

    with set_registry_token(overwrite_from_index_token, from_index, append=True):
        index_db_path = get_hidden_index_database(from_index=from_index, base_dir=base_dir)

    _opm_registry_rm(index_db_path, operators, base_dir)
    fbc_dir, cache_dir = opm_migrate(
        index_db=index_db_path, base_dir=base_dir, generate_cache=generate_cache
    )

    opm_generate_dockerfile(
        fbc_dir=fbc_dir,
        base_dir=base_dir,
        index_db=index_db_path,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )

    return fbc_dir, cache_dir


def opm_create_empty_fbc(
    request_id: int,
    temp_dir: str,
    from_index_resolved: str,
    from_index: str,
    binary_image: str,
    operators: List[str],
) -> None:
    """
    Create an empty FBC index image.

    This only produces the index.Dockerfile file and does not build the container image.

    :param int request_id: the ID of the IIB build request
    :param str temp_dir: the base directory to generate the database and index.Dockerfile in.
    :param str from_index_resolved: the resolved pull specification of the container image
        containing the index that the index image build will be based from.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from. This should point to a digest or stable tag.
    :param list operators: a list of strings representing the packages of the operators to be
        removed from the output index image.
    """
    # if from_index provided is FBC, get the hidden index db location
    if is_image_fbc(from_index_resolved):
        log.debug('%s provided is FBC index image', from_index)
        index_db_path = get_hidden_index_database(from_index=from_index, base_dir=temp_dir)
    # if the from_index is SQLite based, get the default index db location
    else:
        from iib.workers.tasks.build import _get_index_database

        log.debug('%s provided is SQLite index image', from_index)
        index_db_path = _get_index_database(from_index=from_index, base_dir=temp_dir)

    # Remove all the operators from the index
    set_request_state(request_id, 'in_progress', 'Removing operators from index image')
    _opm_registry_rm(index_db_path=index_db_path, operators=operators, base_dir=temp_dir)

    # Migrate the index to FBC
    fbc_dir, _ = opm_migrate(index_db=index_db_path, base_dir=temp_dir)

    opm_generate_dockerfile(
        fbc_dir=fbc_dir,
        base_dir=temp_dir,
        index_db=index_db_path,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )


def opm_registry_add_fbc_fragment(
    request_id: int,
    temp_dir: str,
    from_index: str,
    binary_image: str,
    fbc_fragment: str,
    overwrite_from_index_token: Optional[str],
) -> None:
    """
    Add FBC fragment to from_index image.

    This only produces the index.Dockerfile file and does not build the container image.

    :param int request_id: the id of IIB request
    :param str temp_dir: the base directory to generate the database and index.Dockerfile in.
    :param str from_index: the pull specification of the container image containing the index that
        the index image build will be based from.
    :param str binary_image: the pull specification of the container image where the opm binary
        gets copied from. This should point to a digest or stable tag.
    :param str fbc_fragment: the pull specification of fbc fragment to be added in from_index.
    :param str overwrite_from_index_token: token used to access the image
    """
    set_request_state(request_id, 'in_progress', 'Extracting operator package from fbc_fragment')
    # fragment path will look like /tmp/iib-**/fbc-fragment
    fragment_path, fragment_operator = extract_fbc_fragment(
        temp_dir=temp_dir, fbc_fragment=fbc_fragment
    )

    is_operator_in_db, index_db_path = verify_operator_exists(
        from_index=from_index,
        base_dir=temp_dir,
        operator_package=fragment_operator,
        overwrite_from_index_token=overwrite_from_index_token,
    )

    # the dir where all the configs from from_index is stored
    # this will look like /tmp/iib-**/configs
    from_index_configs_dir = get_catalog_dir(from_index=from_index, base_dir=temp_dir)

    log.info("The content of from_index configs located at %s", from_index_configs_dir)

    if is_operator_in_db:
        log.info('Removing %s from %s index.db ', fragment_operator, from_index)
        _opm_registry_rm(
            index_db_path=index_db_path, operators=[fragment_operator], base_dir=temp_dir
        )
        # migated_catalog_dir path will look like /tmp/iib-**/catalog
        migated_catalog_dir, _ = opm_migrate(
            index_db=index_db_path,
            base_dir=temp_dir,
            generate_cache=False,
        )
        log.info("Migated catalog after removing from db at %s", migated_catalog_dir)

        # copy the content of migrated_catalog to from_index's config
        log.info("Copying content of %s to %s", migated_catalog_dir, from_index_configs_dir)
        for operator_package in os.listdir(migated_catalog_dir):
            shutil.copytree(
                os.path.join(migated_catalog_dir, operator_package),
                os.path.join(from_index_configs_dir, operator_package),
                dirs_exist_ok=True,
            )

    # copy fragment_operator to from_index configs
    set_request_state(request_id, 'in_progress', 'Adding fbc_fragment to from_index')
    fragment_opr_src_path = os.path.join(fragment_path, fragment_operator)
    fragment_opr_dest_path = os.path.join(from_index_configs_dir, fragment_operator)
    if os.path.exists(fragment_opr_dest_path):
        shutil.rmtree(fragment_opr_dest_path)
    log.info(
        "Copying content of %s to %s",
        fragment_opr_src_path,
        fragment_opr_dest_path,
    )
    shutil.copytree(fragment_opr_src_path, fragment_opr_dest_path)

    local_cache_path = os.path.join(temp_dir, 'cache')
    generate_cache_locally(
        base_dir=temp_dir, fbc_dir=from_index_configs_dir, local_cache_path=local_cache_path
    )

    log.info("Dockerfile generated from %s", from_index_configs_dir)
    opm_generate_dockerfile(
        fbc_dir=from_index_configs_dir,
        base_dir=temp_dir,
        index_db=index_db_path,
        binary_image=binary_image,
        dockerfile_name='index.Dockerfile',
    )


def verify_operator_exists(
    from_index: str,
    base_dir: str,
    operator_package: str,
    overwrite_from_index_token: Optional[str],
):
    """
    Check if operator exists in index image.

    :param str from_index: index in which operator existence is checked
    :param str base_dir: base temp directory for IIB request
    :param str operator_package: operator_package to check
    :param str overwrite_from_index_token: token used to access the image
    :return: is_package_in_index, index_db_path
    :rtype: (str, str)
    """
    from iib.workers.tasks.build import terminate_process, get_bundle_json
    from iib.workers.tasks.iib_static_types import BundleImage
    from iib.workers.tasks.utils import run_cmd
    from iib.workers.tasks.utils import set_registry_token

    is_package_in_index = False

    log.info("Verifying if operator package %s exists in index %s", operator_package, from_index)

    # check if operater package exists in hidden index.db
    with set_registry_token(overwrite_from_index_token, from_index, append=True):
        index_db_path = get_hidden_index_database(from_index=from_index, base_dir=base_dir)

    port, rpc_proc = opm_registry_serve(db_path=index_db_path)
    bundles = run_cmd(
        ['grpcurl', '-plaintext', f'localhost:{port}', 'api.Registry/ListBundles'],
        exc_msg='Failed to get bundle data from index image',
    )
    terminate_process(rpc_proc)
    present_bundles: List[BundleImage] = get_bundle_json(bundles)

    for bundle in present_bundles:
        if bundle['packageName'] == operator_package:
            is_package_in_index = True
            log.info("operator package %s found in index_db %s", operator_package, index_db_path)
    return is_package_in_index, index_db_path
