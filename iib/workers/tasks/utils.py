# SPDX-License-Identifier: GPL-3.0-or-later
import base64
from contextlib import contextmanager
import functools
import inspect
import json
import logging
import os
import re
import subprocess

from operator_manifest.operator import ImageName

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config

log = logging.getLogger(__name__)


def get_image_labels(pull_spec):
    """
    Get the labels from the image.

    :param list<str> labels: the labels to get
    :return: the dictionary of the labels on the image
    :rtype: dict
    """
    if pull_spec.startswith('docker://'):
        full_pull_spec = pull_spec
    else:
        full_pull_spec = f'docker://{pull_spec}'
    log.debug('Getting the labels from %s', full_pull_spec)
    return skopeo_inspect(full_pull_spec, '--config').get('config', {}).get('Labels', {})


def retry(
    attempts=get_worker_config().iib_total_attempts, wait_on=Exception, logger=None,
):
    """
    Retry a section of code until success or max attempts are reached.

    :param int attempts: the total number of attempts to make before erroring out
    :param Exception wait_on: the exception on encountering which the function will be retried
    :param logging logger: the logger to log the messages on
    :raises IIBError: if the maximum attempts are reached
    """

    def wrapper(function):
        @functools.wraps(function)
        def inner(*args, **kwargs):
            remaining_attempts = attempts
            while True:
                try:
                    return function(*args, **kwargs)
                except wait_on as e:
                    remaining_attempts -= 1
                    if remaining_attempts <= 0:
                        if logger is not None:
                            logger.exception(
                                'The maximum number of attempts (%s) have failed', attempts
                            )
                        raise
                    if logger is not None:
                        logger.warning(
                            'Exception %r raised from %r.  Retrying now',
                            e,
                            f'{function.__module__}.{function.__name__}',
                        )

        return inner

    return wrapper


def reset_docker_config():
    """Create a symlink from ``iib_docker_config_template`` to ``~/.docker/config.json``."""
    conf = get_worker_config()
    docker_config_path = os.path.join(os.path.expanduser('~'), '.docker', 'config.json')

    try:
        log.debug('Removing the Docker config at %s', docker_config_path)
        os.remove(docker_config_path)
    except FileNotFoundError:
        pass

    if os.path.exists(conf.iib_docker_config_template):
        log.debug(
            'Creating a symlink from %s to %s', conf.iib_docker_config_template, docker_config_path
        )
        os.symlink(conf.iib_docker_config_template, docker_config_path)


@contextmanager
def set_registry_token(token, container_image):
    """
    Configure authentication to the registry that ``container_image`` is from.

    This context manager will reset the authentication to the way it was after it exits. If
    ``token`` is falsy, this context manager will do nothing.

    :param str token: the token in the format of ``username:password``
    :param str container_image: the pull specification of the container image to parse to determine
        the registry this token is for.
    :return: None
    :rtype: None
    """
    if not token:
        log.debug(
            'Not changing the Docker configuration since no overwrite_from_index_token was provided'
        )
        yield

        return

    docker_config_path = os.path.join(os.path.expanduser('~'), '.docker', 'config.json')
    try:
        log.debug('Removing the Docker config symlink at %s', docker_config_path)
        try:
            os.remove(docker_config_path)
        except FileNotFoundError:
            log.debug('The Docker config symlink at %s does not exist', docker_config_path)

        conf = get_worker_config()
        if os.path.exists(conf.iib_docker_config_template):
            with open(conf.iib_docker_config_template, 'r') as f:
                docker_config = json.load(f)
        else:
            docker_config = {}

        registry = ImageName.parse(container_image).registry
        log.debug('Setting the override token for the registry %s in the Docker config', registry)
        docker_config.setdefault('auths', {})
        encoded_token = base64.b64encode(token.encode('utf-8')).decode('utf-8')
        docker_config['auths'][registry] = {'auth': encoded_token}
        with open(docker_config_path, 'w') as f:
            json.dump(docker_config, f)

        yield
    finally:
        reset_docker_config()


@retry(wait_on=IIBError, logger=log)
def skopeo_inspect(*args, return_json=True):
    """
    Wrap the ``skopeo inspect`` command.

    :param args: any arguments to pass to ``skopeo inspect``
    :param bool return_json: if ``True``, the output will be parsed as JSON and returned
    :return: a dictionary of the JSON output from the skopeo inspect command
    :rtype: dict
    :raises IIBError: if the command fails
    """
    exc_msg = None
    for arg in args:
        if arg.startswith('docker://'):
            exc_msg = f'Failed to inspect {arg}. Make sure it exists and is accessible to IIB.'
            break

    skopeo_timeout = get_worker_config().iib_skopeo_timeout
    cmd = ['skopeo', '--command-timeout', skopeo_timeout, 'inspect'] + list(args)
    output = run_cmd(cmd, exc_msg=exc_msg)
    if return_json:
        return json.loads(output)

    return output


@retry(wait_on=IIBError, logger=log)
def podman_pull(*args):
    """
    Wrap the ``podman pull`` command.

    :param args: any arguments to pass to ``podman pull``
    :raises IIBError: if the command fails
    """
    run_cmd(
        ['podman', 'pull'] + list(args),
        exc_msg=f'Failed to pull the container image {" ".join(args)}',
    )


def run_cmd(cmd, params=None, exc_msg=None):
    """
    Run the given command with the provided parameters.

    :param iter cmd: iterable representing the command to be executed
    :param dict params: keyword parameters for command execution
    :param str exc_msg: an optional exception message when the command fails
    :return: the command output
    :rtype: str
    :raises IIBError: if the command fails
    """
    exc_msg = exc_msg or 'An unexpected error occurred'
    if not params:
        params = {}
    params.setdefault('universal_newlines', True)
    params.setdefault('encoding', 'utf-8')
    params.setdefault('stderr', subprocess.PIPE)
    params.setdefault('stdout', subprocess.PIPE)

    log.debug('Running the command "%s"', ' '.join(cmd))
    response = subprocess.run(cmd, **params)

    if response.returncode != 0:
        log.error('The command "%s" failed with: %s', ' '.join(cmd), response.stderr)
        if cmd[0] == 'opm':
            # Capture the error message right before the help display
            regex = r'^(?:Error: )(.+)$'
            # Start from the last log message since the failure occurs near the bottom
            for msg in reversed(response.stderr.splitlines()):
                match = re.match(regex, msg)
                if match:
                    raise IIBError(f'{exc_msg.rstrip(".")}: {match.groups()[0]}')

        raise IIBError(exc_msg)

    return response.stdout


def request_logger(func):
    """
    Log messages relevant to the current request to a dedicated file.

    If ``iib_request_logs_dir`` is set, a temporary log handler is added before the decorated
    function is invoked. It's then removed once the decorated function completes execution.

    If ``iib_request_logs_dir`` is not set, the temporary log handler will not be added.

    :param function func: the function to be decorated. The function must take the ``request_id``
        parameter.
    :return: the decorated function
    :rtype: function
    """
    worker_config = get_worker_config()
    log_dir = worker_config.iib_request_logs_dir
    log_level = worker_config.iib_request_logs_level
    log_format = worker_config.iib_request_logs_format

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        request_log_handler = None
        if log_dir:
            log_formatter = logging.Formatter(log_format)
            request_id = _get_function_arg_value('request_id', func, args, kwargs)
            if not request_id:
                raise IIBError(f'Unable to get "request_id" from {func.__name__}')

            log_file_path = os.path.join(log_dir, f'{request_id}.log')
            request_log_handler = logging.FileHandler(log_file_path)
            request_log_handler.setLevel(log_level)
            request_log_handler.setFormatter(log_formatter)
            os.chmod(log_file_path, 0o775)
            logger = logging.getLogger()
            logger.addHandler(request_log_handler)
        try:
            return func(*args, **kwargs)
        finally:
            if request_log_handler:
                logger.removeHandler(request_log_handler)

    return wrapper


def _get_function_arg_value(arg_name, func, args, kwargs):
    """Return the value of the given argument name."""
    original_func = func
    while getattr(func, '__wrapped__', None):
        original_func = func.__wrapped__
    argspec = inspect.getargspec(original_func).args

    arg_index = argspec.index(arg_name)
    arg_value = kwargs.get(arg_name, None)
    if arg_value is None and len(args) > arg_index:
        arg_value = args[arg_index]
    return arg_value
