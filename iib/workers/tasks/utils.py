# SPDX-License-Identifier: GPL-3.0-or-later
import functools
import json
import logging
import re
import subprocess

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


def run_cmd(cmd, params=None, exc_msg=None, cmd_repr=None):
    """
    Run the given command with the provided parameters.

    :param iter cmd: iterable representing the command to be executed
    :param dict params: keyword parameters for command execution
    :param str exc_msg: an optional exception message when the command fails
    :param iter cmd_repr: an optional representation of the command to be executed. If not
        provided, this is derived from the ``cmd`` parameter. This is useful when the command
        contains sensitive information that must not be logged.
    :return: the command output
    :rtype: str
    :raises IIBError: if the command fails
    """
    exc_msg = exc_msg or 'An unexpected error occurred'
    cmd_repr = cmd_repr or cmd
    if not params:
        params = {}
    params.setdefault('universal_newlines', True)
    params.setdefault('encoding', 'utf-8')
    params.setdefault('stderr', subprocess.PIPE)
    params.setdefault('stdout', subprocess.PIPE)

    log.debug('Running the command "%s"', cmd_repr)
    response = subprocess.run(cmd, **params)

    if response.returncode != 0:
        log.error('The command "%s" failed with: %s', ' '.join(cmd_repr), response.stderr)
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
