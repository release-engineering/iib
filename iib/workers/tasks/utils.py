# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import subprocess

from iib.exceptions import IIBError


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
    return skopeo_inspect(full_pull_spec).get('Labels', {})


def skopeo_inspect(*args):
    """
    Wrap the ``skopeo inspect`` command.

    :param args: any arguments to pass to ``skopeo inspect``
    :return: a dictionary of the JSON output from the skopeo inspect command
    :rtype: dict
    :raises IIBError: if the command fails
    """
    exc_msg = None
    for arg in args:
        if arg.startswith('docker://'):
            exc_msg = f'Failed to inspect {arg}. Make sure it exists and is accessible to IIB.'
            break

    cmd = ['skopeo', 'inspect'] + list(args)
    return json.loads(run_cmd(cmd, exc_msg=exc_msg))


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
    if not params:
        params = {}
    params.setdefault('universal_newlines', True)
    params.setdefault('encoding', 'utf-8')
    params.setdefault('stderr', subprocess.PIPE)
    params.setdefault('stdout', subprocess.PIPE)

    response = subprocess.run(cmd, **params)

    if response.returncode != 0:
        log.error('The command "%s" failed with: %s', ' '.join(cmd), response.stderr)
        raise IIBError(exc_msg or 'An unexpected error occurred')

    return response.stdout
