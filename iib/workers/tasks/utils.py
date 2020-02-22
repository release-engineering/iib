# SPDX-License-Identifier: GPL-3.0-or-later
import copy
import json
import logging
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
    return skopeo_inspect(full_pull_spec).get('Labels', {})


def skopeo_inspect(*args, use_creds=False):
    """
    Wrap the ``skopeo inspect`` command.

    :param *args: any arguments to pass to ``skopeo inspect``
    :param bool use_creds: if true, the registry credentials in the configuration will be used
    :return: a dictionary of the JSON output from the skopeo inspect command
    :rtype: dict
    :raises iib.exceptions.IIBError: if the command fails
    """
    exc_msg = None
    for arg in args:
        if arg.startswith('docker://'):
            exc_msg = f'Failed to inspect {arg}. Make sure it exists and is accessible to IIB.'
            break

    cmd = ['skopeo', 'inspect'] + list(args)
    if use_creds:
        conf = get_worker_config()
        cmd.extend(['--creds', conf['iib_registry_credentials']])
    return json.loads(run_cmd(cmd, exc_msg=exc_msg))


def run_cmd(cmd, params=None, exc_msg=None):
    """
    Run the given command with the provided parameters.

    :param iter cmd: iterable representing the command to be executed
    :param dict params: keyword parameters for command execution
    :param str exc_msg: an optional exception message when the command fails
    :return: the command output
    :rtype: str
    :raises iib.exceptions.IIBError: if the command fails
    """
    if not params:
        params = {}
    params.setdefault('universal_newlines', True)
    params.setdefault('encoding', 'utf-8')
    params.setdefault('stderr', subprocess.PIPE)
    params.setdefault('stdout', subprocess.PIPE)

    response = subprocess.run(cmd, **params)

    if response.returncode != 0:
        conf = get_worker_config()
        _, password = conf['iib_registry_credentials'].split(':', 1)
        sanitized_cmd = copy.copy(cmd)
        for i, arg in enumerate(cmd):
            if arg in (conf['iib_registry_credentials'], password):
                sanitized_cmd[i] = '********'
        log.error('The command "%s" failed with: %s', ' '.join(sanitized_cmd), response.stderr)
        raise IIBError(exc_msg or 'An unexpected error occurred')

    return response.stdout
