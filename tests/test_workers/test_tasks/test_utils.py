# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import textwrap
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import utils


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_labels(mock_si):
    skopeo_rv = {'config': {'Labels': {'some_label': 'value'}}}
    mock_si.return_value = skopeo_rv
    assert utils.get_image_labels('some-image:latest') == skopeo_rv['config']['Labels']


def test_retry():
    mock_func = mock.Mock()

    @utils.retry(attempts=3, wait_on=IIBError)
    def _func_to_retry():
        mock_func()
        raise IIBError('Some error')

    with pytest.raises(IIBError, match='Some error'):
        _func_to_retry()

    assert mock_func.call_count == 3


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 0
    mock_sub_run.return_value = mock_rv

    utils.run_cmd(['echo', 'hello world'], {'cwd': '/some/path'})

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_with_cmd_repr(mock_sub_run, caplog):
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    utils_logger = logging.getLogger('iib.workers.tasks.utils')
    utils_logger.disabled = False
    utils_logger.setLevel(logging.DEBUG)

    mock_sub_run.return_value = mock.Mock(returncode=0)

    secret = 'top-secret'
    secret_redacted = '*****'

    utils.run_cmd(['echo', secret], cmd_repr=['echo', secret_redacted])

    mock_sub_run.assert_called_once()
    assert secret not in caplog.text
    assert secret_redacted in caplog.text


@pytest.mark.parametrize('exc_msg', (None, 'Houston, we have a problem!'))
@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed(mock_sub_run, exc_msg):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = 'some failure'
    mock_sub_run.return_value = mock_rv

    expected_exc = exc_msg or 'An unexpected error occurred'
    with pytest.raises(IIBError, match=expected_exc):
        utils.run_cmd(['echo', 'hello'], exc_msg=exc_msg)

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed_opm(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = textwrap.dedent(
        '''
        time="2020-05-12T15:42:19Z" level=info msg="loading bundle file" dir=bundle_tmp775962984/manifests file=serverstatusrequest.crd.yaml load=bundle
        time="2020-05-12T15:42:19Z" level=info msg="loading bundle file" dir=bundle_tmp775962984/manifests file=volumesnapshotlocation.crd.yaml load=bundle
        time="2020-05-12T15:42:19Z" level=error msg="permissive mode disabled" bundles="[registry/namespace/bundle:v1.0-14]" error="error loading bundle from image: Error adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement that couldn't be found"
        Error: error loading bundle from image: Error adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement that couldn't be found
        Usage:
          opm index add [flags]

        Examples:
          # Create an index image from scratch with a single bundle image
          opm index add --bundles quay.io/operator-framework/operator-bundle-prometheus@sha256:a3ee653ffa8a0d2bbb2fabb150a94da6e878b6e9eb07defd40dc884effde11a0 --tag quay.io/operator-framework/monitoring:1.0.0

          # Add a single bundle image to an index image
          opm index add --bundles quay.io/operator-framework/operator-bundle-prometheus:0.15.0 --from-index quay.io/operator-framework/monitoring:1.0.0 --tag quay.io/operator-framework/monitoring:1.0.1

          # Add multiple bundles to an index and generate a Dockerfile instead of an image
          opm index add --bundles quay.io/operator-framework/operator-bundle-prometheus:0.15.0,quay.io/operator-framework/operator-bundle-prometheus:0.22.2 --generate

        Flags:
          -i, --binary-image opm        container image for on-image opm command
          -b, --bundles strings         comma separated list of bundles to add
          -c, --container-tool string   tool to interact with container images (save, build, etc.). One of: [docker, podman] (default "podman")
          -f, --from-index string       previous index to add to
              --generate                if enabled, just creates the dockerfile and saves it to local disk
          -h, --help                    help for add
              --mode string             graph update mode that defines how channel graphs are updated. One of: [replaces, semver, semver-skippatch] (default "replaces")
          -d, --out-dockerfile string   if generating the dockerfile, this flag is used to (optionally) specify a dockerfile name
              --permissive              allow registry load errors
              --skip-tls                skip TLS certificate verification for container image registries while pulling bundles
          -t, --tag string              custom tag for container image being built
        '''  # noqa: E501
    )
    mock_sub_run.return_value = mock_rv

    expected_exc = (
        'Failed to add the bundles to the index image: error loading bundle from image: Error '
        'adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement '
        'that couldn\'t be found'
    )
    with pytest.raises(IIBError, match=expected_exc):
        utils.run_cmd(
            ['opm', 'index', 'add', '--generate', '--bundles', 'quay.io/ns/some_bundle:v1.0'],
            exc_msg='Failed to add the bundles to the index image',
        )

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_skopeo_inspect(mock_run_cmd):
    mock_run_cmd.return_value = '{"Name": "some-image"}'
    image = 'docker://some-image:latest'
    rv = utils.skopeo_inspect(image)
    assert rv == {"Name": "some-image"}
    skopeo_args = mock_run_cmd.call_args[0][0]
    expected = ['skopeo', '--command-timeout', '300s', 'inspect', image]
    assert skopeo_args == expected


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_podman_pull(mock_run_cmd):
    image = 'some-image:latest'
    utils.podman_pull(image)
    mock_run_cmd.assert_called_once_with(['podman', 'pull', image], exc_msg=mock.ANY)
