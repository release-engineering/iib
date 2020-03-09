# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import utils


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_labels(mock_si):
    skopeo_rv = {'Labels': {'some_label': 'value'}}
    mock_si.return_value = skopeo_rv
    assert utils.get_image_labels('some-image:latest') == skopeo_rv['Labels']


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
    mock_rv.stderr = (
        'time="2020-03-09T08:58:21-04:00" level=info msg="loading bundle file" '
        'dir=bundle_tmp922306995/manifests file=volumesnapshotlocation.crd.yaml load=bundle\n'
        'time="2020-03-09T08:58:21-04:00" level=fatal msg="permissive mode disabled" '
        'bundles="[quay.io/ns/some_bundle:v1.0]" error="error loading bundle from image: Error '
        'adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement '
        'that couldn\'t be found"'
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
    expected = ['skopeo', '--command-timeout', '30s', 'inspect', image]
    assert skopeo_args == expected
