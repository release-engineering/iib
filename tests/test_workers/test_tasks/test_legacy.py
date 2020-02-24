# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import legacy


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_legacy_support_packages(mock_skopeo_inspect):
    mock_skopeo_inspect.return_value = {
        'Labels': {
            'com.redhat.delivery.backport': True,
            'operators.operatorframework.io.bundle.package.v1': 'prometheus',
        }
    }

    packages = legacy.get_legacy_support_packages(['some_bundle'])
    mock_skopeo_inspect.assert_called_once()
    assert packages == {'prometheus'}


@mock.patch('os.listdir')
def test_verify_package_info_missing_pkg(mock_listdir):
    mock_listdir.return_value = ['package.yaml']
    with pytest.raises(
        IIBError, match='package download-pkg is missing in index image index:image'
    ):
        legacy._verify_package_info('/some/dir/download-pkg', 'index:image')


@mock.patch('shutil.make_archive')
def test_zip_package_success(mock_shutil):
    legacy._zip_package('something/download-pkg')
    mock_shutil.assert_called_once_with('something/manifests', 'zip', 'something/download-pkg')


@mock.patch('shutil.make_archive')
def test_zip_package_failure(mock_shutil):
    mock_shutil.side_effect = AttributeError('Nothing works!')
    with pytest.raises(IIBError, match='Unable to zip exported package for download-pkg'):
        legacy._zip_package('something/download-pkg')


@mock.patch('iib.workers.tasks.legacy.open')
@mock.patch('iib.workers.tasks.legacy.requests.post')
def test_push_package_manifest_success(mock_requests, mock_open):
    mock_requests.return_value.ok = True
    legacy._push_package_manifest('something/download-pkg', 'cnr_token', 'organization')
    mock_open.assert_called_once_with('something/manifests.zip', 'rb')
    mock_requests.assert_called_once()


@mock.patch('iib.workers.tasks.legacy.open')
@mock.patch('iib.workers.tasks.legacy.requests.post')
def test_push_package_manifest_failure(mock_requests, mock_open):
    mock_requests.return_value.ok = False
    mock_requests.return_value.json.return_value = {"error": "Unauthorized"}
    expected = 'Push to organization in the legacy app registry was unsucessful'
    with pytest.raises(IIBError, match=expected):
        legacy._push_package_manifest('something/download-pkg', 'cnr_token', 'organization')
    mock_open.assert_called_once_with('something/manifests.zip', 'rb')
    mock_requests.assert_called_once()


@mock.patch('iib.workers.tasks.legacy.run_cmd')
@mock.patch('iib.workers.tasks.legacy._verify_package_info')
@mock.patch('iib.workers.tasks.legacy._zip_package')
@mock.patch('iib.workers.tasks.legacy._push_package_manifest')
@mock.patch('iib.workers.tasks.legacy.set_request_state')
def test_opm_index_export(mock_srs, mock_ppm, mock_zp, mock_vpi, mock_run_cmd):
    packages = ['prometheus']
    legacy.opm_index_export(packages, 3, 'from:index', 'token', 'org')

    # This is only directly called once in the actual function
    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'export']
    assert 'prometheus' in opm_args
    mock_vpi.assert_called_once()
    mock_zp.assert_called_once()
    mock_ppm.assert_called_once()
    mock_srs.assert_called_once()
