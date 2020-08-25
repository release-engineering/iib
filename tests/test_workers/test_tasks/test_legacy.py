# SPDX-License-Identifier: GPL-3.0-or-later
import json
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import legacy


@pytest.mark.parametrize(
    'backport_label, ocp_version, force_backport, expect_backport',
    (
        ('true', 'v4.5', True, True),
        ('true', 'v4.5', False, True),
        ('True', 'v4.5', False, True),
        ('false', 'v4.5', True, True),
        ('false', 'v4.5', False, False),
        ('False', 'v4.5', False, False),
        ('', 'v4.5', False, False),
        ('true', 'v4.6', True, False),
        ('true', 'v4.6', False, False),
        ('false', 'v4.6', True, False),
    ),
)
@mock.patch('iib.workers.tasks.legacy.set_request_state')
@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_legacy_support_packages(
    mock_skopeo_inspect, mock_srs, backport_label, ocp_version, force_backport, expect_backport
):
    mock_skopeo_inspect.return_value = {
        'config': {
            'Labels': {
                'com.redhat.delivery.backport': backport_label,
                'operators.operatorframework.io.bundle.package.v1': 'prometheus',
            }
        }
    }

    packages = legacy.get_legacy_support_packages(
        ['some_bundle'], 1, ocp_version, force_backport=force_backport
    )
    if ocp_version == 'v4.5':
        mock_skopeo_inspect.assert_called_once()
    else:
        mock_skopeo_inspect.assert_not_called()
    if expect_backport:
        assert packages == {'prometheus'}
    else:
        assert packages == set()

    if force_backport and expect_backport:
        mock_srs.assert_called_once_with(1, 'in_progress', 'Backport legacy support will be forced')
    else:
        mock_srs.assert_not_called()


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
    mock_requests.return_value.json.return_value = {"message": "Unauthorized"}
    expected = 'Push to organization in the legacy app registry was unsucessful: Unauthorized'
    with pytest.raises(IIBError, match=expected):
        legacy._push_package_manifest('something/download-pkg', 'cnr_token', 'organization')
    mock_open.assert_called_once_with('something/manifests.zip', 'rb')
    mock_requests.assert_called_once()


@mock.patch('iib.workers.tasks.legacy.open')
@mock.patch('iib.workers.tasks.legacy.requests.post')
def test_push_package_manifest_failure_invalid_json(mock_requests, mock_open):
    mock_requests.return_value.ok = False
    mock_requests.return_value.json.side_effect = json.JSONDecodeError('Invalid Json', '', 1)
    mock_requests.return_value.text = 'Something went wrong'
    expected = (
        'Push to organization in the legacy app registry was unsucessful: Something went wrong'
    )
    with pytest.raises(IIBError, match=expected):
        legacy._push_package_manifest('something/download-pkg', 'cnr_token', 'organization')
    mock_open.assert_called_once_with('something/manifests.zip', 'rb')
    mock_requests.assert_called_once()


@mock.patch('iib.workers.tasks.legacy.run_cmd')
def test_opm_index_export(mock_run_cmd):
    legacy._opm_index_export('from:index', 'prometheus', '/')

    # This is only directly called once in the actual function
    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'export']
    assert 'prometheus' in opm_args


@mock.patch('iib.workers.tasks.legacy._verify_package_info')
@mock.patch('iib.workers.tasks.legacy._zip_package')
@mock.patch('iib.workers.tasks.legacy._push_package_manifest')
@mock.patch('iib.workers.tasks.legacy.set_omps_operator_version')
@mock.patch('iib.workers.tasks.legacy.set_request_state')
@mock.patch('iib.workers.tasks.legacy._opm_index_export')
def test_export_legacy_packages(mock_oie, mock_srs, mock_soov, mock_ppm, mock_zp, mock_vpi):

    mock_ppm.return_value = {
        'extracted_files': [
            '1.1.1/backup.crd.yaml',
            '1.1.1/backupstoragelocation.crd.yaml',
            '1.1.1/deletebackuprequest.crd.yaml',
            '1.1.1/downloadrequest.crd.yaml',
            '1.1.1/mig-operator.v1.1.1.clusterserviceversion.yaml',
            '1.1.1/migcluster.crd.yaml',
            '1.1.1/migmigration.crd.yaml',
            '1.1.1/migplan.crd.yaml',
            '1.1.1/migrationcontroller.crd.yaml',
            '1.1.1/migstorage.crd.yaml',
            '1.1.1/podvolumebackup.crd.yaml',
            '1.1.1/podvolumerestore.crd.yaml',
            '1.1.1/resticrepository.crd.yaml',
            '1.1.1/restore.crd.yaml',
            '1.1.1/schedule.crd.yaml',
            '1.1.1/serverstatusrequest.crd.yaml',
            '1.1.1/volumesnapshotlocation.crd.yaml',
            'package.yaml',
        ],
        'organization': 'redhat-operators-devtest',
        'repo': 'lgallett-bundle',
        'version': '37.0.0',
    }

    packages = {'lgallett-bundle'}
    legacy.export_legacy_packages(packages, 3, 'from:index', 'token', 'org')

    mock_oie.assert_called_once()
    mock_vpi.assert_called_once()
    mock_zp.assert_called_once()
    mock_ppm.assert_called_once()
    mock_srs.assert_called_once()
    mock_soov.assert_called_once_with(3, {'lgallett-bundle': '37.0.0'})


@pytest.mark.parametrize(
    'cnr_token_val, error_msg',
    (
        (
            None,
            'Legacy support is required for prometheus;'
            ' Both cnr_token and organization should be non-empty strings',
        ),
        ('token', 'IIB is not configured to handle the legacy app registry'),
    ),
)
@mock.patch('iib.workers.tasks.legacy.get_worker_config')
def test_validate_legacy_params_and_config_failure(mock_gwc, cnr_token_val, error_msg):
    mock_gwc.return_value = {'iib_omps_url': None}
    with pytest.raises(IIBError, match=error_msg):
        legacy.validate_legacy_params_and_config(
            ['prometheus'], ['quay.io/msd/bundle'], cnr_token_val, 'org'
        )


def test_validate_legacy_params_and_config_success():
    try:
        legacy.validate_legacy_params_and_config(
            ['prometheus'], ['quay.io/msd/bundle'], 'cnr_token_val', 'org'
        )
    except IIBError as err:
        pytest.fail(f'Unexpected failure: {err}')
