# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

from operator_manifest.operator import ImageName
import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_recursive_related_bundles


# Re-use the yaml instance to ensure configuration is also used in tests
yaml = build_recursive_related_bundles.yaml


@pytest.mark.parametrize('organization', ('acme', None))
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._cleanup')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.get_resolved_image')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.podman_pull')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._copy_files_from_image')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.set_request_state')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.get_worker_config')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.update_request')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.OperatorManifest.from_directory')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._get_bundle_metadata')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.get_related_bundle_images')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.write_related_bundles_file')
def test_handle_recusrsive_related_bundles_request(
    mock_wrbf,
    mock_grbi,
    mock_gbm,
    mock_omfd,
    mock_ur,
    mock_gwc,
    mock_srs,
    mock_aob,
    mock_cffi,
    mock_temp_dir,
    mock_pp,
    mock_gri,
    mock_cleanup,
    organization,
    tmpdir,
):
    parent_bundle_image = 'bundle-image:latest'
    parent_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    org = organization
    request_id = 99

    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)
    mock_gri.return_value = parent_bundle_image_resolved
    mock_gwc.return_value = {
        'iib_max_recursive_related_bundles': 15,
        'iib_request_recursive_related_bundles_dir': 'some-dir',
        'iib_registry': 'quay.io',
    }
    mock_omfd.return_value = 'operator-manifest'
    mock_gbm.side_effect = [
        {'found_pullspecs': [ImageName.parse('pullspec-1'), ImageName.parse('pullspec-2')]},
        {'found_pullspecs': []},
        {'found_pullspecs': []},
    ]
    mock_grbi.side_effect = [['pullspec-1', 'pullspec-2'], [], []]

    build_recursive_related_bundles.handle_recursive_related_bundles_request(
        parent_bundle_image, org, request_id
    )
    mock_cleanup.assert_called_once()
    assert mock_gbm.call_count == 3
    assert mock_grbi.call_count == 3
    assert mock_ur.call_count == 3
    if org:
        mock_aob.cal_count = 3
    else:
        mock_aob.assert_not_called()
    mock_gri.assert_called_once()
    mock_wrbf.assert_called_once_with(
        ['pullspec-2', 'pullspec-1', 'bundle-image@sha256:abcdef'],
        99,
        'some-dir',
        'recursive_related_bundles',
    )


@mock.patch('iib.workers.tasks.build_recursive_related_bundles._cleanup')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.get_resolved_image')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.podman_pull')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._copy_files_from_image')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._adjust_operator_bundle')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.set_request_state')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.get_worker_config')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.update_request')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.OperatorManifest.from_directory')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles._get_bundle_metadata')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.get_related_bundle_images')
@mock.patch('iib.workers.tasks.build_recursive_related_bundles.write_related_bundles_file')
def test_handle_recusrsive_related_bundles_request_max_bundles_reached(
    mock_wrbf,
    mock_grbi,
    mock_gbm,
    mock_omfd,
    mock_ur,
    mock_gwc,
    mock_srs,
    mock_aob,
    mock_cffi,
    mock_temp_dir,
    mock_pp,
    mock_gri,
    mock_cleanup,
    tmpdir,
):
    parent_bundle_image = 'bundle-image:latest'
    parent_bundle_image_resolved = 'bundle-image@sha256:abcdef'
    organization = 'acme'
    request_id = 99

    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)
    mock_gri.return_value = parent_bundle_image_resolved
    mock_gwc.return_value = {
        'iib_max_recursive_related_bundles': 15,
        'iib_request_recursive_related_bundles_dir': 'some-dir',
        'iib_registry': 'quay.io',
    }
    mock_omfd.return_value = 'operator-manifest'
    mock_gbm.return_value = {
        'found_pullspecs': [
            ImageName.parse('child-bundle-1'),
            ImageName.parse('child-bundle-2'),
            ImageName.parse('child-bundle-3'),
        ]
    }
    mock_grbi.return_value = ['child-bundle-1', 'child-bundle-2', 'child-bundle-3']

    expected = 'Max number of related bundles exceeded. Potential DOS attack!'
    with pytest.raises(IIBError, match=expected):
        build_recursive_related_bundles.handle_recursive_related_bundles_request(
            parent_bundle_image, organization, request_id
        )
        assert mock_gbm.call_count == 5
        assert mock_grbi.call_count == 5
        assert mock_ur.call_count == 2
        mock_gri.assert_called_once()
