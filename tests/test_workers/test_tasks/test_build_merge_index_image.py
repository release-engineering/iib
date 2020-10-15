# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_merge_index_image


@pytest.mark.parametrize(
    'target_index, target_index_resolved, binary_image',
    (
        ('target-from-index:1.0', 'target-index@sha256:resolved', 'binary-image:1.0'),
        (None, None, None),
    ),
)
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._deprecate_bundles')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_external_arch_pull_spec')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_bundles_missing_in_source')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_present_bundles')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_merge_index_image._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_merge_index_image._cleanup')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
def test_handle_merge_request(
    mock_add_label_to_index,
    mock_cleanup,
    mock_prfb,
    mock_uiibs,
    mock_srs,
    mock_gpb,
    mock_abmis,
    mock_gbfdl,
    mock_geaps,
    mock_dep_b,
    mock_bi,
    mock_pi,
    mock_capml,
    mock_vii,
    mock_uiips,
    target_index,
    target_index_resolved,
    binary_image,
):
    prebuild_info = {
        'arches': {'amd64', 'other_arch'},
        'binary_image': binary_image,
        'target_ocp_version': '4.6',
        'source_from_index_resolved': 'source-index@sha256:resolved',
        'target_index_resolved': target_index_resolved,
        'distribution_scope': 'stage',
    }
    mock_prfb.return_value = prebuild_info
    mock_gbfdl.return_value = ['some-bundle:1.0']
    binary_image_config = {'prod': {'v4.5': 'some_image'}, 'stage': {'stage': 'some_other_img'}}

    build_merge_index_image.handle_merge_request(
        'source-from-index:1.0',
        ['some-bundle:1.0'],
        1,
        binary_image,
        target_index,
        distribution_scope='stage',
        binary_image_config=binary_image_config,
    )

    mock_cleanup.assert_called_once()
    mock_prfb.assert_called_once_with(
        1,
        binary_image,
        overwrite_from_index_token=None,
        source_from_index='source-from-index:1.0',
        target_index=target_index,
        distribution_scope='stage',
        binary_image_config=binary_image_config,
    )
    mock_uiibs.assert_called_once_with(1, prebuild_info)
    if target_index:
        assert mock_gpb.call_count == 2
    else:
        assert mock_gpb.call_count == 1
    mock_vii.assert_not_called()
    mock_abmis.assert_called_once()
    mock_gbfdl.assert_called_once()
    mock_geaps.assert_called_once()
    mock_dep_b.assert_called_once()
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    assert mock_capml.call_count == 1
    mock_uiips.assert_called_once()


@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._deprecate_bundles')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_external_arch_pull_spec')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_bundles_from_deprecation_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_bundles_missing_in_source')
@mock.patch('iib.workers.tasks.build_merge_index_image._get_present_bundles')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
@mock.patch('iib.workers.tasks.build_merge_index_image._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_merge_index_image._prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_merge_index_image._cleanup')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
def test_handle_merge_request_no_deprecate(
    mock_add_label_to_index,
    mock_cleanup,
    mock_prfb,
    mock_uiibs,
    mock_srs,
    mock_gpb,
    mock_abmis,
    mock_gbfdl,
    mock_geaps,
    mock_dep_b,
    mock_bi,
    mock_pi,
    mock_capml,
    mock_vii,
    mock_uiips,
):
    prebuild_info = {
        'arches': {'amd64', 'other_arch'},
        'binary_image': 'binary-image:1.0',
        'target_ocp_version': '4.6',
        'source_from_index_resolved': 'source-index@sha256:resolved',
        'target_index_resolved': 'target-index@sha256:resolved',
        'distribution_scope': 'stage',
    }
    mock_prfb.return_value = prebuild_info
    mock_gbfdl.return_value = []

    build_merge_index_image.handle_merge_request(
        'source-from-index:1.0',
        ['some-bundle:1.0'],
        1,
        'binary-image:1.0',
        'target-from-index:1.0',
        distribution_scope='stage',
    )

    mock_cleanup.assert_called_once()
    mock_prfb.assert_called_once_with(
        1,
        'binary-image:1.0',
        binary_image_config=None,
        overwrite_from_index_token=None,
        source_from_index='source-from-index:1.0',
        target_index='target-from-index:1.0',
        distribution_scope='stage',
    )
    mock_uiibs.assert_called_once_with(1, prebuild_info)
    assert mock_gpb.call_count == 2
    mock_abmis.assert_called_once()
    mock_gbfdl.assert_called_once()
    mock_geaps.assert_called_once()
    assert mock_dep_b.call_count == 0
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    mock_vii.assert_not_called()
    mock_capml.assert_called_once()
    mock_uiips.assert_called_once()


@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image._opm_index_add')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
def test_add_bundles_missing_in_source(
    mock_srs, mock_oia, mock_aolti, mock_bi, mock_pi, mock_capml
):
    source_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:234567',
            'csvName': 'bundle2-2.0',
        },
        {
            'packageName': 'bundle5',
            'version': '5.0-2',
            'bundlePath': 'quay.io/bundle2@sha256:456132',
            'csvName': 'bundle5-5.0',
        },
    ]
    target_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:456789',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle4',
            'version': '4.0',
            'bundlePath': 'quay.io/bundle4@sha256:567890',
            'csvName': 'bundle4-4.0',
        },
        {
            'packageName': 'bundle5',
            'version': '5.0-1',
            'bundlePath': 'quay.io/bundle4@sha256:569854',
            'csvName': 'bundle5-5.0',
        },
    ]
    missing_bundles = build_merge_index_image._add_bundles_missing_in_source(
        source_bundles,
        target_bundles,
        'some_dir',
        'binary-image:4.5',
        'index-image:4.6',
        1,
        'amd64',
        '4.6',
    )
    assert missing_bundles == [
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:456789',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle4',
            'version': '4.0',
            'bundlePath': 'quay.io/bundle4@sha256:567890',
            'csvName': 'bundle4-4.0',
        },
    ]
    mock_srs.assert_called_once()
    mock_oia.assert_called_once_with(
        'some_dir',
        ['quay.io/bundle3@sha256:456789', 'quay.io/bundle4@sha256:567890'],
        'binary-image:4.5',
        'index-image:4.6',
        None,
    )
    assert mock_aolti.call_count == 2
    mock_bi.assert_called_once()
    mock_pi.assert_called_once()
    mock_capml.assert_called_once()


@pytest.mark.parametrize(
    'source_bundles, target_bundles, error_msg',
    (
        (
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                    'csvName': 'bundle1-1.0',
                },
                {
                    'packageName': 'bundle2',
                    'version': '2.0',
                    'bundlePath': 'quay.io/bundle2:234567',
                    'csvName': 'bundle2-2.0',
                },
            ],
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                },
                {
                    'packageName': 'bundle3',
                    'version': '3.0',
                    'bundlePath': 'quay.io/bundle3@sha256:456789',
                },
                {
                    'packageName': 'bundle4',
                    'version': '4.0',
                    'bundlePath': 'quay.io/bundle4@sha256:567890',
                },
            ],
            'Bundle quay.io/bundle2:234567 in the source index image is not defined via digest',
        ),
        (
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                    'csvName': 'bundle1-1.0',
                },
                {
                    'packageName': 'bundle2',
                    'version': '2.0',
                    'bundlePath': 'quay.io/bundle2:234567',
                    'csvName': 'bundle2-2.0',
                },
            ],
            [
                {
                    'packageName': 'bundle1',
                    'version': '1.0',
                    'bundlePath': 'quay.io/bundle1@sha256:123456',
                },
                {
                    'packageName': 'bundle3',
                    'version': '3.0',
                    'bundlePath': 'quay.io/bundle3@sha256:456789',
                },
                {
                    'packageName': 'bundle4',
                    'version': '4.0',
                    'bundlePath': 'quay.io/bundle4@sha256:567890',
                },
            ],
            'Bundle quay.io/bundle2:234567 in the source index image is not defined via digest',
        ),
    ),
)
@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image._opm_index_add')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
def test_add_bundles_missing_in_source_error_tag_specified(
    mock_srs,
    mock_oia,
    mock_aolti,
    mock_bi,
    mock_pi,
    mock_capml,
    source_bundles,
    target_bundles,
    error_msg,
):
    with pytest.raises(IIBError, match=error_msg):
        build_merge_index_image._add_bundles_missing_in_source(
            source_bundles,
            target_bundles,
            'some_dir',
            'binary-image:4.5',
            'index-image:4.6',
            1,
            'amd64',
            '4.6',
        )


@mock.patch('iib.workers.tasks.build_merge_index_image._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_merge_index_image._push_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._build_image')
@mock.patch('iib.workers.tasks.build_merge_index_image._add_label_to_index')
@mock.patch('iib.workers.tasks.build_merge_index_image._opm_index_add')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_request_state')
def test_add_bundles_missing_in_source_none_missing(
    mock_srs, mock_oia, mock_aolti, mock_bi, mock_pi, mock_capml
):
    source_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:123456',
            'csvName': 'bundle2-2.0',
        },
        {
            'packageName': 'bundle3',
            'version': '3.0',
            'bundlePath': 'quay.io/bundle3@sha256:123456',
            'csvName': 'bundle3-3.0',
        },
        {
            'packageName': 'bundle4',
            'version': '4.0',
            'bundlePath': 'quay.io/bundle4@sha256:123456',
            'csvName': 'bundle4-4.0',
        },
    ]
    target_bundles = [
        {
            'packageName': 'bundle1',
            'version': '1.0',
            'bundlePath': 'quay.io/bundle1@sha256:123456',
            'csvName': 'bundle1-1.0',
        },
        {
            'packageName': 'bundle2',
            'version': '2.0',
            'bundlePath': 'quay.io/bundle2@sha256:123456',
            'csvName': 'bundle2-2.0',
        },
    ]
    missing_bundles = build_merge_index_image._add_bundles_missing_in_source(
        source_bundles,
        target_bundles,
        'some_dir',
        'binary-image:4.5',
        'index-image:4.6',
        1,
        'amd64',
        '4.6',
    )
    assert missing_bundles == []
    mock_srs.assert_called_once()
    mock_oia.assert_called_once()
    assert mock_aolti.call_count == 2
    mock_bi.assert_called_once()
    mock_pi.assert_called_once()
    mock_capml.assert_called_once()


@mock.patch('iib.workers.tasks.build_merge_index_image._get_resolved_bundles')
def test_get_bundles_from_deprecation_list(mock_grb):
    present_bundles = [
        {'packageName': 'bundle1', 'version': '1.0', 'bundlePath': 'quay.io/bundle1@sha256:123456'},
        {'packageName': 'bundle2', 'version': '2.0', 'bundlePath': 'quay.io/bundle2@sha256:987654'},
        {'packageName': 'bundle3', 'version': '3.0', 'bundlePath': 'quay.io/bundle3@sha256:not555'},
    ]
    deprecation_list = [
        'quay.io/bundle1@sha256:123456',
        'quay.io/bundle2@sha256:987654',
        'quay.io/bundle4@sha256:1a2bcd',
    ]
    mock_grb.return_value = [
        'quay.io/bundle1@sha256:123456',
        'quay.io/bundle2@sha256:987654',
        'quay.io/bundle3@sha256:abcdef',
    ]
    deprecate_bundles = build_merge_index_image._get_bundles_from_deprecation_list(
        present_bundles, deprecation_list
    )
    assert deprecate_bundles == ['quay.io/bundle1@sha256:123456', 'quay.io/bundle2@sha256:987654']
    mock_grb.assert_called_once_with(deprecation_list)


@mock.patch('iib.workers.tasks.build_merge_index_image.run_cmd')
@mock.patch('iib.workers.tasks.build_merge_index_image.set_registry_token')
def test_deprecate_bundles(mock_srt, mock_run_cmd):
    bundles = ['quay.io/bundle1:1.0', 'quay.io/bundle2:2.0']
    from_index = 'quay.io/index-image:4.6'
    binary_image = 'quay.io/binary-image:4.6'
    cmd = [
        'opm',
        'index',
        'deprecatetruncate',
        '--generate',
        '--binary-image',
        binary_image,
        '--from-index',
        from_index,
        '--bundles',
        ','.join(bundles),
    ]
    build_merge_index_image._deprecate_bundles(bundles, 'some_dir', binary_image, from_index, '4.6')
    mock_run_cmd.assert_called_once_with(
        cmd, {'cwd': 'some_dir'}, exc_msg='Failed to deprecate the bundles'
    )
