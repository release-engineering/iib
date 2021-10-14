# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build_create_empty_index
from iib.workers.tasks.utils import RequestConfigCreateIndexImage


@mock.patch('iib.workers.tasks.build_create_empty_index.grpcurl_get_db_data')
def test_get_present_operators(mock_grpcurl, tmpdir):

    mock_grpcurl.side_effect = ['{\n"name": "package1"\n}\n{\n"name": "package2"\n}\n']
    operators = build_create_empty_index._get_present_operators(
        'quay.io/index-image:4.5', tmpdir.join("index.db")
    )

    mock_grpcurl.assert_called_once()
    assert operators == ['package1', 'package2']


@mock.patch('iib.workers.tasks.build_create_empty_index.grpcurl_get_db_data')
def test_get_no_present_operators(mock_grpcurl, tmpdir):

    mock_grpcurl.return_value = None
    operators = build_create_empty_index._get_present_operators(
        'quay.io/index-image:4.5', tmpdir.join("index.db")
    )

    assert mock_grpcurl.call_count == 1
    assert operators == []


@pytest.mark.parametrize('binary_image', ('binary-image:latest', None))
@pytest.mark.parametrize('from_index', ('index-image:latest', 'index-image:latest'))
@mock.patch('iib.workers.tasks.build_create_empty_index._cleanup')
@mock.patch('iib.workers.tasks.build_create_empty_index.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_create_empty_index._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_create_empty_index.set_request_state')
@mock.patch('iib.workers.tasks.build_create_empty_index._get_present_operators')
@mock.patch('iib.workers.tasks.build_create_empty_index._opm_index_rm')
@mock.patch('iib.workers.tasks.build_create_empty_index._add_label_to_index')
@mock.patch('iib.workers.tasks.build_create_empty_index._build_image')
@mock.patch('iib.workers.tasks.build_create_empty_index._push_image')
@mock.patch('iib.workers.tasks.build_create_empty_index._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_create_empty_index._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_create_empty_index.is_image_dc')
def test_handle_create_empty_index_request(
    mock_iidc,
    mock_uiips,
    mock_capml,
    mock_pi,
    mock_bi,
    mock_alti,
    mock_oir,
    mock_gpo,
    mock_srs,
    mock_uiibs,
    mock_prfb,
    mock_cleanup,
    binary_image,
    from_index,
):
    arches = {'amd64', 's390x'}
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    mock_iidc.return_value = False
    labels = {"version": "v4.5"}
    from_index_resolved = "index-image-resolved:latest"
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': 'binary_image',
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.5',
        'distribution_scope': 'prod',
    }

    mock_gpo.return_value = ["operator1", "operator2"]

    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec

    build_create_empty_index.handle_create_empty_index_request(
        from_index=from_index,
        request_id=3,
        binary_image=binary_image,
        labels=labels,
        binary_image_config=binary_image_config,
    )

    mock_cleanup.assert_called_once()
    mock_prfb.assert_called_once_with(
        3,
        RequestConfigCreateIndexImage(
            _binary_image=binary_image,
            from_index=from_index,
            labels=labels,
            binary_image_config=binary_image_config,
        ),
    )

    mock_uiibs.asser_called_once()

    assert mock_srs.call_count == 5
    mock_oir.assert_called_once()
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2

    mock_capml.assert_called_once_with(3, {'s390x', 'amd64'}, [])
    mock_uiips.assert_called_once_with(
        output_pull_spec=output_pull_spec,
        request_id=3,
        arches=arches,
        from_index=from_index,
        resolved_prebuild_from_index=from_index_resolved,
    )


@mock.patch('iib.workers.tasks.build_create_empty_index._cleanup')
@mock.patch('iib.workers.tasks.build_create_empty_index.is_image_dc')
def test_handle_create_empty_index_request_raises(mock_iidc, mock_c):
    mock_iidc.return_value = True
    with pytest.raises(IIBError):
        build_create_empty_index.handle_create_empty_index_request(
            from_index='index-image:latest',
            request_id=3,
            binary_image='binary-image:latest',
            labels={"version": "v4.5"},
            binary_image_config={'prod': {'v4.5': 'some_image'}},
        )
