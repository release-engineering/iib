from unittest import mock

import json
import os
import pytest


from iib.workers.tasks import build_add_deprecations
from iib.workers.tasks.utils import RequestConfigAddDeprecations, IIBError
from iib.workers.config import get_worker_config


@mock.patch('iib.workers.tasks.build_add_deprecations._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build_add_deprecations._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build_add_deprecations._push_image')
@mock.patch('iib.workers.tasks.build_add_deprecations._build_image')
@mock.patch('iib.workers.tasks.build_add_deprecations._add_label_to_index')
@mock.patch('iib.workers.tasks.build_add_deprecations.add_deprecations_to_index')
@mock.patch('iib.workers.tasks.opm_operations.verify_operators_exists')
@mock.patch('iib.workers.tasks.build_add_deprecations.verify_operators_exists')
@mock.patch('iib.workers.tasks.build_add_deprecations.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_add_deprecations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_add_deprecations.set_request_state')
@mock.patch('iib.workers.tasks.opm_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_add_deprecations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_add_deprecations._cleanup')
def test_handle_add_deprecations_request(
    mock_cleanup,
    mock_prfb,
    mock_sov,
    mock_srs,
    mock_uiibs,
    mock_temp_dir,
    mock_voe,
    mock_ovoe,
    mock_adti,
    mock_alti,
    mock_bi,
    mock_pi,
    mock_cpml,
    mock_uiips,
    tmpdir,
):
    arches = {'amd64', 's390x'}
    request_id = 11
    from_index = 'from-index:latest'
    from_index_resolved = 'from-index@sha256:bcdefg'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    binary_image_resolved = 'binary-image@sha256:abcdef'
    operator_package = 'deprecation-operator'
    deprecation_schema = '{"schema":"olm.deprecations","message":"deprecation-msg"}'

    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': binary_image_resolved,
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.6',
        'distribution_scope': "prod",
        'operator_package': 'deprecation-operator',
        'deprecation_schema': '{"schema":"olm.deprecations","message":"deprecation-msg"}',
    }

    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)
    mock_voe.return_value = set([operator_package]), "index/db/path"

    build_add_deprecations.handle_add_deprecations_request(
        request_id=request_id,
        operator_package=operator_package,
        deprecation_schema=deprecation_schema,
        from_index=from_index,
        binary_image=binary_image,
        binary_image_config=binary_image_config,
    )
    mock_prfb.assert_called_once_with(
        request_id,
        RequestConfigAddDeprecations(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=None,
            binary_image_config=binary_image_config,
            distribution_scope='prod',
            operator_package=operator_package,
            deprecation_schema=deprecation_schema,
        ),
    )
    mock_sov.assert_called_once_with(from_index_resolved)
    mock_cpml.assert_called_once_with(request_id, {'s390x', 'amd64'}, None)
    mock_voe.assert_called_once_with(from_index_resolved, tmpdir, [operator_package], None)
    mock_voe.return_value = set(operator_package), "index/db/path"
    mock_adti.assert_called_once_with(
        request_id,
        tmpdir,
        from_index_resolved,
        operator_package,
        deprecation_schema,
        binary_image_resolved,
        "index/db/path",
    )
    assert mock_srs.call_count == 3
    assert mock_alti.call_count == 2
    assert mock_bi.call_count == 2
    assert mock_pi.call_count == 2
    assert mock_srs.call_args[0][1] == 'complete'


@mock.patch('iib.workers.tasks.build_add_deprecations._push_image')
@mock.patch('iib.workers.tasks.build_add_deprecations._build_image')
@mock.patch('iib.workers.tasks.build_add_deprecations.add_deprecations_to_index')
@mock.patch('iib.workers.tasks.opm_operations.verify_operators_exists')
@mock.patch('iib.workers.tasks.build_add_deprecations.verify_operators_exists')
@mock.patch('iib.workers.tasks.build_add_deprecations.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build_add_deprecations._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build_add_deprecations.set_request_state')
@mock.patch('iib.workers.tasks.opm_operations.Opm.set_opm_version')
@mock.patch('iib.workers.tasks.build_add_deprecations.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build_add_deprecations._cleanup')
def test_add_deprecation_operator_not_exist(
    mock_cleanup,
    mock_prfb,
    mock_sov,
    mock_srs,
    mock_uiibs,
    mock_temp_dir,
    mock_voe,
    mock_ovoe,
    mock_adti,
    mock_bi,
    mock_pi,
    tmpdir,
):
    arches = {'amd64', 's390x'}
    request_id = 11
    from_index = 'from-index:latest'
    from_index_resolved = 'from-index@sha256:bcdefg'
    binary_image = 'binary-image:latest'
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    binary_image_resolved = 'binary-image@sha256:abcdef'
    operator_package = 'deprecation-operator'
    deprecation_schema = '{"schema":"olm.deprecations","message":"deprecation-msg"}'

    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': binary_image_resolved,
        'from_index_resolved': from_index_resolved,
        'ocp_version': 'v4.6',
        'distribution_scope': "prod",
        'operator_package': 'deprecation-operator',
        'deprecation_schema': '{"schema":"olm.deprecations","message":"deprecation-msg"}',
    }

    mock_temp_dir.return_value.__enter__.return_value = str(tmpdir)
    mock_voe.return_value = None, None

    with pytest.raises(
        IIBError,
        match=f"Cannot add deprecations for {operator_package},"
        " It is either not present in index or opted in fbc",
    ):
        build_add_deprecations.handle_add_deprecations_request(
            request_id=request_id,
            operator_package=operator_package,
            deprecation_schema=deprecation_schema,
            from_index=from_index,
            binary_image=binary_image,
            binary_image_config=binary_image_config,
        )
        mock_prfb.assert_called_once_with(
            request_id,
            RequestConfigAddDeprecations(
                _binary_image=binary_image,
                from_index=from_index,
                overwrite_from_index_token=None,
                binary_image_config=binary_image_config,
                distribution_scope='prod',
                operator_package=operator_package,
                deprecation_schema=deprecation_schema,
            ),
        )
        mock_sov.assert_called_once_with(from_index_resolved)
        mock_adti.call_count == 0
        mock_bi.call_count == 0
        mock_pi.call_count == 0


@mock.patch('iib.workers.tasks.build_add_deprecations.create_dockerfile')
@mock.patch('iib.workers.tasks.build_add_deprecations.generate_cache_locally')
@mock.patch('iib.workers.tasks.build_add_deprecations.opm_validate')
@mock.patch('iib.workers.tasks.build_add_deprecations.get_catalog_dir')
@mock.patch('iib.workers.tasks.build_add_deprecations.set_request_state')
def test_add_deprecations_to_index_dep_dir_not_exist(
    mock_srs, mock_gcd, mock_ov, mock_gcl, mock_cd, tmpdir
):
    request_id = 11
    from_index_resolved = 'from-index@sha256:bcdefg'
    binary_image_resolved = 'binary-image@sha256:abcdef'
    operator_package = 'deprecation-operator'
    deprecation_schema = '{"schema": "olm.deprecations", "message": "deprecation-msg"}'
    index_db_path = '/tmpdir/indexdb'

    configs_dir = os.path.join(tmpdir, 'configs')
    os.makedirs(configs_dir)
    mock_gcd.return_value = configs_dir
    build_add_deprecations.add_deprecations_to_index(
        request_id,
        tmpdir,
        from_index_resolved,
        operator_package,
        deprecation_schema,
        binary_image_resolved,
        index_db_path,
    )
    mock_ov.assert_called_once()
    mock_gcl.assert_called_once_with(
        base_dir=tmpdir,
        fbc_dir=mock_gcd.return_value,
        local_cache_path=os.path.join(tmpdir, 'cache'),
    )
    mock_cd.assert_called_once()
    # assert deprecations_file and dir exist
    assert os.path.exists(
        os.path.join(
            configs_dir, get_worker_config()['operator_deprecations_dir'], operator_package
        )
    )

    # assert file has right content
    operator_deprecation_file = os.path.join(
        configs_dir,
        get_worker_config()['operator_deprecations_dir'],
        operator_package,
        f'{operator_package}.json',
    )
    with open(operator_deprecation_file, 'r') as output_file:
        assert output_file.read() == deprecation_schema


@mock.patch('iib.workers.tasks.build_add_deprecations.create_dockerfile')
@mock.patch('iib.workers.tasks.build_add_deprecations.generate_cache_locally')
@mock.patch('iib.workers.tasks.build_add_deprecations.opm_validate')
@mock.patch('iib.workers.tasks.build_add_deprecations.get_catalog_dir')
@mock.patch('iib.workers.tasks.build_add_deprecations.set_request_state')
def test_add_deprecations_to_index_dep_dir_exist(
    mock_srs, mock_gcd, mock_ov, mock_gcl, mock_cd, tmpdir
):
    request_id = 11
    from_index_resolved = 'from-index@sha256:bcdefg'
    binary_image_resolved = 'binary-image@sha256:abcdef'
    operator_package = 'deprecation-operator'
    deprecation_schema = '{"schema": "olm.deprecations", "message": "deprecation-msg"}'
    index_db_path = '/tmpdir/indexdb'

    configs_dir = os.path.join(tmpdir, 'configs')
    os.makedirs(os.path.join(configs_dir, get_worker_config()['operator_deprecations_dir']))
    mock_gcd.return_value = configs_dir
    build_add_deprecations.add_deprecations_to_index(
        request_id,
        tmpdir,
        from_index_resolved,
        operator_package,
        deprecation_schema,
        binary_image_resolved,
        index_db_path,
    )
    mock_ov.assert_called_once()
    mock_gcl.assert_called_once_with(
        base_dir=tmpdir,
        fbc_dir=mock_gcd.return_value,
        local_cache_path=os.path.join(tmpdir, 'cache'),
    )
    mock_cd.assert_called_once()
    # assert file has right content
    operator_deprecation_file = os.path.join(
        configs_dir,
        get_worker_config()['operator_deprecations_dir'],
        operator_package,
        f'{operator_package}.json',
    )
    with open(operator_deprecation_file, 'r') as output_file:
        assert output_file.read() == deprecation_schema


@mock.patch('iib.workers.tasks.build_add_deprecations.create_dockerfile')
@mock.patch('iib.workers.tasks.build_add_deprecations.generate_cache_locally')
@mock.patch('iib.workers.tasks.build_add_deprecations.opm_validate')
@mock.patch('iib.workers.tasks.build_add_deprecations.get_catalog_dir')
@mock.patch('iib.workers.tasks.build_add_deprecations.set_request_state')
def test_add_deprecations_to_index_file_exist(
    mock_srs, mock_gcd, mock_ov, mock_gcl, mock_cd, tmpdir
):
    request_id = 11
    from_index_resolved = 'from-index@sha256:bcdefg'
    binary_image_resolved = 'binary-image@sha256:abcdef'
    operator_package = 'deprecation-operator'
    old_deprecation_schema = '{"schema": "olm.deprecations", "message": "old deprecation-msg"}'
    new_deprecation_schema = '{"schema": "olm.deprecations", "message": "new deprecation-msg"}'
    index_db_path = '/tmpdir/indexdb'

    configs_dir = os.path.join(tmpdir, 'configs')
    os.makedirs(
        os.path.join(
            configs_dir, get_worker_config()['operator_deprecations_dir'], operator_package
        )
    )
    operator_deprecation_file = os.path.join(
        configs_dir,
        get_worker_config()['operator_deprecations_dir'],
        operator_package,
        f'{operator_package}.json',
    )
    with open(operator_deprecation_file, 'w') as output_file:
        json.dump(json.loads(old_deprecation_schema), output_file)
    mock_gcd.return_value = configs_dir
    build_add_deprecations.add_deprecations_to_index(
        request_id,
        tmpdir,
        from_index_resolved,
        operator_package,
        new_deprecation_schema,
        binary_image_resolved,
        index_db_path,
    )
    mock_ov.assert_called_once()
    mock_gcl.assert_called_once_with(
        base_dir=tmpdir,
        fbc_dir=mock_gcd.return_value,
        local_cache_path=os.path.join(tmpdir, 'cache'),
    )
    mock_cd.assert_called_once()

    # assert file has right content
    with open(operator_deprecation_file, 'r') as output_file:
        assert output_file.read() == new_deprecation_schema
