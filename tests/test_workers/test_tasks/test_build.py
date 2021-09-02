# SPDX-License-Identifier: GPL-3.0-or-later
import copy
import os
import re
import stat
import textwrap
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import build
from iib.workers.tasks.utils import RequestConfigAddRm


@pytest.mark.parametrize('arch', ('amd64', 'ppc64le', 's390x', 'spam'))
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_build_image(mock_run_cmd, arch):
    build._build_image('/some/dir', 'some.Dockerfile', 3, arch)

    mock_run_cmd.assert_called_once_with(
        [
            'buildah',
            'bud',
            '--no-cache',
            '--override-arch',
            arch,
            '--arch',
            arch,
            '-t',
            f'iib-build:3-{arch}',
            '-f',
            '/some/dir/some.Dockerfile',
        ],
        {'cwd': '/some/dir'},
        exc_msg=mock.ANY,
    )


@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.reset_docker_config')
def test_cleanup(mock_rdc, mock_run_cmd):
    build._cleanup()

    mock_run_cmd.assert_called_once()
    rmi_args = mock_run_cmd.call_args[0][0]
    assert rmi_args[0:2] == ['podman', 'rmi']
    mock_rdc.assert_called_once_with()


@mock.patch('iib.workers.tasks.build.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_create_and_push_manifest_list(mock_run_cmd, mock_td, tmp_path):
    mock_td.return_value.__enter__.return_value = tmp_path

    build._create_and_push_manifest_list(3, {'amd64', 's390x'})

    expected_calls = [
        mock.call(
            ['buildah', 'manifest', 'create', 'registry:8443/iib-build:3'],
            exc_msg='Failed to create the manifest list locally: registry:8443/iib-build:3',
        ),
        mock.call(
            [
                'buildah',
                'manifest',
                'add',
                'registry:8443/iib-build:3',
                'docker://registry:8443/iib-build:3-amd64',
            ],
            exc_msg=(
                'Failed to add docker://registry:8443/iib-build:3-amd64'
                ' to the local manifest list: registry:8443/iib-build:3'
            ),
        ),
        mock.call(
            [
                'buildah',
                'manifest',
                'add',
                'registry:8443/iib-build:3',
                'docker://registry:8443/iib-build:3-s390x',
            ],
            exc_msg=(
                'Failed to add docker://registry:8443/iib-build:3-s390x'
                ' to the local manifest list: registry:8443/iib-build:3'
            ),
        ),
        mock.call(
            [
                'buildah',
                'manifest',
                'push',
                '--all',
                'registry:8443/iib-build:3',
                'docker://registry:8443/iib-build:3',
            ],
            exc_msg='Failed to push the manifest list to registry:8443/iib-build:3',
        ),
    ]
    assert mock_run_cmd.call_args_list == expected_calls


@pytest.mark.parametrize(
    'iib_index_image_output_registry, from_index, overwrite, expected, resolved_from_index,'
    'add_or_rm',
    (
        (None, None, False, '{default}', None, False),
        (
            'registry-proxy.domain.local',
            None,
            False,
            'registry-proxy.domain.local/{default_no_registry}',
            None,
            False,
        ),
        (None, 'quay.io/ns/iib:v4.5', True, 'quay.io/ns/iib:v4.5', 'quay.io/ns/iib:abcdef', True),
        (None, 'quay.io/ns/iib:v5.4', True, 'quay.io/ns/iib:v5.4', 'quay.io/ns/iib:fedcba', True),
    ),
)
@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('iib.workers.tasks.build._overwrite_from_index')
@mock.patch('iib.workers.tasks.build.update_request')
@mock.patch('iib.workers.tasks.build.get_resolved_image')
@mock.patch('iib.workers.tasks.build.set_registry_token')
def test_update_index_image_pull_spec(
    mock_st_rgstr_tknm,
    mock_get_rslv_img,
    mock_ur,
    mock_ofi,
    mock_gwc,
    iib_index_image_output_registry,
    from_index,
    overwrite,
    expected,
    resolved_from_index,
    add_or_rm,
):
    default_no_registry = 'namespace/some-image:3'
    default = f'quay.io/{default_no_registry}'
    expected_pull_spec = expected.format(default=default, default_no_registry=default_no_registry)
    request_id = 2
    arches = {'amd64'}
    overwrite_token = 'username:password'

    mock_get_rslv_img.return_value = "quay.io/ns/iib@sha256:abcdef1234"
    mock_gwc.return_value = {
        'iib_index_image_output_registry': iib_index_image_output_registry,
        'iib_registry': 'quay.io',
    }

    if add_or_rm:
        build._update_index_image_pull_spec(
            default,
            request_id,
            arches,
            from_index,
            overwrite,
            overwrite_token,
            resolved_prebuild_from_index=resolved_from_index,
            add_or_rm=add_or_rm,
        )
    else:
        build._update_index_image_pull_spec(
            default,
            request_id,
            arches,
            from_index,
            overwrite,
            overwrite_token,
            resolved_prebuild_from_index=resolved_from_index,
        )

    mock_ur.assert_called_once()
    update_request_payload = mock_ur.call_args[0][1]
    if add_or_rm:
        assert update_request_payload.keys() == {'arches', 'index_image', 'index_image_resolved'}
    else:
        assert update_request_payload.keys() == {'arches', 'index_image'}
    assert update_request_payload['index_image'] == expected_pull_spec
    if overwrite:
        mock_ofi.assert_called_once_with(
            request_id, default, from_index, resolved_from_index, overwrite_token
        )
    else:
        mock_ofi.assert_not_called()


@pytest.mark.parametrize('request_id, arch', ((1, 'amd64'), (5, 's390x')))
def test_get_local_pull_spec(request_id, arch):
    rv = build._get_local_pull_spec(request_id, arch)

    assert re.match(f'.+:{request_id}-{arch}', rv)


@pytest.mark.parametrize('from_index', (None, 'some_index:latest'))
@pytest.mark.parametrize('bundles', (['bundle:1.2', 'bundle:1.3'], []))
@pytest.mark.parametrize('overwrite_csv', (True, False))
@pytest.mark.parametrize('container_tool', (None, 'podwoman'))
@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_opm_index_add(mock_run_cmd, mock_srt, from_index, bundles, overwrite_csv, container_tool):
    build._opm_index_add(
        '/tmp/somedir',
        bundles,
        'binary-image:latest',
        from_index,
        'user:pass',
        overwrite_csv,
        container_tool=container_tool,
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'add']
    if bundles:
        assert ','.join(bundles) in opm_args
    else:
        assert '""' in opm_args
    if from_index:
        assert '--from-index' in opm_args
        assert from_index in opm_args
    else:
        assert '--from-index' not in opm_args
    if overwrite_csv:
        assert '--overwrite-latest' in opm_args
    else:
        assert '--overwrite-latest' not in opm_args
    if container_tool:
        assert '--container-tool' in opm_args
        assert container_tool in opm_args
    else:
        assert '--container-tool' not in opm_args
    assert "--enable-alpha" in opm_args

    mock_srt.assert_called_once_with('user:pass', from_index)


@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_opm_index_rm(mock_run_cmd, mock_srt):
    operators = ['operator_1', 'operator_2']
    build._opm_index_rm(
        '/tmp/somedir', operators, 'binary-image:latest', 'some_index:latest', 'user:pass'
    )

    mock_run_cmd.assert_called_once()
    opm_args = mock_run_cmd.call_args[0][0]
    assert opm_args[0:3] == ['opm', 'index', 'rm']
    assert ','.join(operators) in opm_args
    assert 'some_index:latest' in opm_args
    mock_srt.assert_called_once_with('user:pass', 'some_index:latest')


@pytest.mark.parametrize(
    'output_pull_spec, from_index, resolved_from_index,'
    'overwrite_from_index_token, oci_export_expected',
    (
        (
            'quay.io/ns/repo:1',
            'quay.io/user_ns/repo:v1',
            'quay.io/user_ns/repo:abcdef',
            'user:pass',
            True,
        ),
        (
            'quay.io/ns/repo:1',
            'docker.io/user_ns/repo:v1',
            'quay.io/user_ns/repo:abcdef',
            'user:pass',
            False,
        ),
        (
            'quay.io/ns/repo:1',
            'quay.io/user_ns/repo:v1',
            'quay.io/user_ns/repo:abcdef',
            None,
            False,
        ),
    ),
)
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build.tempfile.TemporaryDirectory')
@mock.patch('iib.workers.tasks.build._skopeo_copy')
@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build._verify_index_image')
def test_overwrite_from_index(
    mock_vii,
    mock_srt,
    mock_sc,
    mock_td,
    mock_srs,
    output_pull_spec,
    from_index,
    resolved_from_index,
    overwrite_from_index_token,
    oci_export_expected,
):
    mock_td.return_value.name = '/tmp/iib-12345'
    build._overwrite_from_index(
        1, output_pull_spec, from_index, resolved_from_index, overwrite_from_index_token
    )

    if oci_export_expected:
        oci_pull_spec = f'oci:{mock_td.return_value.name}'
        mock_sc.assert_has_calls(
            (
                mock.call(
                    f'docker://{output_pull_spec}', oci_pull_spec, copy_all=True, exc_msg=mock.ANY
                ),
                mock.call(oci_pull_spec, f'docker://{from_index}', copy_all=True, exc_msg=mock.ANY),
            )
        )
        mock_td.return_value.cleanup.assert_called_once_with()
    else:
        mock_sc.assert_called_once_with(
            f'docker://{output_pull_spec}',
            f'docker://{from_index}',
            copy_all=True,
            exc_msg=mock.ANY,
        )
        mock_td.return_value.cleanup.assert_not_called()

    mock_srt.assert_called_once()
    mock_vii.assert_called_once_with(resolved_from_index, from_index, overwrite_from_index_token)


@pytest.mark.parametrize('bundle_mapping', (True, False))
@pytest.mark.parametrize('from_index_resolved', (True, False))
@mock.patch('iib.workers.tasks.build.update_request')
def test_update_index_image_build_state(mock_ur, bundle_mapping, from_index_resolved):
    prebuild_info = {
        'arches': ['amd64', 's390x'],
        'binary_image': 'binary-image:1',
        'binary_image_resolved': 'binary-image@sha256:12345',
        'extra': 'ignored',
        'distribution_scope': 'stage',
    }

    if bundle_mapping:
        prebuild_info['bundle_mapping'] = {
            'some-bundle': ['quay.io/some-bundle:v1'],
            'some-bundle2': ['quay.io/some-bundle2:v1'],
        }

    if from_index_resolved:
        prebuild_info['from_index_resolved'] = 'from-index-image@sha256:abcde'

    expected_payload = copy.deepcopy(prebuild_info)
    del expected_payload['arches']
    del expected_payload['extra']
    expected_payload['state'] = 'in_progress'
    expected_payload['state_reason'] = mock.ANY

    request_id = 1
    build._update_index_image_build_state(request_id, prebuild_info)
    mock_ur.assert_called_once_with(request_id, expected_payload, mock.ANY)


@pytest.mark.parametrize('schema_version', (1, 2))
@mock.patch('iib.workers.tasks.build._get_local_pull_spec')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.skopeo_inspect')
@mock.patch('iib.workers.tasks.build._skopeo_copy')
def test_push_image(mock_sc, mock_si, mock_run_cmd, mock_glps, schema_version):
    mock_glps.return_value = 'source:tag'
    mock_si.return_value = {'schemaVersion': schema_version}

    build._push_image(3, 'amd64')

    push_args = mock_run_cmd.mock_calls[0][1][0]
    assert push_args[0:2] == ['podman', 'push']
    assert 'source:tag' in push_args
    destination = 'docker://registry:8443/iib-build:3-amd64'
    assert destination in push_args

    if schema_version == 1:
        mock_sc.assert_called_once()
    else:
        mock_run_cmd.assert_called_once()


@pytest.mark.parametrize('copy_all', (False, True))
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_skopeo_copy(mock_run_cmd, copy_all):
    destination = 'some_destination'
    build._skopeo_copy(destination, destination, copy_all=copy_all)
    skopeo_args = mock_run_cmd.mock_calls[0][1][0]
    if copy_all:
        expected = [
            'skopeo',
            '--command-timeout',
            '300s',
            'copy',
            '--format',
            'v2s2',
            '--all',
            destination,
            destination,
        ]
    else:
        expected = [
            'skopeo',
            '--command-timeout',
            '300s',
            'copy',
            '--format',
            'v2s2',
            destination,
            destination,
        ]
    assert skopeo_args == expected
    mock_run_cmd.assert_called_once()


@mock.patch('iib.workers.tasks.build.run_cmd')
def test_skopeo_copy_fail_max_retries(mock_run_cmd):
    match_str = 'Something went wrong'
    mock_run_cmd.side_effect = IIBError(match_str)
    destination = 'some_destination'
    with pytest.raises(IIBError, match=match_str):
        build._skopeo_copy(destination, destination)
        assert mock_run_cmd.call_count == 5


@pytest.mark.parametrize(
    'force_backport, binary_image', ((True, 'binary-image:latest'), (False, None))
)
@pytest.mark.parametrize('distribution_scope', ('dev', 'stage', 'prod'))
@pytest.mark.parametrize('deprecate_bundles', (True, False))
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._serve_index_registry')
@mock.patch('iib.workers.tasks.build.deprecate_bundles')
@mock.patch('iib.workers.tasks.utils.get_resolved_bundles')
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.verify_labels')
@mock.patch('iib.workers.tasks.build.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build._opm_index_add')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build.get_resolved_bundles')
@mock.patch('iib.workers.tasks.build._add_label_to_index')
@mock.patch('iib.workers.tasks.build._get_present_bundles')
@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.is_image_dc')
def test_handle_add_request(
    mock_iidc,
    mock_srt,
    mock_gpb,
    mock_alti,
    mock_grb,
    mock_gb,
    mock_capml,
    mock_srs,
    mock_srs2,
    mock_uiips,
    mock_vii,
    mock_pi,
    mock_bi,
    mock_oia,
    mock_uiibs,
    mock_prfb,
    mock_vl,
    mock_cleanup,
    mock_ugrb,
    mock_dep_b,
    mock_sir,
    mock_run_cmd,
    force_backport,
    binary_image,
    distribution_scope,
    deprecate_bundles,
):
    arches = {'amd64', 's390x'}
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    mock_iidc.return_value = False
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image or 'some_image',
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
        'ocp_version': 'v4.5',
        'distribution_scope': distribution_scope,
    }
    mock_grb.return_value = ['some-bundle@sha', 'some-deprecation-bundle@sha']
    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec
    mock_gpb.return_value = [{'bundlePath': 'random_bundle@sha'}], ['random_bundle@sha']
    bundles = ['some-bundle:2.3-1', 'some-deprecation-bundle:1.1-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    deprecation_list = []
    if deprecate_bundles:
        mock_ugrb.return_value = ['random_bundle@sha', 'some-deprecation-bundle@sha']
        deprecation_list = ['random_bundle@sha', 'some-deprecation-bundle@sha']

    # Simulate opm's behavior of creating files that cannot be deleted
    def side_effect(_, temp_dir, *args, **kwargs):
        read_only_dir = os.path.join(temp_dir, 'read-only-dir')
        os.mkdir(read_only_dir)
        with open(os.path.join(read_only_dir, 'read-only-file'), 'w') as f:
            os.chmod(f.fileno(), stat.S_IRUSR | stat.S_IRGRP)
        # Make the dir read-only *after* populating it
        os.chmod(read_only_dir, mode=stat.S_IRUSR | stat.S_IRGRP)

    mock_dep_b.side_effect = side_effect

    port = 0
    my_mock = mock.MagicMock()
    mock_sir.return_value = (port, my_mock)
    mock_run_cmd.return_value = '{"packageName": "package1", "version": "v1.0", \
        "bundlePath": "bundle1"\n}'

    build.handle_add_request(
        bundles,
        3,
        binary_image,
        'from-index:latest',
        ['s390x'],
        cnr_token,
        organization,
        force_backport,
        False,
        None,
        None,
        greenwave_config,
        binary_image_config=binary_image_config,
        deprecation_list=deprecation_list,
    )

    mock_sir.assert_called_once()
    mock_run_cmd.assert_called_once()
    mock_run_cmd.assert_has_calls(
        [
            mock.call(
                ['grpcurl', '-plaintext', f'localhost:{port}', 'api.Registry/ListBundles'],
                exc_msg=mock.ANY,
            ),
        ]
    )

    mock_cleanup.assert_called_once()
    mock_vl.assert_called_once()
    mock_prfb.assert_called_once_with(
        3,
        RequestConfigAddRm(
            _binary_image=binary_image,
            from_index='from-index:latest',
            overwrite_from_index_token=None,
            add_arches=['s390x'],
            bundles=['some-bundle:2.3-1', 'some-deprecation-bundle:1.1-1'],
            distribution_scope=None,
            binary_image_config=binary_image_config,
        ),
    )
    mock_gb.assert_called_once()
    assert 2 == mock_alti.call_count

    mock_oia.assert_called_once()

    if distribution_scope in ['dev', 'stage']:
        assert mock_oia.call_args[0][5]
    else:
        assert not mock_oia.call_args[0][5]

    mock_srt.assert_called_once()

    if deprecate_bundles:
        # Take into account the temporarily created index image
        assert mock_bi.call_count == len(arches) + 1
    else:
        assert mock_bi.call_count == len(arches)

    assert mock_pi.call_count == len(arches)

    mock_uiips.assert_called_once()
    mock_vii.assert_not_called()
    mock_capml.assert_called_once()
    assert mock_srs.call_count == 4
    if deprecate_bundles:
        mock_dep_b.assert_called_once_with(
            ['random_bundle@sha', 'some-deprecation-bundle@sha'],
            mock.ANY,
            binary_image or 'some_image',
            'containers-storage:localhost/iib-build:3-amd64',
            None,
            container_tool='podman',
        )
    else:
        mock_dep_b.assert_not_called()


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.is_image_dc')
def test_handle_add_request_raises(mock_iidc, mock_runcmd, mock_c):
    mock_iidc.return_value = True
    with pytest.raises(IIBError):
        build.handle_add_request(
            bundles=['some-bundle:2.3-1', 'some-deprecation-bundle:1.1-1'],
            request_id=3,
            binary_image='binary-image:latest',
            from_index='from-index:latest',
            add_arches=['s390x'],
            cnr_token='token',
            organization='org',
            force_backport=True,
            overwrite_from_index=False,
            overwrite_from_index_token=None,
            distribution_scope=None,
            greenwave_config={'some_key': 'other_value'},
            binary_image_config={'prod': {'v4.5': 'some_image'}},
            deprecation_list=[],
        )


@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._serve_index_registry')
@mock.patch('iib.workers.tasks.build.deprecate_bundles')
@mock.patch('iib.workers.tasks.utils.get_resolved_bundles')
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.verify_labels')
@mock.patch('iib.workers.tasks.build.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build._opm_index_add')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build.get_resolved_bundles')
@mock.patch('iib.workers.tasks.build._add_label_to_index')
@mock.patch('iib.workers.tasks.build._get_present_bundles')
@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.is_image_dc')
def test_handle_add_request_check_index_label_behavior(
    mock_iidc,
    mock_srt,
    mock_gpb,
    mock_alti,
    mock_grb,
    mock_gb,
    mock_capml,
    mock_srs,
    mock_uiips,
    mock_vii,
    mock_pi,
    mock_bi,
    mock_oia,
    mock_uiibs,
    mock_prfb,
    mock_vl,
    mock_cleanup,
    mock_ugrb,
    mock_dep_b,
    mock_sir,
    mock_run_cmd,
):
    arches = {'amd64', 's390x'}
    binary_image_config = {'prod': {'v4.5': 'some_image'}}
    mock_iidc.return_value = False
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': 'binary-image:latest',
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
        'ocp_version': 'v4.5',
        'distribution_scope': 'stage',
    }
    mock_grb.return_value = ['some-bundle@sha', 'some-deprecation-bundle@sha']
    output_pull_spec = 'quay.io/namespace/some-image:3'
    mock_capml.return_value = output_pull_spec
    mock_gpb.return_value = [{'bundlePath': 'random_bundle@sha'}], ['random_bundle@sha']
    bundles = ['some-bundle:2.3-1', 'some-deprecation-bundle:1.1-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    mock_ugrb.return_value = ['random_bundle@sha', 'some-deprecation-bundle@sha']
    deprecation_list = ['random_bundle@sha', 'some-deprecation-bundle@sha']
    # Assume default labels are set on the index
    label_state = {'LABEL_SET': 'default_labels_set'}

    def _add_label_to_index(*args):
        # Set the labels in the index again making sure they were wiped out
        if label_state['LABEL_SET'] == 'wiping_out_labels':
            label_state['LABEL_SET'] = 'setting_label_in_add_label_to_index'

    mock_alti.side_effect = _add_label_to_index

    def deprecate_bundles_mock(*args, **kwargs):
        # Wipe out the labels on the index
        label_state['LABEL_SET'] = 'wiping_out_labels'

    mock_dep_b.side_effect = deprecate_bundles_mock

    port = 0
    my_mock = mock.MagicMock()
    mock_sir.return_value = (port, my_mock)
    mock_run_cmd.return_value = '{"packageName": "package1", "version": "v1.0", \
        "bundlePath": "bundle1"\n}'

    build.handle_add_request(
        bundles,
        3,
        'binary-image:latest',
        'from-index:latest',
        ['s390x'],
        cnr_token,
        organization,
        True,
        False,
        None,
        None,
        greenwave_config,
        binary_image_config=binary_image_config,
        deprecation_list=deprecation_list,
    )

    mock_sir.assert_called_once()
    mock_run_cmd.assert_called_once()
    mock_run_cmd.assert_has_calls(
        [
            mock.call(
                ['grpcurl', '-plaintext', f'localhost:{port}', 'api.Registry/ListBundles'],
                exc_msg=mock.ANY,
            ),
        ]
    )

    mock_cleanup.assert_called_once()
    mock_vl.assert_called_once()
    mock_prfb.assert_called_once_with(
        3,
        RequestConfigAddRm(
            _binary_image='binary-image:latest',
            from_index='from-index:latest',
            overwrite_from_index_token=None,
            add_arches=['s390x'],
            bundles=['some-bundle:2.3-1', 'some-deprecation-bundle:1.1-1'],
            distribution_scope=None,
            binary_image_config=binary_image_config,
        ),
    )
    mock_dep_b.assert_called_once_with(
        ['random_bundle@sha', 'some-deprecation-bundle@sha'],
        mock.ANY,
        'binary-image:latest',
        'containers-storage:localhost/iib-build:3-amd64',
        None,
        container_tool='podman',
    )
    # Assert the labels are set again once they were wiped out
    assert label_state['LABEL_SET'] == 'setting_label_in_add_label_to_index'
    assert mock_alti.call_count == 2


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.build.gate_bundles')
@mock.patch('iib.workers.tasks.build.verify_labels')
@mock.patch('iib.workers.tasks.build.get_resolved_bundles')
def test_handle_add_request_gating_failure(
    mock_grb, mock_vl, mock_gb, mock_srs, mock_srs2, mock_cleanup
):
    error_msg = 'Gating failure!'
    mock_gb.side_effect = IIBError(error_msg)
    mock_grb.return_value = ['some-bundle@sha']
    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    with pytest.raises(IIBError, match=error_msg):
        build.handle_add_request(
            bundles,
            'binary-image:latest',
            3,
            'from-index:latest',
            ['s390x'],
            cnr_token,
            organization,
            None,
            False,
            None,
            None,
            greenwave_config,
        )
    mock_cleanup.assert_called_once_with()
    mock_srs2.assert_called_once()
    mock_vl.assert_called_once()
    mock_gb.assert_called_once_with(['some-bundle@sha'], greenwave_config)


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build.get_resolved_bundles')
def test_handle_add_request_bundle_resolution_failure(mock_grb, mock_srs, mock_cleanup):
    error_msg = 'Bundle Resolution failure!'
    mock_grb.side_effect = IIBError(error_msg)
    bundles = ['some-bundle:2.3-1']
    cnr_token = 'token'
    organization = 'org'
    greenwave_config = {'some_key': 'other_value'}
    with pytest.raises(IIBError, match=error_msg):
        build.handle_add_request(
            bundles,
            'binary-image:latest',
            3,
            'from-index:latest',
            ['s390x'],
            cnr_token,
            organization,
            False,
            None,
            greenwave_config,
        )
    mock_cleanup.assert_called_once_with()
    mock_srs.assert_called_once()
    mock_grb.assert_called_once_with(bundles)


@pytest.mark.parametrize('binary_image', ('binary-image:latest', None))
@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.prepare_request_for_build')
@mock.patch('iib.workers.tasks.build._update_index_image_build_state')
@mock.patch('iib.workers.tasks.build._opm_index_rm')
@mock.patch('iib.workers.tasks.build._build_image')
@mock.patch('iib.workers.tasks.build._push_image')
@mock.patch('iib.workers.tasks.build._verify_index_image')
@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.build._create_and_push_manifest_list')
@mock.patch('iib.workers.tasks.build._update_index_image_pull_spec')
@mock.patch('iib.workers.tasks.build._add_label_to_index')
@mock.patch('iib.workers.tasks.build.is_image_dc')
def test_handle_rm_request(
    mock_iidc,
    mock_alti,
    mock_uiips,
    mock_capml,
    mock_srs,
    mock_srs2,
    mock_vii,
    mock_pi,
    mock_bi,
    mock_uiibs,
    mock_oir,
    mock_prfb,
    mock_cleanup,
    binary_image,
):
    arches = {'amd64', 's390x'}
    mock_iidc.return_value = False
    mock_prfb.return_value = {
        'arches': arches,
        'binary_image': binary_image,
        'binary_image_resolved': 'binary-image@sha256:abcdef',
        'from_index_resolved': 'from-index@sha256:bcdefg',
        'ocp_version': 'v4.6',
        'distribution_scope': 'PROD',
    }
    binary_image_config = {'prod': {'v4.6': 'some_image'}}
    build.handle_rm_request(
        ['some-operator'],
        3,
        'from-index:latest',
        binary_image,
        binary_image_config=binary_image_config,
    )

    mock_cleanup.assert_called_once()
    mock_prfb.assert_called_once_with(
        3,
        RequestConfigAddRm(
            _binary_image=binary_image,
            from_index='from-index:latest',
            overwrite_from_index_token=None,
            add_arches=None,
            binary_image_config=binary_image_config,
            distribution_scope=None,
        ),
    )
    mock_oir.assert_called_once()
    assert mock_alti.call_count == 2
    assert mock_bi.call_count == len(arches)
    assert mock_pi.call_count == len(arches)
    mock_vii.assert_not_called()
    assert mock_srs.call_count == 2
    mock_capml.assert_called_once()
    mock_uiips.assert_called_once()
    assert mock_srs.call_args[0][1] == 'complete'


@mock.patch('iib.workers.tasks.build._cleanup')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.is_image_dc')
def test_handle_rm_request_raises(mock_iidc, mock_runcmd, mock_c):
    mock_iidc.return_value = True
    with pytest.raises(IIBError):
        build.handle_rm_request(
            operators=['some-operator'],
            request_id=3,
            from_index='from-index:latest',
            binary_image='binary-image:latest',
            binary_image_config={'prod': {'v4.6': 'some_image'}},
        )


@mock.patch('iib.workers.tasks.build.set_registry_token')
@mock.patch('iib.workers.tasks.build.get_resolved_image')
def test_verify_index_image_failure(mock_gri, mock_srt):
    mock_gri.return_value = 'image:works'
    match_str = (
        'The supplied from_index image changed during the IIB request.'
        ' Please resubmit the request.'
    )
    with pytest.raises(IIBError, match=match_str):
        build._verify_index_image('image:doesnt_work', 'unresolved_image', 'user:pass')

    mock_srt.assert_called_once_with('user:pass', 'unresolved_image')


@pytest.mark.parametrize('fail_rm', (True, False))
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build.podman_pull')
def test_copy_files_from_image(mock_podman_pull, mock_run_cmd, fail_rm):
    image = 'bundle-image:latest'
    src_path = '/manifests'
    dest_path = '/destination/path/manifests'

    container_id = 'df2ff736efeaff598330a128b3dc4875caf254d9f416cefd86ec009b74d1488b'

    side_effect = [f'{container_id}\n', '']
    if fail_rm:
        side_effect.append(IIBError('Uh oh! Something went wrong.'))
    else:
        side_effect.append('')
    mock_run_cmd.side_effect = side_effect

    build._copy_files_from_image(image, src_path, dest_path)
    mock_podman_pull.assert_called_once()

    mock_run_cmd.assert_has_calls(
        [
            mock.call(['podman', 'create', image, 'unused'], exc_msg=mock.ANY),
            mock.call(['podman', 'cp', f'{container_id}:{src_path}', dest_path], exc_msg=mock.ANY),
            mock.call(['podman', 'rm', container_id], exc_msg=mock.ANY),
        ]
    )


def test_add_label_to_index(tmpdir):
    operator_dir = tmpdir.mkdir('operator')
    dockerfile_txt = textwrap.dedent(
        '''\
        FROM scratch

        COPY manifests /manifests/
        COPY metadata/annotations.yaml /metadata/annotations.yaml

        LABEL operators.operatorframework.io.bundle.mediatype.v1=registry+v1
        LABEL operators.operatorframework.io.bundle.manifests.v1=manifests/
        LABEL operators.operatorframework.io.bundle.metadata.v1=metadata/
        LABEL operators.operatorframework.io.bundle.package.v1=my-operator
        LABEL operators.operatorframework.io.bundle.channels.v1=release-v1.1
        LABEL operators.operatorframework.io.bundle.channel.default.v1=release-v1.1

        # This last block are standard Red Hat container labels
        LABEL \
        com.redhat.component="my-operator-bundle-container" \
        version="v1.1" \
        name="my-operator-bundle" \
        License="ASL 2.0" \
        io.k8s.display-name="my-operator bundle" \
        io.k8s.description="demo of bundle" \
        summary="demo of bundle" \
        maintainer="John Doe <jdoe@redhat.com>"
        '''
    )
    operator_dir.join('Dockerfile').write(dockerfile_txt)

    build._add_label_to_index(
        'com.redhat.index.delivery.version', 'v4.5', operator_dir, 'Dockerfile'
    )

    expected = dockerfile_txt + '\nLABEL com.redhat.index.delivery.version="v4.5"\n'
    assert operator_dir.join('Dockerfile').read_text('utf-8') == expected


def test_get_missing_bundles_no_match():
    assert build._get_missing_bundles(
        [
            {
                'packageName': 'bundle1',
                'version': 'v1.0',
                'bundlePath': 'quay.io/pkg/pkg1@sha256:987654',
            },
            {
                'packageName': 'bundle2',
                'version': 'v2.0',
                'bundlePath': 'quay.io/pkg/pkg2@sha256:111111',
            },
        ],
        ['quay.io/ns/repo@sha256:123456'],
    ) == ['quay.io/ns/repo@sha256:123456']


def test_get_missing_bundles_match_hash():
    assert (
        build._get_missing_bundles(
            [
                {
                    'packageName': 'bundle1',
                    'version': 'v1.0',
                    'bundlePath': 'quay.io/pkg/pkg1@sha256:987654',
                },
                {
                    'packageName': 'bundle2',
                    'version': 'v2.0',
                    'bundlePath': 'quay.io/pkg/pkg2@sha256:111111',
                },
            ],
            ['quay.io/pkg/pkg1@sha256:987654'],
        )
        == []
    )


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_present_bundles(mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, tmpdir):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_gil.return_value = 'some-path'
    mock_run_cmd.side_effect = [
        'api.Registry.ListBundles',
        '{"packageName": "package1", "version": "v1.0", "bundlePath":"bundle1"\n}'
        '\n{\n"packageName": "package2", "version": "v2.0", "bundlePath":"bundle2"}'
        '\n{\n"packageName": "package2", "version": "v2.0", "bundlePath":"bundle2"}',
    ]
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.return_value = 'address already in use'
    my_mock.poll.side_effect = [1, None]
    bundles, bundles_pull_spec = build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir))
    assert bundles == [
        {'packageName': 'package1', 'version': 'v1.0', 'bundlePath': 'bundle1'},
        {'packageName': 'package2', 'version': 'v2.0', 'bundlePath': 'bundle2'},
    ]
    assert bundles_pull_spec == ['bundle1', 'bundle2']
    assert mock_run_cmd.call_count == 2


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_no_present_bundles(mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, tmpdir):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_gil.return_value = 'some-path'
    mock_run_cmd.side_effect = ['api.Registry.ListBundles', '']
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.return_value = 'address already in use'
    my_mock.poll.side_effect = [1, None]
    bundle, bundle_pull_spec = build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir))
    assert bundle == []
    assert bundle_pull_spec == []
    assert mock_run_cmd.call_count == 2


@mock.patch('time.time')
@mock.patch('os.remove')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_present_bundles_grpc_not_initialize(
    mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, mock_remove, mock_time, tmpdir
):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_run_cmd.side_effect = ['', '', '', '', ''] * 4
    mock_time.side_effect = list(range(1, 80))
    mock_gil.return_value = 'some-path'
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    with pytest.raises(IIBError, match='Index registry has not been initialized after 5 tries'):
        build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir))
    assert mock_run_cmd.call_count == 20


@mock.patch('time.time')
@mock.patch('os.remove')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
@mock.patch('iib.workers.tasks.build.get_image_label')
def test_get_present_bundles_grpc_delayed_initialize(
    mock_gil, mock_copy, mock_run_cmd, mock_popen, mock_sleep, mock_remove, mock_time, tmpdir
):
    with open(tmpdir.join('cidfile.txt'), 'w+') as f:
        f.write('container_id')
    mock_time.side_effect = [i * 0.5 for i in range(1, 80)]
    mock_gil.return_value = 'some-path'
    mock_run_cmd.side_effect = [
        '',
        '',
        '',
        '',
        '',
        '',
        'api.Registry.ListBundles',
        '{"packageName": "package1", "version": "v1.0", "bundlePath": "bundle1"\n}'
        '\n{\n"packageName": "package2", "version": "v2.0", "bundlePath": "bundle2"}'
        '\n{\n"packageName": "package2", "version": "v2.0", "bundlePath": "bundle2"}',
    ]
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.poll.return_value = None
    bundles, bundles_pull_spec = build._get_present_bundles('quay.io/index-image:4.5', str(tmpdir))
    assert bundles == [
        {'packageName': 'package1', 'version': 'v1.0', 'bundlePath': 'bundle1'},
        {'packageName': 'package2', 'version': 'v2.0', 'bundlePath': 'bundle2'},
    ]
    assert bundles_pull_spec == ['bundle1', 'bundle2']
    assert mock_run_cmd.call_count == 8


@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
@mock.patch('iib.workers.tasks.build.run_cmd')
def test_serve_image_registry(mock_run_cmd, mock_popen, mock_sleep, tmpdir):
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.side_effect = ['address already in use', 'address already in use']
    mock_run_cmd.return_value = 'api.Registry.ListBundles'
    my_mock.poll.side_effect = [1, 1, None]
    port, _ = build._serve_index_registry('some_path.db')
    assert port == 50053
    assert my_mock.poll.call_count == 3


@mock.patch('iib.workers.tasks.build.get_worker_config')
@mock.patch('time.sleep')
@mock.patch('subprocess.Popen')
def test_serve_image_registry_no_ports(mock_popen, mock_sleep, mock_config, tmpdir):
    my_mock = mock.MagicMock()
    mock_popen.return_value = my_mock
    my_mock.stderr.read.side_effect = [
        'address already in use',
        'address already in use',
        'address already in use',
    ]
    my_mock.poll.side_effect = [1, 1, 1, None]
    mock_config.return_value = {
        'iib_grpc_start_port': 50051,
        'iib_grpc_init_wait_time': 1,
        'iib_grpc_max_port_tries': 3,
        'iib_grpc_max_tries': 3,
    }
    with pytest.raises(IIBError, match='No free port has been found after 3 attempts.'):
        build._serve_index_registry('some_path.db')
    assert my_mock.poll.call_count == 3
