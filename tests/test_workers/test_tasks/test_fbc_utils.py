# SPDX-License-Identifier: GPL-3.0-or-later
import datetime
import json
import os
import tempfile
from textwrap import dedent
from unittest import mock

import pytest
import ruamel.yaml

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks.fbc_utils import (
    is_image_fbc,
    merge_catalogs_dirs,
    enforce_json_config_dir,
    extract_fbc_fragment,
    _serialize_datetime,
)


yaml = ruamel.yaml.YAML()


@pytest.mark.parametrize(
    "skopeo_output,is_fbc",
    [
        (
            {
                "created": "2021-11-10T13:56:39.522635487Z",
                "author": "Bazel",
                "architecture": "amd64",
                "os": "linux",
                "config": {
                    "User": "0",
                    "Env": [
                        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                        "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
                    ],
                    "Entrypoint": ["/bin/opm"],
                    "Cmd": ["serve", "/configs"],
                    "WorkingDir": "/",
                    "Labels": {"io.buildah.version": "1.23.1"},
                },
                "rootfs": {
                    "type": "layers",
                    "diff_ids": [
                        "sha256:c0d270ab7e0db0fa1db41d15b679a7b77ffbb9db62790095c7aee41444435933",
                    ],
                },
                "history": [
                    {
                        "created": "1970-01-01T00:00:00Z",
                        "created_by": "bazel build ...",
                        "author": "Bazel",
                    },
                ],
            },
            False,
        ),
        (
            {
                "created": "2021-11-10T13:56:39.522635487Z",
                "author": "Bazel",
                "architecture": "amd64",
                "os": "linux",
                "config": {
                    "User": "0",
                    "Env": [
                        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                        "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
                    ],
                    "Entrypoint": ["/bin/opm"],
                    "Cmd": ["serve", "/configs"],
                    "WorkingDir": "/",
                    "Labels": {
                        "io.buildah.version": "1.23.1",
                        "operators.operatorframework.io.index.configs.v1": "/configs",
                    },
                },
                "rootfs": {
                    "type": "layers",
                    "diff_ids": [
                        "sha256:c0d270ab7e0db0fa1db41d15b679a7b77ffbb9db62790095c7aee41444435933",
                    ],
                },
                "history": [
                    {
                        "created": "1970-01-01T00:00:00Z",
                        "created_by": "bazel build ...",
                        "author": "Bazel",
                    },
                ],
            },
            True,
        ),
    ],
)
@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_is_image_fbc(mock_si, skopeo_output, is_fbc):
    image = 'some-image:latest'

    mock_si.return_value = skopeo_output
    assert is_image_fbc(image) is is_fbc


@mock.patch('iib.workers.tasks.opm_operations.Opm')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch("iib.workers.tasks.fbc_utils.enforce_json_config_dir")
def test_merge_catalogs_dirs(mock_enforce_json, mock_rc, mock_opm, tmpdir):
    source_dir = os.path.join(tmpdir, 'src')
    destination_dir = os.path.join(tmpdir, 'dst')
    os.makedirs(destination_dir, exist_ok=True)
    operator_dir = os.path.join(source_dir, 'operator')
    os.makedirs(operator_dir, exist_ok=True)

    # create few temp files in operator directory
    for _ in range(3):
        tempfile.NamedTemporaryFile(dir=operator_dir, delete=False)

    merge_catalogs_dirs(src_config=source_dir, dest_config=destination_dir)
    mock_enforce_json.assert_called_once_with(destination_dir)
    mock_rc.assert_called_once_with(
        [mock_opm.opm_version, 'validate', destination_dir],
        exc_msg=f'Failed to validate the content from config_dir {destination_dir}',
    )

    for r, d, f in os.walk(source_dir):

        root_dir = str(r).replace(f'{source_dir}/', '')
        for dd in d:
            # path to source directory
            sdir = os.path.join(source_dir, root_dir, dd)
            # path to destination directory
            ddir = os.path.join(destination_dir, root_dir, dd)

            assert os.path.isdir(ddir)
            # check if source and destination permissions are the same
            assert os.stat(ddir).st_mode == os.stat(sdir).st_mode
        for df in f:
            # path to source file
            dfile = os.path.join(destination_dir, root_dir, df)
            # path to destination file
            sfile = os.path.join(source_dir, root_dir, df)

            assert os.path.isfile(dfile)
            # check if source and destination permissions are the same
            assert os.stat(dfile).st_mode == os.stat(sfile).st_mode


@mock.patch('shutil.copytree')
@mock.patch('os.path.isdir')
def test_merge_catalogs_dirs_raise(mock_isdir, mock_cpt, tmpdir):
    mock_isdir.return_value = False
    source_dir = os.path.join(tmpdir, 'src')
    destination_dir = os.path.join(tmpdir, 'dst')

    with pytest.raises(IIBError, match=f"config directory does not exist: {source_dir}"):
        merge_catalogs_dirs(src_config=source_dir, dest_config=destination_dir)

    mock_cpt.assert_not_called()


def test_enforce_json_config_dir(tmpdir):
    file_prefix = "test_file"
    data = {"foo": "bar"}
    test_file = os.path.join(tmpdir, f"{file_prefix}.yaml")
    expected_file = os.path.join(tmpdir, f"{file_prefix}.json")
    with open(test_file, 'w') as w:
        yaml.dump(data, w)

    enforce_json_config_dir(tmpdir)

    assert os.path.isfile(expected_file)
    assert not os.path.isfile(test_file)

    with open(expected_file, 'r') as f:
        assert json.load(f) == data


def test_enforce_json_config_dir_multiple_chunks_input(tmpdir):
    multiple_chunks_yaml = """\
    ---
    foo: bar
    bar: foo
    ---
    another: data
    ---
    one_more: chunk
    createdAt: 2025-01-21T07:15:29
    """

    expected_result = (
        '{"foo": "bar", "bar": "foo"}{"another": "data"}'
        '{"one_more": "chunk", "createdAt": "2025-01-21T07:15:29"}'
    )

    input = os.path.join(tmpdir, "test_file.yaml")
    output = os.path.join(tmpdir, "test_file.json")
    with open(input, 'w') as w:
        w.write(dedent(multiple_chunks_yaml))

    enforce_json_config_dir(tmpdir)

    with open(output, 'r') as f:
        assert f.read() == expected_result


@pytest.mark.parametrize('ldr_output', [['testoperator'], ['test1', 'test2'], []])
@mock.patch('os.listdir')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
def test_extract_fbc_fragment(mock_cffi, mock_osldr, ldr_output, tmpdir):
    test_fbc_fragment = "example.com/test/fbc_fragment:latest"
    mock_osldr.return_value = ldr_output
    # The function now adds -0 suffix by default when fragment_index is not provided
    fbc_fragment_path = os.path.join(tmpdir, f"{get_worker_config()['temp_fbc_fragment_path']}-0")

    if not ldr_output:
        with pytest.raises(IIBError):
            extract_fbc_fragment(tmpdir, test_fbc_fragment)
    else:
        extract_fbc_fragment(tmpdir, test_fbc_fragment)
        mock_cffi.assert_called_once_with(
            test_fbc_fragment, get_worker_config()['fbc_fragment_catalog_path'], fbc_fragment_path
        )
        mock_osldr.assert_called_once_with(fbc_fragment_path)


@pytest.mark.parametrize('ldr_output', [['testoperator'], ['test1', 'test2']])
@mock.patch('os.listdir')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
def test_extract_fbc_fragment_with_index(mock_cffi, mock_osldr, ldr_output, tmpdir):
    """Test extract_fbc_fragment with non-zero fragment_index values."""
    test_fbc_fragment = "example.com/test/fbc_fragment:latest"
    mock_osldr.return_value = ldr_output

    # Test with fragment_index = 2
    fragment_index = 2
    fbc_fragment_path = os.path.join(
        tmpdir, f"{get_worker_config()['temp_fbc_fragment_path']}-{fragment_index}"
    )

    result_path, result_operators = extract_fbc_fragment(
        tmpdir, test_fbc_fragment, fragment_index=fragment_index
    )

    # Verify the path includes the correct index
    assert result_path == fbc_fragment_path
    assert result_path.endswith(f"-{fragment_index}")
    assert result_operators == ldr_output

    # Verify the function was called with the correct path
    mock_cffi.assert_called_once_with(
        test_fbc_fragment, get_worker_config()['fbc_fragment_catalog_path'], fbc_fragment_path
    )
    mock_osldr.assert_called_once_with(fbc_fragment_path)


@mock.patch('os.listdir')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
def test_extract_fbc_fragment_isolation(mock_cffi, mock_osldr, tmpdir):
    """Test that multiple fragments with different indices create isolated directories."""
    test_fbc_fragment1 = "example.com/test/fbc_fragment1:latest"
    test_fbc_fragment2 = "example.com/test/fbc_fragment2:latest"

    # Mock different outputs for each fragment
    mock_osldr.side_effect = [['operator1'], ['operator2']]

    # Extract first fragment with index 0
    path1, operators1 = extract_fbc_fragment(tmpdir, test_fbc_fragment1, fragment_index=0)

    # Extract second fragment with index 1
    path2, operators2 = extract_fbc_fragment(tmpdir, test_fbc_fragment2, fragment_index=1)

    # Verify paths are different and include correct indices
    assert path1 != path2
    assert path1.endswith("-0")
    assert path2.endswith("-1")

    # Verify operators are different (no cross-contamination)
    assert operators1 == ['operator1']
    assert operators2 == ['operator2']
    assert operators1 != operators2

    # Verify _copy_files_from_image was called with different paths
    expected_calls = [
        mock.call(test_fbc_fragment1, get_worker_config()['fbc_fragment_catalog_path'], path1),
        mock.call(test_fbc_fragment2, get_worker_config()['fbc_fragment_catalog_path'], path2),
    ]
    mock_cffi.assert_has_calls(expected_calls, any_order=True)


def test__serialize_datetime():
    assert (
        _serialize_datetime(datetime.datetime.fromisoformat("2025-01-22")) == "2025-01-22T00:00:00"
    )


def test__serialize_datetime_raise():
    with pytest.raises(TypeError, match="Type <class 'int'> is not serializable."):
        _serialize_datetime(2025)
