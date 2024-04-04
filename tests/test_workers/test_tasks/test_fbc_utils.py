# SPDX-License-Identifier: GPL-3.0-or-later
import json
import os
import tempfile
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


@mock.patch("iib.workers.tasks.fbc_utils.enforce_json_config_dir")
def test_merge_catalogs_dirs(mock_enforce_json, tmpdir):
    source_dir = os.path.join(tmpdir, 'src')
    destination_dir = os.path.join(tmpdir, 'dst')
    os.makedirs(destination_dir, exist_ok=True)
    operator_dir = os.path.join(source_dir, 'operator')
    os.makedirs(operator_dir, exist_ok=True)

    # create few temp files in operator directory
    for _ in range(3):
        tempfile.NamedTemporaryFile(dir=operator_dir, delete=False)

    merge_catalogs_dirs(src_config=source_dir, dest_config=destination_dir)
    mock_enforce_json.assert_has_calls(
        [
            mock.call(source_dir),
            mock.call(destination_dir),
        ]
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

    mock_cpt.not_called()


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


@pytest.mark.parametrize('ldr_output', [['testoperator'], ['test1', 'test2'], []])
@mock.patch('os.listdir')
@mock.patch('iib.workers.tasks.build._copy_files_from_image')
def test_extract_fbc_fragment(mock_cffi, mock_osldr, ldr_output, tmpdir):
    test_fbc_fragment = "example.com/test/fbc_fragment:latest"
    mock_osldr.return_value = ldr_output
    fbc_fragment_path = os.path.join(tmpdir, get_worker_config()['temp_fbc_fragment_path'])

    if not ldr_output:
        with pytest.raises(IIBError):
            extract_fbc_fragment(tmpdir, test_fbc_fragment)
    else:
        extract_fbc_fragment(tmpdir, test_fbc_fragment)
    mock_cffi.assert_has_calls([mock.call(test_fbc_fragment, '/configs', fbc_fragment_path)])
    mock_osldr.assert_has_calls([mock.call(fbc_fragment_path)])
