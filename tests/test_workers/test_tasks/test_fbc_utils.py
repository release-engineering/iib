# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.workers.tasks.fbc_utils import is_image_fbc


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
                            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/busybox",
                            "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"
                        ],
                        "Entrypoint": [
                            "/bin/opm"
                        ],
                        "Cmd": [
                            "serve",
                            "/configs"
                        ],
                        "WorkingDir": "/",
                        "Labels": {
                            "io.buildah.version": "1.23.1",
                        }
                    },
                    "rootfs": {
                        "type": "layers",
                        "diff_ids": [
                            "sha256:c0d270ab7e0db0fa1db41d15b679a7b77ffbb9db62790095c7aee41444435933",
                        ]
                    },
                    "history": [
                        {
                            "created": "1970-01-01T00:00:00Z",
                            "created_by": "bazel build ...",
                            "author": "Bazel"
                        },
                    ]
                },
                False
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
                            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/busybox",
                            "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"
                        ],
                        "Entrypoint": [
                            "/bin/opm"
                        ],
                        "Cmd": [
                            "serve",
                            "/configs"
                        ],
                        "WorkingDir": "/",
                        "Labels": {
                            "io.buildah.version": "1.23.1",
                            "operators.operatorframework.io.index.configs.v1": "/configs"
                        }
                    },
                    "rootfs": {
                        "type": "layers",
                        "diff_ids": [
                            "sha256:c0d270ab7e0db0fa1db41d15b679a7b77ffbb9db62790095c7aee41444435933",
                        ]
                    },
                    "history": [
                        {
                            "created": "1970-01-01T00:00:00Z",
                            "created_by": "bazel build ...",
                            "author": "Bazel"
                        },
                    ]
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
