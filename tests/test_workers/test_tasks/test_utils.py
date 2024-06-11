# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
import stat
import textwrap
from unittest import mock

import pytest

from iib.exceptions import ExternalServiceError, IIBError
from iib.workers.config import get_worker_config
from iib.workers.tasks import utils


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_labels(mock_si):
    skopeo_rv = {'config': {'Labels': {'some_label': 'value'}}}
    mock_si.return_value = skopeo_rv
    assert utils.get_image_labels('some-image:latest') == skopeo_rv['config']['Labels']


@pytest.mark.parametrize('config_exists', (True, False))
@pytest.mark.parametrize('template_exists', (True, False))
@mock.patch('os.path.expanduser')
@mock.patch('os.remove')
@mock.patch('os.path.exists')
@mock.patch('os.symlink')
def test_reset_docker_config(
    mock_symlink, mock_exists, mock_remove, mock_expanduser, config_exists, template_exists
):
    mock_expanduser.return_value = '/home/iib-worker'
    if not config_exists:
        mock_remove.side_effect = FileNotFoundError()
    mock_exists.return_value = template_exists

    utils.reset_docker_config()

    mock_remove.assert_called_once_with('/home/iib-worker/.docker/config.json')
    if template_exists:
        mock_symlink.assert_called_once_with(
            '/home/iib-worker/.docker/config.json.template', '/home/iib-worker/.docker/config.json'
        )
    else:
        mock_symlink.assert_not_called()


@pytest.mark.parametrize('config_exists', (True, False))
@pytest.mark.parametrize('template_exists', (True, False))
@mock.patch('os.path.expanduser')
@mock.patch('os.remove')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.utils.open')
@mock.patch('iib.workers.tasks.utils.json.dump')
@mock.patch('iib.workers.tasks.utils.reset_docker_config')
def test_set_registry_token(
    mock_rdc,
    mock_json_dump,
    mock_open,
    mock_exists,
    mock_remove,
    mock_expanduser,
    config_exists,
    template_exists,
):
    mock_expanduser.return_value = '/home/iib-worker'
    if not config_exists:
        mock_remove.side_effect = FileNotFoundError()
    mock_exists.return_value = template_exists
    mock_open.side_effect = mock.mock_open(
        read_data=(
            r'{"auths": {"quay.io": {"auth": "IkhlbGxvIE9wZXJhdG9yLCBnaXZlIG1lIHRoZSBudW1iZXIg'
            r'Zm9yIDkxMSEiIC0gSG9tZXIgSi4gU2ltcHNvbgo="}}}'
        )
    )

    with utils.set_registry_token('user:pass', 'registry.redhat.io/ns/repo:latest'):
        pass

    mock_remove.assert_called_once_with('/home/iib-worker/.docker/config.json')
    if template_exists:
        mock_open.assert_has_calls(
            (
                mock.call('/home/iib-worker/.docker/config.json.template', 'r'),
                mock.call('/home/iib-worker/.docker/config.json', 'w'),
            )
        )
        assert mock_open.call_count == 2
        assert mock_json_dump.call_args[0][0] == {
            'auths': {
                'quay.io': {
                    'auth': (
                        'IkhlbGxvIE9wZXJhdG9yLCBnaXZlIG1lIHRoZSBudW1iZXIgZm9yIDkxMSEiIC0gSG9tZXIgSi'
                        '4gU2ltcHNvbgo='
                    )
                },
                'registry.redhat.io': {'auth': 'dXNlcjpwYXNz'},
            }
        }
    else:
        mock_open.assert_called_once_with('/home/iib-worker/.docker/config.json', 'w')
        assert mock_open.call_count == 1
        assert mock_json_dump.call_args[0][0] == {
            'auths': {'registry.redhat.io': {'auth': 'dXNlcjpwYXNz'}}
        }

    mock_rdc.assert_called_once_with()


@pytest.mark.parametrize('config_exists', (True, False))
@pytest.mark.parametrize('template_exists', (True, False))
@mock.patch('os.path.expanduser')
@mock.patch('os.remove')
@mock.patch('os.path.exists')
@mock.patch('iib.workers.tasks.utils.open')
@mock.patch('iib.workers.tasks.utils.json.dump')
@mock.patch('iib.workers.tasks.utils.reset_docker_config')
def test_set_registry_auths(
    mock_rdc,
    mock_json_dump,
    mock_open,
    mock_exists,
    mock_remove,
    mock_expanduser,
    config_exists,
    template_exists,
):
    mock_expanduser.return_value = '/home/iib-worker'
    if not config_exists:
        mock_remove.side_effect = FileNotFoundError()
    mock_exists.return_value = template_exists
    mock_open.side_effect = mock.mock_open(
        read_data=(
            r'{"auths": {"quay.io": {"auth": "IkhlbGxvIE9wZXJhdG9yLCBnaXZlIG1lIHRoZSBudW1iZXIg'
            r'Zm9yIDkxMSEiIC0gSG9tZXIgSi4gU2ltcHNvbgo="}, "quay.overwrite.io": '
            r'{"auth": "foo_bar"}}}'
        )
    )

    registry_auths = {
        'auths': {
            'registry.redhat.io': {'auth': 'YOLO'},
            'registry.redhat.stage.io': {'auth': 'YOLO_FOO'},
            'quay.overwrite.io': {'auth': 'YOLO_QUAY'},
        }
    }
    with utils.set_registry_auths(registry_auths):
        pass

    mock_remove.assert_called_once_with('/home/iib-worker/.docker/config.json')
    if template_exists:
        mock_open.assert_has_calls(
            (
                mock.call('/home/iib-worker/.docker/config.json.template', 'r'),
                mock.call('/home/iib-worker/.docker/config.json', 'w'),
            )
        )
        assert mock_open.call_count == 2
        assert mock_json_dump.call_args[0][0] == {
            'auths': {
                'quay.io': {
                    'auth': (
                        'IkhlbGxvIE9wZXJhdG9yLCBnaXZlIG1lIHRoZSBudW1iZXIgZm9yIDkxMSEiIC0gSG9tZXIgSi'
                        '4gU2ltcHNvbgo='
                    )
                },
                'quay.overwrite.io': {'auth': 'YOLO_QUAY'},
                'registry.redhat.io': {'auth': 'YOLO'},
                'registry.redhat.stage.io': {'auth': 'YOLO_FOO'},
            }
        }
    else:
        mock_open.assert_called_once_with('/home/iib-worker/.docker/config.json', 'w')
        assert mock_open.call_count == 1
        assert mock_json_dump.call_args[0][0] == registry_auths

    mock_rdc.assert_called_once_with()


@mock.patch('os.remove')
def test_set_registry_token_null_token(mock_remove):
    with utils.set_registry_token(None, 'quay.io/ns/repo:latest'):
        pass

    mock_remove.assert_not_called()


@mock.patch('os.remove')
def test_set_container_image_null(mock_remove):
    with utils.set_registry_token('token_username:token_pass', None):
        pass

    mock_remove.assert_not_called()


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 0
    mock_sub_run.return_value = mock_rv

    utils.run_cmd(['echo', 'hello world'], {'cwd': '/some/path'})

    mock_sub_run.assert_called_once()


@pytest.mark.parametrize('exc_msg', (None, 'Houston, we have a problem!'))
@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed(mock_sub_run, exc_msg):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = 'some failure'
    mock_sub_run.return_value = mock_rv

    expected_exc = exc_msg or 'An unexpected error occurred'
    with pytest.raises(IIBError, match=expected_exc):
        utils.run_cmd(['echo', 'hello'], exc_msg=exc_msg)

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed_opm(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = textwrap.dedent(
        '''
        time="2020-05-12T15:42:19Z" level=info msg="loading bundle file" dir=bundle_tmp775962984/manifests file=serverstatusrequest.crd.yaml load=bundle
        time="2020-05-12T15:42:19Z" level=info msg="loading bundle file" dir=bundle_tmp775962984/manifests file=volumesnapshotlocation.crd.yaml load=bundle
        time="2020-05-12T15:42:19Z" level=error msg="permissive mode disabled" bundles="[registry/namespace/bundle:v1.0-14]" error="error loading bundle from image: Error adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement that couldn't be found"
        Error: error loading bundle from image: Error adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement that couldn't be found
        Usage:
          opm index add [flags]

        Examples:
          # Create an index image from scratch with a single bundle image
          opm index add --bundles quay.io/operator-framework/operator-bundle-prometheus@sha256:a3ee653ffa8a0d2bbb2fabb150a94da6e878b6e9eb07defd40dc884effde11a0 --tag quay.io/operator-framework/monitoring:1.0.0

          # Add a single bundle image to an index image
          opm index add --bundles quay.io/operator-framework/operator-bundle-prometheus:0.15.0 --from-index quay.io/operator-framework/monitoring:1.0.0 --tag quay.io/operator-framework/monitoring:1.0.1

          # Add multiple bundles to an index and generate a Dockerfile instead of an image
          opm index add --bundles quay.io/operator-framework/operator-bundle-prometheus:0.15.0,quay.io/operator-framework/operator-bundle-prometheus:0.22.2 --generate

        Flags:
          -i, --binary-image opm        container image for on-image opm command
          -b, --bundles strings         comma separated list of bundles to add
          -c, --container-tool string   tool to interact with container images (save, build, etc.). One of: [docker, podman] (default "podman")
          -f, --from-index string       previous index to add to
              --generate                if enabled, just creates the dockerfile and saves it to local disk
          -h, --help                    help for add
              --mode string             graph update mode that defines how channel graphs are updated. One of: [replaces, semver, semver-skippatch] (default "replaces")
          -d, --out-dockerfile string   if generating the dockerfile, this flag is used to (optionally) specify a dockerfile name
              --permissive              allow registry load errors
              --skip-tls                skip TLS certificate verification for container image registries while pulling bundles
          -t, --tag string              custom tag for container image being built
        '''  # noqa: E501
    )
    mock_sub_run.return_value = mock_rv

    expected_exc = (
        'Failed to add the bundles to the index image: error loading bundle from image: Error '
        'adding package error loading bundle into db: cam-operator.v1.0.1 specifies replacement '
        'that couldn\'t be found'
    )
    with pytest.raises(IIBError, match=expected_exc):
        utils.run_cmd(
            ['opm', 'index', 'add', '--generate', '--bundles', 'quay.io/ns/some_bundle:v1.0'],
            exc_msg='Failed to add the bundles to the index image',
        )

    mock_sub_run.assert_called_once()


@pytest.mark.parametrize(
    'expected_exc, subprocess_stderr',
    [
        (
            r'Failed build the index image:.*error creating build container: 503 \(Service Unavailable\)',  # noqa: E501
            textwrap.dedent(
                '''
                2021-12-14 08:52:39,144 iib.workers.tasks.utils DEBUG utils.run_cmd Running the command "buildah bud --no-cache --override-arch s390x --arch s390x -t iib-build:56056-s390x -f /tmp/iib-ozo81z6o/index.Dockerfile"
                2021-12-14 08:55:10,212 iib.workers.tasks.utils ERROR utils.run_cmd The command "buildah bud --no-cache --override-arch s390x --arch s390x -t iib-build:56056-s390x -f /tmp/iib-ozo81z6o/index.Dockerfile" failed with: Trying to pull registry.redhat.io/openshift4/ose-operator-registry@sha256:72498731bbea4307178f9d0d237bf2a8439bfa8f580f87c35e5a73cb1c854bd6...
                Copying blob sha256:27cb39a08c6eb46426e92622c4edea9b9b8495b2401d02c773e239dd40d99a22
                error creating build container: reading blob sha256:3224b0f72681ebcfaec3c51b3d7efe187a5cab0355b4bbe6cffadde0d17d2292: Error fetching blob: invalid status code from registry 503 (Service Unavailable)
                time="2021-12-14T08:55:10-05:00" level=error msg="exit status 125"
                '''  # noqa: E501
            ),
        ),
        (
            r'Failed build the index image:.*read/write on closed pipe',
            textwrap.dedent(
                '''
                2024-04-25 15:46:56,754 iib.workers.tasks.utils ForkPoolWorker-1 request-715681 ERROR utils.run_cmd The command "buildah bud --no-cache --format docker --override-arch arm64 --arch arm64 -t iib-build:715681-arm64 -f /tmp/iib-715681-h8oqqbe6/index.Dockerfile" failed with: Trying to pull registry.redhat.io/openshift4/ose-operator-registry@sha256:26ebec42ba8d632ac9e2b7af92eba447c90f1d864d93481ac44d092e003600db...
                time="2024-04-25T15:46:56Z" level=error msg="Can't add file /home/iib-worker-cvp-parallel-2/.local/share/containers/storage/overlay/5a5a673222a5bde8d43d28bcab6665b4921a62ec34de97a2f170a1a46774169e/diff/tmp/cache/cache/advanced-cluster-management_release-2.7_advanced-cluster-management.v2.7.2.json to tar: io: read/write on closed pipe"
                time="2024-04-25T15:46:56Z" level=error msg="io: read/write on closed pipe"
                time="2024-04-25T15:46:56Z" level=error msg="Can't close tar writer: io: read/write on closed pipe"
                Error: committing container for step to file: io: read/write on closed pipe
                '''  # noqa: E501
            ),
        ),
    ],
)
@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed_buildah(mock_sub_run, expected_exc, subprocess_stderr):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = subprocess_stderr
    mock_sub_run.return_value = mock_rv

    with pytest.raises(ExternalServiceError, match=expected_exc):
        utils.run_cmd(
            [
                'buildah',
                'bud',
                '--no-cache',
                '--format',
                'docker',
                '--override-arch',
                's390x',
                '--arch',
                's390x',
                '-t',
                'iib-build:56056-s390x',
                '-f',
                '/tmp/iib-ozo81z6o/index.Dockerfile',
            ],
            exc_msg='Failed build the index image',
        )

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed_buildah_manifest_rm(mock_sub_run):
    mock_rv = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = textwrap.dedent(
        '''
        1 error occurred:
            * something: image not known
        '''  # noqa: E501
    )
    mock_sub_run.return_value = mock_rv

    expected_exc = 'Manifest list not found locally.'
    with pytest.raises(IIBError, match=expected_exc):
        utils.run_cmd(
            ['buildah', 'manifest', 'rm', 'something'],
            exc_msg='Failed to remove local manifest list. something does not exist',
        )

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.subprocess.run')
def test_run_cmd_failed_buildah_registry_unavailable(mock_sub_run: mock.MagicMock) -> None:
    mock_rv: mock.Mock = mock.Mock()
    mock_rv.returncode = 1
    mock_rv.stderr = textwrap.dedent(
        '''
        error creating build container: 
        Error determining manifest MIME type for docker://registry.redhat.io/openshift4/ose-operator-registry@sha256:8f3d471eccaad18e61fe6326c544cfcfaff35c012c6d5c4da01cbe887e03b904: 
        Error reading manifest sha256:db6fd9f033865da55ab2e4647ae283a51556cd11ef4241361ac04cb05b5ef795 in registry.redhat.io/openshift4/ose-operator-registry: 
        received unexpected HTTP status: 503 Service Unavailable
        '''  # noqa: E501 W291
    ).replace('\n', '')
    mock_sub_run.return_value = mock_rv

    expected_exc: str = 'error creating build container: 503 Service Unavailable'
    with pytest.raises(ExternalServiceError, match=expected_exc):
        utils.run_cmd(
            [
                'buildah',
                'bud',
                '--no-cache',
                '--format',
                'docker',
                '--override-arch',
                'amd64',
                '--arch',
                'amd64',
                '-t',
                'foo',
                '-f',
                'bar',
            ],
            exc_msg=f'Failed to build the container image on the arch amd64',
        )

    mock_sub_run.assert_called_once()


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_skopeo_inspect(mock_run_cmd):
    mock_run_cmd.return_value = '{"Name": "some-image"}'
    image = 'docker://some-image:latest'
    rv = utils.skopeo_inspect(image)
    assert rv == {"Name": "some-image"}
    skopeo_args = mock_run_cmd.call_args[0][0]
    expected = ['skopeo', '--command-timeout', '300s', 'inspect', image]
    assert skopeo_args == expected


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_podman_pull(mock_run_cmd):
    image = 'some-image:latest'
    utils.podman_pull(image)
    mock_run_cmd.assert_called_once_with(['podman', 'pull', image], exc_msg=mock.ANY)


@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.utils.upload_file_to_s3_bucket')
def test_request_logger(mock_ufts3b, mock_runcmd, tmpdir):
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    utils_logger = logging.getLogger('iib.workers.tasks.utils')
    utils_logger.disabled = False
    utils_logger.setLevel(logging.DEBUG)

    logs_dir = tmpdir.join('logs')
    logs_dir.mkdir()
    config = get_worker_config()
    config.iib_request_logs_dir = str(logs_dir)
    config.iib_aws_s3_bucket_name = 's3-bucket'

    mock_runcmd.side_effects = [
        'Version: version.Version{OpmVersion:"v1.21.0", GitCommit:"9999f796", '
        'BuildDate:"2022-03-03T21:23:12Z", GoOs:"linux", GoArch:"amd64"}',
        'podman version 4.0.2',
        'buildah version 1.24.2 (image-spec 1.0.2-dev, runtime-spec 1.0.2-dev)',
    ]

    original_handlers_count = len(logging.getLogger().handlers)

    @utils.request_logger
    def mock_handler(spam, eggs, request_id, bacon):
        logging.getLogger('iib.workers.tasks.utils').info('this is a test')

    expected_message = (
        ' iib.workers.tasks.utils MainProcess request-{rid} '
        'INFO test_utils.mock_handler this is a test\n'
    )

    mock_handler('spam', 'eggs', 123, 'bacon')
    assert logs_dir.join('123.log').read().endswith(expected_message.format(rid=123))
    assert original_handlers_count == len(logging.getLogger().handlers)
    mock_ufts3b.assert_called_with(f'{logs_dir}/123.log', 'request_logs', '123.log')

    mock_handler('spam', 'eggs', bacon='bacon', request_id=321)
    assert logs_dir.join('321.log').read().endswith(expected_message.format(rid=321))
    assert original_handlers_count == len(logging.getLogger().handlers)
    mock_ufts3b.assert_called_with(f'{logs_dir}/321.log', 'request_logs', '321.log')

    assert mock_ufts3b.call_count == 2


def test_request_logger_no_request_id(tmpdir):
    logs_dir = tmpdir.join('logs')
    logs_dir.mkdir()
    get_worker_config().iib_request_logs_dir = str(logs_dir)

    @utils.request_logger
    def mock_handler(spam, eggs, request_id, bacon):
        raise ValueError('Handler executed unexpectedly')

    with pytest.raises(IIBError, match='Unable to get "request_id" from'):
        mock_handler('spam', 'eggs', None, 'bacon')

    assert not logs_dir.listdir()


@pytest.mark.parametrize(
    'pull_spec, expected',
    (
        (
            'quay.io/ns/image:8',
            (
                'quay.io/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1c7e82'
                '8c568e'
            ),
        ),
        (
            'quay.io:443/ns/image:8',
            (
                'quay.io:443/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1'
                'c7e828c568e'
            ),
        ),
        (
            (
                'quay.io/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1c7e8'
                '28c568e'
            ),
            (
                'quay.io/ns/image@sha256:3182d6cb9a6b9e31112cbe8c7b994d870bf0c8d7bed1b827af1f1c7e82'
                '8c568e'
            ),
        ),
    ),
)
@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_resolved_image(mock_si, pull_spec, expected):
    mock_si.return_value = textwrap.dedent(
        '''
        {
           "schemaVersion": 2,
           "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
           "config": {
              "mediaType": "application/vnd.docker.container.image.v1+json",
              "size": 5545,
              "digest": "sha256:720713e1a4410985aacd7008719efd13d8a32e76d08d34fca202a60ff43e516d"
           },
           "layers": [
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 76275160,
                 "digest": "sha256:a3ac36470b00df382448e79f7a749aa6833e4ac9cc90e3391f778820db9fa407"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 1598,
                 "digest": "sha256:82a8f4ea76cb6f833c5f179b3e6eda9f2267ed8ac7d1bf652f88ac3e9cc453d1"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 3500790,
                 "digest": "sha256:e1a6856f83e7ab214d6a8200d5fd22f2311e794c91c59eae3fd49699cbc4a14e"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 8236572,
                 "digest": "sha256:c82b363416dcd84a2f1c292c3a85b21cbf01f5f2ee7f8b88f4dcfffe53ce549d"
              },
              {
                 "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                 "size": 92298818,
                 "digest": "sha256:8befc59eb9f1a3f40d3de0eccca8762c95800322c3a83fe40bbc0273df394ac1"
              }
           ]
        }
        '''  # noqa: E501
    ).strip('\n')
    rv = utils.get_resolved_image(pull_spec)
    assert rv == expected


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_resolved_image_manifest_list(mock_si):
    mock_si.return_value = (
        r'{"manifests":[{"digest":"sha256:9e0c275e0bcb495773b10a18e499985d782810e47b4fce076422acb4b'
        r'c3da3dd","mediaType":"application\/vnd.docker.distribution.manifest.v2+json","platform":'
        r'{"architecture":"amd64","os":"linux"},"size":529},{"digest":"sha256:85313b812ad747dd19cf1'
        r'8078795b576cc4ae9cd2ca2ccccd7b5c12722b2effd","mediaType":"application\/vnd.docker.distrib'
        r'ution.manifest.v2+json","platform":{"architecture":"arm64","os":"linux","variant":"v8"},"'
        r'size":529},{"digest":"sha256:567785922b920b35aee6a217f70433fd437b335ad45054743c960d1aaa14'
        r'3dcd","mediaType":"application\/vnd.docker.distribution.manifest.v2+json","platform":{"ar'
        r'chitecture":"ppc64le","os":"linux"},"size":529}],"mediaType":"application\/vnd.docker.dis'
        r'tribution.manifest.list.v2+json","schemaVersion":2}'
    )
    rv = utils.get_resolved_image('docker.io/library/centos:8')
    assert rv == (
        'docker.io/library/centos@sha256:fe8d824220415eed5477b63addf40fb06c3b049404242b31982106ac'
        '204f6700'
    )
    mock_si.assert_called_once_with(
        'docker://docker.io/library/centos:8', '--raw', return_json=False
    )


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_resolved_image_schema_1(mock_si):
    image_manifest_schema_1 = textwrap.dedent(
        """\
        {
           "schemaVersion": 1,
           "name": "repository/name",
           "tag": "1.0.0",
           "architecture": "amd64",
           "fsLayers": [],
           "history": [],
           "signatures": [
              {
                 "header": {},
                 "signature": "text-that-changes-per-request",
                 "protected": "spam"
              }
           ]
        }
        """
    )

    skopeo_output = {
        "Name": "registry.example.com/repository/name",
        "Tag": "1.0.0",
        "Digest": "sha256:aa6680b35f45cf0fd6fb5f417159257ba410a47b8fa20d37b4c7fcd4a564b3fb",
        "RepoTags": ["1.0.0", "latest"],
        "Created": "2019-12-04T06:41:46.3149046Z",
        "DockerVersion": "19.03.2",
        "Labels": {},
        "Architecture": "amd64",
        "Os": "linux",
        "Layers": [],
        "Env": [],
    }

    mock_si.side_effect = [image_manifest_schema_1, skopeo_output]
    rv = utils.get_resolved_image('registry.example.com/repository/name:1.0.0')
    assert rv == (
        'registry.example.com/repository/name@sha256:aa6680b35f45cf0fd6fb5f417159257ba410a47b8fa2'
        '0d37b4c7fcd4a564b3fb'
    )

    mock_si.assert_has_calls(
        [
            mock.call(
                'docker://registry.example.com/repository/name:1.0.0', '--raw', return_json=False
            ),
            mock.call('docker://registry.example.com/repository/name:1.0.0'),
        ]
    )


@pytest.mark.parametrize(
    'skopeo_inspect_rv, expected_response',
    (
        (
            {
                'mediaType': 'application/vnd.docker.distribution.manifest.list.v2+json',
                'manifests': [
                    {'platform': {'architecture': 'amd64'}, 'digest': 'arch_digest'},
                    {'platform': {'architecture': 's390x'}, 'digest': 'different_arch_digest'},
                ],
            },
            ['some_bundle@arch_digest'],
        ),
        (
            {
                'mediaType': 'application/vnd.docker.distribution.manifest.v2+json',
                'schemaVersion': 2,
            },
            ['some_bundle@manifest_digest'],
        ),
    ),
)
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_resolved_bundles_success(mock_si, mock_gri, skopeo_inspect_rv, expected_response):
    mock_si.return_value = skopeo_inspect_rv
    mock_gri.return_value = 'some_bundle@manifest_digest'
    response = utils.get_resolved_bundles(['some_bundle:1.2'])
    if skopeo_inspect_rv['mediaType'] == 'application/vnd.docker.distribution.manifest.v2+json':
        mock_gri.assert_called_once()
    else:
        mock_gri.assert_not_called()
    assert response == expected_response


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_resolved_bundles_failure(mock_si):
    skopeo_inspect_rv = {
        'mediaType': 'application/vnd.docker.distribution.notmanifest.v2+json',
        'schemaVersion': 1,
    }
    mock_si.return_value = skopeo_inspect_rv
    with pytest.raises(IIBError, match='.+ and schema version 1 is not supported by IIB.'):
        utils.get_resolved_bundles(['some_bundle@some_sha'])


@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_rasise_exception_on_none_mediatype_skopeo_inspect(mock_run_cmd):
    mock_run_cmd.return_value = '{"Name": "some-image"}'
    image = 'docker://some-image:latest'
    with pytest.raises(IIBError, match='mediaType not found'):
        utils.skopeo_inspect(image, '--raw', require_media_type=True)


@pytest.mark.parametrize(
    'pull_spec, expected',
    (
        ('quay.io/ns/repo:latest', 'quay.io/ns/repo'),
        ('quay.io/ns/repo@sha256:123456', 'quay.io/ns/repo'),
    ),
)
def test_get_container_image_name(pull_spec, expected):
    assert utils._get_container_image_name(pull_spec) == expected


@mock.patch('iib.workers.tasks.utils.get_resolved_bundles')
def testget_bundles_from_deprecation_list(mock_grb):
    present_bundles = [
        'quay.io/bundle1@sha256:123456',
        'quay.io/bundle2@sha256:987654',
        'quay.io/bundle3@sha256:not555',
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
    deprecate_bundles = utils.get_bundles_from_deprecation_list(present_bundles, deprecation_list)
    assert deprecate_bundles == ['quay.io/bundle1@sha256:123456', 'quay.io/bundle2@sha256:987654']
    mock_grb.assert_called_once_with(deprecation_list)


def test_chmod_recursiverly(tmpdir):
    # Create a directory structure like this:
    # spam-dir/
    # └── eggs-dir
    #     ├── eggs-file
    #     ├── eggs-symlink -> spam-dir/eggs-dir/missing-file
    #     └── bacon-dir

    spam_dir = tmpdir.mkdir('spam-dir')
    eggs_dir = spam_dir.mkdir('eggs-dir')
    bacon_dir = eggs_dir.mkdir('bacon-dir')

    eggs_file = eggs_dir.join('eggs_file')
    eggs_file.write('')

    eggs_symlink = eggs_dir.join('eggs-symlink')
    missing_file = eggs_dir.join('missing-file')
    os.symlink(str(missing_file), str(eggs_symlink))
    assert not missing_file.exists()

    # Set the current file mode to some initial known values so we can verify
    # they're modified properly
    eggs_file.chmod(stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    bacon_dir.chmod(stat.S_IRUSR | stat.S_IRWXG)
    eggs_dir.chmod(stat.S_IRWXU)
    spam_dir.chmod(stat.S_IRUSR)

    expected_dir_mode = stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
    expected_file_mode = stat.S_IRUSR | stat.S_IRGRP
    utils.chmod_recursively(spam_dir, dir_mode=expected_dir_mode, file_mode=expected_file_mode)

    def assert_mode(file_path, expected_mode):
        # The last three digits specify the file mode for user-group-others
        assert oct(os.stat(file_path).st_mode)[-3:] == oct(expected_mode)[-3:]

    assert_mode(eggs_file, expected_file_mode)
    assert_mode(bacon_dir, expected_dir_mode)
    assert_mode(eggs_dir, expected_dir_mode)
    assert_mode(bacon_dir, expected_dir_mode)


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_arches(mock_si):
    mock_si.return_value = {
        'mediaType': 'application/vnd.docker.distribution.manifest.list.v2+json',
        'manifests': [
            {'platform': {'architecture': 'amd64'}},
            {'platform': {'architecture': 's390x'}},
        ],
    }
    rv = utils.get_image_arches('image:latest')
    assert rv == {'amd64', 's390x'}


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_arches_manifest(mock_si):
    mock_si.side_effect = [
        {'mediaType': 'application/vnd.docker.distribution.manifest.v2+json'},
        {'architecture': 'amd64'},
    ]
    rv = utils.get_image_arches('image:latest')
    assert rv == {'amd64'}


@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_arches_not_manifest_list(mock_si):
    mock_si.return_value = {'mediaType': 'application/vnd.docker.distribution.notmanifest.v2+json'}
    with pytest.raises(IIBError, match='.+is neither a v2 manifest list nor a v2 manifest'):
        utils.get_image_arches('image:latest')


@pytest.mark.parametrize('label, expected', (('some_label', 'value'), ('not_there', '')))
@mock.patch('iib.workers.tasks.utils.skopeo_inspect')
def test_get_image_label(mock_si, label, expected):
    mock_si.return_value = {'config': {'Labels': {'some_label': 'value'}}}
    assert utils.get_image_label('some-image:latest', label) == expected


@pytest.mark.parametrize(
    'iib_required_labels', ({'com.redhat.delivery.operator.bundle': 'true'}, {})
)
@mock.patch('iib.workers.tasks.utils.get_worker_config')
@mock.patch('iib.workers.tasks.utils.get_image_labels')
def test_verify_labels(mock_gil, mock_gwc, iib_required_labels):
    mock_gwc.return_value = {'iib_required_labels': iib_required_labels}
    mock_gil.return_value = {'com.redhat.delivery.operator.bundle': 'true'}
    utils.verify_labels(['some-bundle:v1.0'])

    if iib_required_labels:
        mock_gil.assert_called_once()
    else:
        mock_gil.assert_not_called()


@mock.patch('iib.workers.tasks.utils.get_worker_config')
@mock.patch('iib.workers.tasks.utils.get_image_labels')
def test_verify_labels_fails(mock_gil, mock_gwc):
    mock_gwc.return_value = {'iib_required_labels': {'com.redhat.delivery.operator.bundle': 'true'}}
    mock_gil.return_value = {'lunch': 'pizza'}
    with pytest.raises(IIBError, match='som'):
        utils.verify_labels(['some-bundle:v1.0'])


@pytest.mark.parametrize(
    'add_arches, from_index, from_index_arches, bundles, binary_image,'
    'expected_bundle_mapping, distribution_scope, resolved_distribution_scope, binary_image_config',
    (
        ([], 'some-index:latest', {'amd64'}, None, 'binary-image:latest', {}, None, 'prod', {}),
        (['amd64', 's390x'], None, set(), None, 'binary-image:latest', {}, None, 'prod', {}),
        (
            ['amd64'],
            'some-index:latest',
            {'amd64'},
            None,
            'binary-image:latest',
            {},
            None,
            'prod',
            {},
        ),
        (
            ['amd64'],
            None,
            set(),
            ['quay.io/some-bundle:v1', 'quay.io/some-bundle2:v1'],
            None,
            {
                'some-bundle': ['quay.io/some-bundle:v1'],
                'some-bundle2': ['quay.io/some-bundle2:v1'],
            },
            None,
            'prod',
            {'prod': {'v4.5': 'binary-image:prod'}},
        ),
        (
            ['amd64'],
            'some-index:latest',
            set(),
            ['quay.io/some-bundle:v1', 'quay.io/some-bundle2:v1'],
            'binary-image:latest',
            {
                'some-bundle': ['quay.io/some-bundle:v1'],
                'some-bundle2': ['quay.io/some-bundle2:v1'],
            },
            None,
            'prod',
            {},
        ),
        (
            ['amd64'],
            'some-index:latest',
            set(),
            ['quay.io/some-bundle:v1', 'quay.io/some-bundle2:v1'],
            'binary-image:latest',
            {
                'some-bundle': ['quay.io/some-bundle:v1'],
                'some-bundle2': ['quay.io/some-bundle2:v1'],
            },
            None,
            'prod',
            {},
        ),
    ),
)
@mock.patch('iib.workers.tasks.build.set_request_state')
@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.utils.get_image_arches')
@mock.patch('iib.workers.tasks.utils.get_image_label')
@mock.patch('iib.workers.tasks.build.update_request')
def test_prepare_request_for_build(
    mock_ur,
    mock_gil,
    mock_gia,
    mock_gri,
    mock_srs,
    mock_srs2,
    add_arches,
    from_index,
    from_index_arches,
    bundles,
    binary_image,
    expected_bundle_mapping,
    distribution_scope,
    resolved_distribution_scope,
    binary_image_config,
):
    binary_image_resolved = 'binary-image@sha256:abcdef'
    from_index_resolved = None
    expected_arches = set(add_arches) | from_index_arches
    expected_payload_keys = {'binary_image_resolved', 'state', 'state_reason'}
    gil_side_effect = []
    ocp_version = 'v4.5'
    if expected_bundle_mapping:
        expected_payload_keys.add('bundle_mapping')
    if from_index:
        from_index_name = from_index.split(':', 1)[0]
        from_index_resolved = f'{from_index_name}@sha256:bcdefg'
        index_resolved = f'{from_index_name}@sha256:abcdef1234'
        mock_gri.side_effect = [from_index_resolved, binary_image_resolved, index_resolved]
        mock_gia.side_effect = [from_index_arches, expected_arches]
        expected_payload_keys.add('from_index_resolved')
        gil_side_effect = ['v4.6', resolved_distribution_scope]
        ocp_version = 'v4.6'
    else:
        index_resolved = f'index-image@sha256:abcdef1234'
        mock_gri.side_effect = [binary_image_resolved, index_resolved]
        mock_gia.side_effect = [expected_arches]
        gil_side_effect = []

    if bundles:
        bundle_side_effects = [bundle.rsplit('/', 1)[1].split(':', 1)[0] for bundle in bundles]
        gil_side_effect.extend(bundle_side_effects)

    mock_gil.side_effect = gil_side_effect

    rv = utils.prepare_request_for_build(
        1,
        utils.RequestConfigAddRm(
            _binary_image=binary_image,
            from_index=from_index,
            overwrite_from_index_token=None,
            add_arches=add_arches,
            bundles=bundles,
            distribution_scope=distribution_scope,
            binary_image_config=binary_image_config,
        ),
    )

    if not binary_image:
        binary_image = 'binary-image:prod'

    assert rv == {
        'arches': expected_arches,
        'binary_image': binary_image,
        'binary_image_resolved': binary_image_resolved,
        'bundle_mapping': expected_bundle_mapping,
        'from_index_resolved': from_index_resolved,
        'ocp_version': ocp_version,
        # want to verify that the output is always lower cased.
        'distribution_scope': resolved_distribution_scope.lower(),
        'source_from_index_resolved': None,
        'source_ocp_version': 'v4.5',
        'target_index_resolved': None,
        'target_ocp_version': 'v4.6',
    }


@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.utils.get_index_image_info')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.utils.get_image_arches')
def test_prepare_request_for_build_merge_index_img(mock_gia, mock_gri, mock_giii, mock_srs):
    from_index_image_info = {
        'resolved_from_index': None,
        'ocp_version': 'v4.5',
        'arches': set(),
        'resolved_distribution_scope': 'prod',
    }
    source_index_image_info = {
        'resolved_from_index': 'some_resolved_image@sha256',
        'ocp_version': 'v4.5',
        'arches': {'amd64'},
        'resolved_distribution_scope': 'stage',
    }

    target_index_info = {
        'resolved_from_index': 'some_other_image@sha256',
        'ocp_version': 'v4.9',
        'arches': {'amd64'},
        'resolved_distribution_scope': 'prod',
    }
    mock_giii.side_effect = [from_index_image_info, source_index_image_info, target_index_info]
    mock_gri.return_value = 'binary-image@sha256:12345'
    mock_gia.return_value = {'amd64'}
    rv = utils.prepare_request_for_build(
        1,
        utils.RequestConfigMerge(
            _binary_image='binary-image:tag',
            overwrite_target_index_token=None,
            source_from_index='some_source_index:tag',
            target_index='some_target_index:tag',
        ),
    )

    assert rv == {
        'arches': {'amd64'},
        'binary_image': 'binary-image:tag',
        'binary_image_resolved': 'binary-image@sha256:12345',
        'bundle_mapping': {},
        'from_index_resolved': None,
        'ocp_version': 'v4.5',
        'distribution_scope': 'prod',
        'source_ocp_version': 'v4.5',
        'source_from_index_resolved': 'some_resolved_image@sha256',
        'target_index_resolved': 'some_other_image@sha256',
        'target_ocp_version': 'v4.9',
    }


@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.utils.get_image_arches')
def test_prepare_request_for_build_no_arches(mock_gia, mock_gri, mock_srs):
    mock_gia.side_effect = [{'amd64'}]

    with pytest.raises(IIBError, match='No arches.+'):
        utils.prepare_request_for_build(
            1, utils.RequestConfigAddRm(_binary_image='binary-image:latest')
        )


@mock.patch('iib.workers.tasks.utils.set_request_state')
@mock.patch('iib.workers.tasks.utils.get_resolved_image')
@mock.patch('iib.workers.tasks.utils.get_image_arches')
def test_prepare_request_for_build_binary_image_no_arch(mock_gia, mock_gri, mock_srs):
    mock_gia.side_effect = [{'amd64'}]

    expected = 'The binary image is not available for the following arches.+'
    with pytest.raises(IIBError, match=expected):
        utils.prepare_request_for_build(
            1, utils.RequestConfigAddRm(_binary_image='binary-image:latest', add_arches=['s390x'])
        )


@pytest.mark.parametrize(
    'resolved_distribution_scope, distribution_scope, output, raise_exception',
    (
        ('prod', 'prod', 'prod', False),
        ('prod', 'stage', 'stage', False),
        ('prod', 'dev', 'dev', False),
        ('stage', 'stage', 'stage', False),
        ('stage', 'dev', 'dev', False),
        ('stage', None, 'stage', False),
        ('stage', 'prod', 'prod', True),
        ('dev', 'stage', 'prod', True),
        ('dev', 'prod', 'prod', True),
    ),
)
def test_validate_distribution_scope(
    resolved_distribution_scope, distribution_scope, output, raise_exception
):
    if raise_exception:
        expected = f'Cannot set "distribution_scope" to {distribution_scope.lower()} because from'
        f'index is already set to {resolved_distribution_scope.lower()}'
        with pytest.raises(IIBError, match=expected):
            utils._validate_distribution_scope(resolved_distribution_scope, distribution_scope)
    else:
        assert (
            utils._validate_distribution_scope(resolved_distribution_scope, distribution_scope)
            == output
        )


def test_get_binary_image_config_no_config_val():
    with pytest.raises(IIBError, match='IIB does not have a configured binary_image.+'):
        utils.get_binary_image_from_config('prod', 'v4.5', {'prod': {'v4.6': 'binary_image'}})


@pytest.mark.parametrize(
    'endpoint',
    (
        "api.Registry/ListPackages",
        "api.Registry/ListBundles",
    ),
)
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.utils.opm_serve_from_index')
@mock.patch('iib.workers.tasks.build._get_index_database')
def test_grpcurl_get_db_data_success(mock_gid, mock_osfi, mock_run_cmd, tmpdir, endpoint):
    mock_gid.return_value = tmpdir.join('index.db')
    mock_popen = mock.MagicMock()
    mock_osfi.return_value = 50051, mock_popen
    mock_run_cmd.side_effect = ['{\n"name": "package1"\n}\n{\n"name": "package2"\n}\n']
    utils.grpcurl_get_db_data('quay.io/index-image:4.5', str(tmpdir), endpoint)


@pytest.mark.parametrize(
    'endpoint, err_msg',
    (
        (
            "api.Registry/GetPackages",
            "The endpoint 'api.Registry/GetPackages' is not allowed to be used",
        ),
        ("something", "The endpoint 'something' is not allowed to be used"),
    ),
)
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.utils.opm_serve_from_index')
def test_grpcurl_get_db_data_wrong_endpoint(mock_osfi, mock_run_cmd, tmpdir, endpoint, err_msg):
    mock_popen = mock.MagicMock()
    mock_osfi.return_value = 50051, mock_popen

    with pytest.raises(IIBError, match=err_msg):
        utils.grpcurl_get_db_data('quay.io/index-image:4.5', str(tmpdir), endpoint)

    mock_osfi.assert_called_once()
    mock_run_cmd.assert_not_called()


@mock.patch('iib.workers.tasks.utils.opm_registry_serve')
@mock.patch('iib.workers.tasks.utils.get_bundle_json')
@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.utils._add_property_to_index')
def test_add_max_ocp_version_property_empty_index(mock_apti, mock_cmd, mock_gbj, mock_ors, tmpdir):
    port = 0
    my_mock = mock.MagicMock()
    mock_ors.return_value = (port, my_mock)
    mock_cmd.return_value = None

    utils.add_max_ocp_version_property([], tmpdir)

    mock_gbj.assert_not_called()
    mock_apti.assert_not_called()
