# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import textwrap
from unittest import mock

import pytest

from iib.exceptions import IIBError
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
                    ),
                },
                'registry.redhat.io': {'auth': 'dXNlcjpwYXNz'},
            }
        }
    else:
        mock_open.assert_called_once_with('/home/iib-worker/.docker/config.json', 'w')
        assert mock_open.call_count == 1
        assert mock_json_dump.call_args[0][0] == {
            'auths': {'registry.redhat.io': {'auth': 'dXNlcjpwYXNz'}},
        }

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


def test_retry():
    mock_func = mock.Mock()

    @utils.retry(attempts=3, wait_on=IIBError)
    def _func_to_retry():
        mock_func()
        raise IIBError('Some error')

    with pytest.raises(IIBError, match='Some error'):
        _func_to_retry()

    assert mock_func.call_count == 3


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


def test_request_logger(tmpdir):
    # Setting the logging level via caplog.set_level is not sufficient. The flask
    # related settings from previous tests interfere with this.
    utils_logger = logging.getLogger('iib.workers.tasks.utils')
    utils_logger.disabled = False
    utils_logger.setLevel(logging.DEBUG)

    logs_dir = tmpdir.join('logs')
    logs_dir.mkdir()
    get_worker_config().iib_request_logs_dir = str(logs_dir)

    original_handlers_count = len(logging.getLogger().handlers)

    @utils.request_logger
    def mock_handler(spam, eggs, request_id, bacon):
        logging.getLogger('iib.workers.tasks.utils').info('this is a test')

    expected_message = ' iib.workers.tasks.utils INFO test_utils.mock_handler this is a test\n'

    mock_handler('spam', 'eggs', 123, 'bacon')
    assert logs_dir.join('123.log').read().endswith(expected_message)
    assert original_handlers_count == len(logging.getLogger().handlers)

    mock_handler('spam', 'eggs', bacon='bacon', request_id=321)
    assert logs_dir.join('321.log').read().endswith(expected_message)
    assert original_handlers_count == len(logging.getLogger().handlers)


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


@mock.patch('iib.workers.tasks.utils.run_cmd')
@mock.patch('iib.workers.tasks.utils.set_registry_token')
def testdeprecate_bundles(mock_srt, mock_run_cmd):
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
    utils.deprecate_bundles(bundles, 'some_dir', binary_image, from_index, '4.6')
    mock_run_cmd.assert_called_once_with(
        cmd, {'cwd': 'some_dir'}, exc_msg='Failed to deprecate the bundles'
    )
