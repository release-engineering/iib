from unittest import mock

import pytest

from iib.workers.dogpile_cache import skopeo_inspect_should_use_cache
from iib.workers.tasks import utils


@pytest.mark.parametrize(
    'value, result',
    [
        ('docker://with_digest@sha256:93120347593478509347tdsvzkljbn', True),
        ('docker://without_digest:tag', False),
    ],
)
def test_should_cache(value, result):
    assert skopeo_inspect_should_use_cache(value) is result


@mock.patch('dogpile.cache.region.CacheRegion.get')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_skopeo_inspect_cache(mock_run_cmd, moc_dpr_get):
    mock_run_cmd.return_value = '{"Name": "some-image-cache"}'
    image = 'docker://some-image-cache@sha256:129bfb6af3e03997eb_not_real_sha_c7c18d89b40d97'
    rv_expected = {'Name': 'some-image-cache'}
    moc_dpr_get.return_value = rv_expected

    rv = utils.skopeo_inspect(image)
    assert rv == rv_expected
    assert mock_run_cmd.called is False

    assert mock_run_cmd.call_args is None


@mock.patch('dogpile.cache.region.CacheRegion.get')
@mock.patch('iib.workers.tasks.utils.run_cmd')
def test_skopeo_inspect_no_cache(mock_run_cmd, moc_dpr_get):
    mock_run_cmd.return_value = '{"Name": "some-image-cache"}'
    image = 'docker://some-image-no-cache:tag'
    rv_expected = {"Name": "some-image-cache"}

    rv = utils.skopeo_inspect(image)
    assert rv == rv_expected
    assert mock_run_cmd.called is True
    assert moc_dpr_get.called is False

    skopeo_args = mock_run_cmd.call_args[0][0]
    args_expected = ['skopeo', '--command-timeout', '300s', 'inspect', image]
    assert skopeo_args == args_expected
