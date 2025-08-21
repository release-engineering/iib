# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError, FinalStateOverwriteError
from iib.workers.tasks import general


@pytest.mark.parametrize(
    'exc, expected_msg',
    (
        (IIBError('Is it lunch time yet?'), 'Is it lunch time yet?'),
        (
            RuntimeError('I cannot run in the rain!'),
            'An unknown error occurred. See logs for details',
        ),
        (FinalStateOverwriteError("can not overwite final state"), "Already in final state"),
    ),
)
@mock.patch('iib.workers.tasks.general._cleanup')
@mock.patch('iib.workers.tasks.general.set_request_state')
def test_failed_request_callback(mock_srs, mock_cleanup, exc, expected_msg):
    general.failed_request_callback(None, exc, None, 3)
    if isinstance(exc, FinalStateOverwriteError):
        mock_srs.assert_not_called()
    else:
        mock_srs.assert_called_once_with(3, 'failed', expected_msg)
    mock_cleanup.assert_called_once()
