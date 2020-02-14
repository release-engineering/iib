# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import pytest

from iib.exceptions import IIBError
from iib.workers.tasks import general


@pytest.mark.parametrize(
    'exc, expected_msg',
    (
        (IIBError('Is it lunch time yet?'), 'Is it lunch time yet?'),
        (RuntimeError('I cannot run in the rain!'), 'An unknown error occurred'),
    ),
)
@mock.patch('iib.workers.tasks.general.set_request_state')
def test_failed_request_callback(mock_srs, exc, expected_msg):
    general.failed_request_callback(None, exc, None, 3)
    mock_srs(3, expected_msg)
