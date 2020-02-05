# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock


@mock.patch('iib.web.api_v1.ping')
def test_example_endpoint(mock_ping, app, client):
    response = client.get('/api/v1/test')
    mock_ping.delay.assert_called_once()
    assert response.status_code == 200
    assert response.data == b'Test request success!'
