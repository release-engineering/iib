# SPDX-License-Identifier: GPL-3.0-or-later


def test_example_endpoint(app, client):
    response = client.get('/api/v1/test')
    assert response.status_code == 200
    assert response.data == b'Test request success!'
