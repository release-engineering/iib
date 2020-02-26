# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

from iib.web.app import load_config


@mock.patch('iib.web.app.os.getenv')
@mock.patch('iib.web.app.os.path.isfile')
def test_load_config_dev(mock_isfile, mock_getenv):
    mock_app = mock.Mock()

    def new_getenv(key, default_value):
        return {'IIB_DEV': 'true'}.get(key, default_value)

    mock_getenv.side_effect = new_getenv
    load_config(mock_app)

    mock_app.config.from_object.assert_called_once_with('iib.web.config.DevelopmentConfig')
    mock_isfile.assert_not_called()


@mock.patch('iib.web.app.os.getenv')
@mock.patch('iib.web.app.os.path.isfile')
def test_load_config_prod(mock_isfile, mock_getenv):
    mock_isfile.return_value = True
    mock_app = mock.Mock()

    load_config(mock_app)

    mock_app.config.from_object.assert_called_once_with('iib.web.config.ProductionConfig')
    mock_isfile.assert_called_once()
    mock_app.config.from_pyfile.assert_called_once_with('/etc/iib/settings.py')
