from unittest import mock

import kombu
import pytest
import sqlalchemy

from iib.exceptions import IIBError
from iib.web import healthcheck


@pytest.mark.parametrize(
    'exc, exception_msg',
    [
        (None, None),
        (sqlalchemy.exc.SQLAlchemyError('exception'), 'an error occurred'),
        (
            # sqlalchemy.exc.OperationalError('statement', 'params', 'orig')
            sqlalchemy.exc.OperationalError('another exception', None, None),
            'database connection failed',
        ),
    ],
)
@mock.patch.object(healthcheck.db, 'session')
def test_database_connection(mock_db_session, exc, exception_msg):
    session = mock_db_session.return_value

    if exc is not None:
        session.execute.side_effect = [exc]

    ok, reason = healthcheck.database_connection_check()

    assert ok == (exc is None)
    assert reason == exception_msg

    session.execute.assert_called_once_with('SELECT 1')
    session.close.assert_called_once()


@pytest.mark.parametrize(
    'exc, exception_msg',
    [
        (None, None),
        (kombu.exceptions.KombuError('exception'), 'an error occurred'),
        (kombu.exceptions.OperationalError('another exception'), 'broker connection failed'),
    ],
)
@mock.patch('kombu.Connection')
def test_rabbitmq_connection(mock_kombu_conn, exc, exception_msg):
    broker_url = 'amqp://test@rabbitmq:5672//'

    conn = mock_kombu_conn.return_value.__enter__.return_value
    if exc is not None:
        conn.ensure_connection.side_effect = [exc]

    ok, reason = healthcheck.rabbitmq_connection_check(broker_url)

    assert ok == (exc is None)
    assert reason == exception_msg

    mock_kombu_conn.assert_called_once_with(broker_url)
    conn.ensure_connection.assert_called_once_with(max_retries=0)


@pytest.mark.parametrize('retries', [0, 1])
@pytest.mark.parametrize(
    'ping_response, expected_result',
    [
        ({}, []),
        (
            {'celery@cd921bf1b7bd': {'ok': 'pong'}, 'celery@cd921bf1b7bf': {'ok': 'pong'}},
            [
                {'name': 'celery@cd921bf1b7bd', 'available': True},
                {'name': 'celery@cd921bf1b7bf', 'available': True},
            ],
        ),
        (
            {
                'celery@cd921bf1b7bd': {'ok': 'pong'},
                'celery@cd921bf1b7bf': {'error': 'unknown error'},
            },
            [
                {'name': 'celery@cd921bf1b7bd', 'available': True},
                {'name': 'celery@cd921bf1b7bf', 'available': False, 'reason': 'unknown error'},
            ],
        ),
    ],
)
@mock.patch('iib.web.healthcheck._ping_celery_workers')
def test_workers_status(mock_ping_celery_workers, retries, ping_response, expected_result):
    mock_ping_celery_workers.return_value = ping_response

    workers = healthcheck.workers_status(retries=retries)
    assert workers == expected_result

    mock_ping_celery_workers.assert_called_once_with(retries=retries)


def mock_worker_config():
    config = mock.Mock()
    config.broker_url = 'amqp://test@rabbitmq:5672//'
    return config


@pytest.mark.parametrize('short', [True, False])
@mock.patch('iib.web.healthcheck.get_worker_config')
@mock.patch('iib.web.healthcheck.database_connection_check')
@mock.patch('iib.web.healthcheck.rabbitmq_connection_check')
@mock.patch('iib.web.healthcheck.workers_status')
def test_status_all_happy(
    mock_workers_status,
    mock_rabbitmq_connection_check,
    mock_database_connection_check,
    mock_get_worker_config,
    short,
):

    config = mock_worker_config()

    mock_get_worker_config.return_value = config
    mock_database_connection_check.return_value = (True, None)
    mock_rabbitmq_connection_check.return_value = (True, None)
    mock_workers_status.return_value = [{'name': 'celery@a868abda4f69', 'available': True}]

    result = healthcheck.status(short=short)

    expected_services = [
        {'name': 'DATABASE', 'available': True},
        {'name': 'RABBITMQ', 'available': True},
    ]

    assert result == {
        'services': expected_services,
        'workers': mock_workers_status.return_value,
    }

    mock_database_connection_check.assert_called_once()
    mock_rabbitmq_connection_check.assert_called_once_with(config.broker_url)
    mock_workers_status.assert_called_once_with(retries=2)


@pytest.mark.parametrize('short', [True, False])
@mock.patch('iib.web.healthcheck.get_worker_config')
@mock.patch('iib.web.healthcheck.database_connection_check')
@mock.patch('iib.web.healthcheck.rabbitmq_connection_check')
@mock.patch('iib.web.healthcheck.workers_status')
def test_status_database_not_okay(
    mock_workers_status,
    mock_rabbitmq_connection_check,
    mock_database_connection_check,
    mock_get_worker_config,
    short,
):

    config = mock_worker_config()

    mock_get_worker_config.return_value = config

    mock_database_connection_check.return_value = (False, 'database connection failed')
    mock_rabbitmq_connection_check.return_value = (True, None)
    mock_workers_status.return_value = [{'name': 'celery@a868abda4f69', 'available': True}]

    if short:
        with pytest.raises(IIBError, match='DATABASE unavailable: database connection failed'):
            healthcheck.status(short=True)
        return

    result = healthcheck.status(short=False)

    expected_services = [
        {'name': 'DATABASE', 'available': False, 'reason': 'database connection failed'},
        {'name': 'RABBITMQ', 'available': True},
    ]

    assert result == {
        'services': expected_services,
        'workers': mock_workers_status.return_value,
    }

    mock_database_connection_check.assert_called_once()
    mock_rabbitmq_connection_check.assert_called_once_with(config.broker_url)
    mock_workers_status.assert_called_once_with(retries=2)


@pytest.mark.parametrize('short', [True, False])
@pytest.mark.parametrize(
    'workers_status, expected_result',
    [
        ([], False),
        ([{'name': 'celery@a868abda4f68', 'available': False, 'reason': 'unknown error'}], False),
        (
            [
                {'name': 'celery@a868abda4f68', 'available': False, 'reason': 'unknown error'},
                {'name': 'celery@a868abda4f69', 'available': True},
            ],
            True,
        ),
    ],
)
@mock.patch('iib.web.healthcheck.get_worker_config')
@mock.patch('iib.web.healthcheck.database_connection_check')
@mock.patch('iib.web.healthcheck.rabbitmq_connection_check')
@mock.patch('iib.web.healthcheck.workers_status')
def test_status_workers_not_okay(
    mock_workers_status,
    mock_rabbitmq_connection_check,
    mock_database_connection_check,
    mock_get_worker_config,
    short,
    workers_status,
    expected_result,
):

    config = mock_worker_config()

    mock_get_worker_config.return_value = config

    mock_database_connection_check.return_value = (True, None)
    mock_rabbitmq_connection_check.return_value = (True, None)
    mock_workers_status.return_value = workers_status

    if short and not expected_result:
        with pytest.raises(IIBError, match='no workers are available'):
            healthcheck.status(short=True)
        return

    result = healthcheck.status(short=False)

    expected_services = [
        {'name': 'DATABASE', 'available': True},
        {'name': 'RABBITMQ', 'available': True},
    ]

    assert result == {
        'services': expected_services,
        'workers': mock_workers_status.return_value,
    }

    mock_database_connection_check.assert_called_once()
    mock_rabbitmq_connection_check.assert_called_once_with(config.broker_url)
    mock_workers_status.assert_called_once_with(retries=2)
