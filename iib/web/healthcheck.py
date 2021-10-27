import time
import logging
import sqlalchemy
import kombu

from iib.exceptions import IIBError
from iib.web import db
from iib.workers.config import get_worker_config
from iib.workers.tasks.celery import app


log = logging.getLogger(__name__)


def database_connection_check():
    """
    Check if the database connection is working.

    :return: tuple (ok: bool, reason: str or None)
    """
    session = db.session()
    try:
        session.execute('SELECT 1')
    except sqlalchemy.exc.OperationalError as OperError:
        log.error('database connection failed, exception: %s', OperError)
        return False, 'database connection failed'
    except sqlalchemy.exc.SQLAlchemyError as err:
        log.error('an error occurred while trying to connect to the DB server: %s', err)
        return False, 'an error occurred'
    finally:
        session.close()

    return True, None


def rabbitmq_connection_check(broker_url):
    """
    Check if RabbitMQ connection is working.

    :param str broker_url: the AMQP(S) URL to connect to RabbitMQ
    :return: tuple (ok: bool, reason: str or None)
    """
    with kombu.Connection(broker_url) as connection:
        try:
            connection.ensure_connection(max_retries=0)
        except kombu.exceptions.OperationalError:
            log.exception('broker connection failed')
            return False, 'broker connection failed'
        except kombu.exceptions.KombuError as err:
            log.error('an error occurred while trying to connect to the RabbitMQ server: %s', err)
            return False, 'an error occurred'

    return True, None


def _ping_celery_workers(retries):
    """
    Attempt to ping worker, retry on ConnectionError.

    :param int retries: number of retry/retries before returning an empty response
    :return: dict of {hostname: reply} as returned by ping()
    """
    for i in range(retries + 1):
        if i > 0:
            time.sleep(0.25 * 2 ** (i - 1))
        try:
            replies = app.control.inspect().ping()
        except ConnectionError:
            continue

        return replies or {}

    return {}


def workers_status(retries=2):
    """
    Ping workers, check received replies.

    :param int retries: number of retry/retries on ConnectionError when pinging workers
    :return: list of workers status information (empty if no replies)
    """
    replies = _ping_celery_workers(retries=retries)

    # replies is a dict of {hostname: reply}, convert to a sorted list of [(hostname, reply)]
    reply_tuples = sorted(replies.items(), key=lambda kv: kv[0])
    workers = []

    for worker_name, reply in reply_tuples:
        # the reply is in the following format {'ok': 'pong'}.
        worker = {'name': worker_name, 'available': 'ok' in reply}

        if 'ok' not in reply:
            worker['reason'] = reply.get('error', 'unknown reason')

        workers.append(worker)

    return workers


def status(short=False, worker_ping_retries=2):
    """
    Get status of IIB workers and services that IIB depends on.

    :param bool short: raise an error as soon as any problem is found
    :param int worker_ping_retries: number of retry/retries on ConnectionError when pinging workers
    :return: dict with the following keys:
        "services": list of status info for individual services
        "workers": list of status info for individual workers
    :raises IIBError: if short is True and a problem is found
    """
    services = []

    def add_status(service_name, ok, reason):
        if short and not ok:
            raise IIBError(f'{service_name} unavailable: {reason}')
        service = {'name': service_name, 'available': ok}
        if not ok:
            service['reason'] = reason
        services.append(service)

    add_status('DATABASE', *database_connection_check())
    rabbit_ok, rabbit_reason = rabbitmq_connection_check(get_worker_config().broker_url)
    add_status('RABBITMQ', rabbit_ok, rabbit_reason)

    if rabbit_ok:
        workers = workers_status(retries=worker_ping_retries)
    else:
        workers = []

    any_worker_ok = any(worker['available'] for worker in workers)
    if short and not any_worker_ok:
        raise IIBError('no workers are available')

    return {
        'services': services,
        'workers': workers,
    }
