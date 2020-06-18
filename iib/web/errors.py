# SPDX-License-Identifier: GPL-3.0-or-later
import kombu.exceptions
from flask import jsonify, current_app
from werkzeug.exceptions import HTTPException

from iib.exceptions import IIBError, ValidationError
from iib.web import messaging, db


def json_error(error):
    """
    Convert exceptions to JSON responses.

    :param Exception error: an Exception to convert to JSON
    :return: a Flask JSON response
    :rtype: flask.Response
    """
    if isinstance(error, HTTPException):
        if error.code == 404:
            msg = 'The requested resource was not found'
        else:
            msg = error.description
        response = jsonify({'error': msg})
        response.status_code = error.code
    else:
        status_code = 500
        msg = str(error)
        if isinstance(error, ValidationError):
            status_code = 400
        elif isinstance(error, kombu.exceptions.KombuError):
            msg = 'Failed to connect to the broker to schedule a task'

        response = jsonify({'error': msg})
        response.status_code = status_code
    return response


def handle_broker_error(request):
    """
    Handle broker errors by setting the request as failed and raise an IIBError exception.

    :param Request request: Request which will be set as failed
    :raises IIBError: Raises IIBError exception after setting request to failed state
    """
    request.add_state('failed', 'The scheduling of the request failed')
    db.session.commit()
    messaging.send_message_for_state_change(request)

    error_message = f'The scheduling of the build request with ID {request.id} failed'
    current_app.logger.exception(error_message)

    raise IIBError(error_message)


def handle_broker_batch_error(requests):
    """
    Handle broker errors by setting all requests as failed and raise an IIBError exception.

    :param list requests: list of all requests that should be marked as failed
    :raises IIBError: Raises IIBError exception after setting all requests to failed state
    """
    failed_ids = []
    for req in requests:
        failed_ids.append(str(req.id))
        req.add_state('failed', 'The scheduling of the request failed')
        messaging.send_message_for_state_change(req)

    db.session.commit()
    error_message = f'The scheduling of the build requests with IDs {", ".join(failed_ids)} failed'
    current_app.logger.exception(error_message)

    raise IIBError(error_message)
