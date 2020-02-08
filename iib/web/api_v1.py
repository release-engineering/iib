# SPDX-License-Identifier: GPL-3.0-or-later
import flask
from flask_login import login_required

from iib.exceptions import ValidationError
from iib.web import db
from iib.web.models import Request
from iib.workers.tasks.placeholder import ping

api_v1 = flask.Blueprint('api_v1', __name__)


@api_v1.route('/test', methods=['GET'])
def get_request():
    ping.delay()
    return 'Test request success!'


@api_v1.route('/builds/add', methods=['POST'])
@login_required
def add_bundles():
    """
    Submit a request to add operator bundles to an index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = Request.from_json(payload)
    db.session.add(request)
    db.session.commit()

    # TODO: call the celery task to add the operator bundle to the index image

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201
