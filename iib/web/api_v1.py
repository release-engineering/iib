# SPDX-License-Identifier: GPL-3.0-or-later
import flask
from flask_login import login_required

from iib.exceptions import ValidationError
from iib.web import db
from iib.web.models import Request, RequestState, RequestStateMapping
from iib.web.utils import pagination_metadata, str_to_bool
from iib.workers.tasks.placeholder import ping

api_v1 = flask.Blueprint('api_v1', __name__)


@api_v1.route('/test', methods=['GET'])
def get_request():
    ping.delay()
    return 'Test request success!'


@api_v1.route('/builds/<int:request_id>')
def get_build(request_id):
    """
    Retrieve the build request.

    :param int request_id: the request ID that was passed in through the URL.
    :rtype: flask.Response
    :raise NotFound: if the request is not found
    """
    query = Request.query.options(*Request.get_query_options(verbose=True))
    return flask.jsonify(query.get_or_404(request_id).to_json())


@api_v1.route('/builds')
def get_builds():
    """
    Retrieve the paginated build requests.

    :rtype: flask.Response
    """
    state = flask.request.args.get('state')
    verbose = str_to_bool(flask.request.args.get('verbose'))
    max_per_page = flask.current_app.config['IIB_MAX_PER_PAGE']

    query = Request.query.options(*Request.get_query_options(verbose=verbose))
    if state:
        RequestStateMapping.validate_state(state)
        state_int = RequestStateMapping.__members__[state].value
        query = query.join(Request.state)
        query = query.filter(RequestState.state == state_int)

    pagination_query = query.paginate(max_per_page=max_per_page)
    requests = pagination_query.items

    query_params = {}
    if state:
        query_params['state'] = state
    if verbose:
        query_params['verbose'] = verbose

    response = {
        'items': [request.to_json(verbose=verbose) for request in requests],
        'meta': pagination_metadata(pagination_query, **query_params),
    }
    return flask.jsonify(response)


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
