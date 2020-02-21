# SPDX-License-Identifier: GPL-3.0-or-later
import copy

import flask
from flask_login import current_user, login_required
from werkzeug.exceptions import Unauthorized

from iib.exceptions import ValidationError
from iib.web import db
from iib.web.models import Architecture, Image, Operator, Request, RequestState, RequestStateMapping
from iib.web.utils import pagination_metadata, str_to_bool
from iib.workers.tasks.build import handle_add_request, handle_rm_request
from iib.workers.tasks.general import failed_request_callback

api_v1 = flask.Blueprint('api_v1', __name__)


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

    request = Request.from_add_json(payload)
    db.session.add(request)
    db.session.commit()

    args = [
        payload['bundles'],
        payload['binary_image'],
        request.id,
        payload.get('from_index'),
        payload.get('add_arches'),
        payload.get('cnr_token'),
        payload.get('organization'),
    ]
    safe_args = copy.copy(args)
    if payload.get('cnr_token'):
        safe_args[safe_args.index(payload['cnr_token'])] = '*****'

    error_callback = failed_request_callback.s(request.id)
    handle_add_request.apply_async(args=args, link_error=error_callback, argsrepr=repr(safe_args))

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/<int:request_id>', methods=['PATCH'])
@login_required
def patch_request(request_id):
    """
    Modify the given request.

    :param int request_id: the request ID from the URL
    :return: a Flask JSON response
    :rtype: flask.Response
    :raise NotFound: if the request is not found
    :raise ValidationError: if the JSON is invalid
    """
    allowed_users = flask.current_app.config['IIB_WORKER_USERNAMES']
    # current_user.is_authenticated is only ever False when auth is disabled
    if current_user.is_authenticated and current_user.username not in allowed_users:
        raise Unauthorized('This API endpoint is restricted to IIB workers')

    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    if not payload:
        raise ValidationError('At least one key must be specified to update the request')

    valid_keys = {
        'arches',
        'binary_image_resolved',
        'bundle_mapping',
        'from_index_resolved',
        'index_image',
        'state',
        'state_reason',
    }
    invalid_keys = payload.keys() - valid_keys
    if invalid_keys:
        raise ValidationError(
            'The following keys are not allowed: {}'.format(', '.join(invalid_keys))
        )

    for key, value in payload.items():
        if key == 'arches':
            Architecture.validate_architecture_json(value)
        elif key == 'bundle_mapping':
            exc_msg = f'The "{key}" key must be an object with the values as lists of strings'
            if not isinstance(value, dict):
                raise ValidationError(exc_msg)
            for v in value.values():
                if not isinstance(v, list) or any(not isinstance(s, str) for s in v):
                    raise ValidationError(exc_msg)
        elif not value or not isinstance(value, str):
            raise ValidationError(f'The value for "{key}" must be a non-empty string')

    if 'state' in payload and 'state_reason' not in payload:
        raise ValidationError('The "state_reason" key is required when "state" is supplied')
    elif 'state_reason' in payload and 'state' not in payload:
        raise ValidationError('The "state" key is required when "state_reason" is supplied')

    request = Request.query.get_or_404(request_id)
    if 'state' in payload and 'state_reason' in payload:
        RequestStateMapping.validate_state(payload['state'])
        new_state = payload['state']
        new_state_reason = payload['state_reason']
        # This is to protect against a Celery task getting executed twice and setting the
        # state each time
        if request.state.state == new_state and request.state.state_reason == new_state_reason:
            flask.current_app.logger.info('Not adding a new state since it matches the last state')
        else:
            request.add_state(new_state, new_state_reason)

    for key in ('binary_image_resolved', 'from_index_resolved', 'index_image'):
        if key not in payload:
            continue
        key_value = payload.get(key, None)
        key_object = Image.get_or_create(key_value)
        # SQLAlchemy will not add the object to the database if it's already present
        setattr(request, key, key_object)

    for arch in payload.get('arches', []):
        request.add_architecture(arch)

    for operator, bundles in payload.get('bundle_mapping', {}).items():
        operator_img = Operator.get_or_create(operator)
        for bundle in bundles:
            bundle_img = Image.get_or_create(bundle)
            bundle_img.operator = operator_img

    db.session.commit()

    if current_user.is_authenticated:
        flask.current_app.logger.info(
            'The user %s patched request %d', current_user.username, request.id
        )
    else:
        flask.current_app.logger.info('An anonymous user patched request %d', request.id)

    return flask.jsonify(request.to_json()), 200


@api_v1.route('/builds/rm', methods=['POST'])
@login_required
def rm_operators():
    """
    Submit a request to remove operators from an index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = Request.from_remove_json(payload)
    db.session.add(request)
    db.session.commit()

    error_callback = failed_request_callback.s(request.id)
    handle_rm_request.apply_async(
        args=[
            payload['operators'],
            payload['binary_image'],
            request.id,
            payload['from_index'],
            payload.get('add_arches'),
        ],
        link_error=error_callback,
    )

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201
