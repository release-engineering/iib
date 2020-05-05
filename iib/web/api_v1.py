# SPDX-License-Identifier: GPL-3.0-or-later
import copy

import flask
from flask_login import current_user, login_required
from sqlalchemy.orm import with_polymorphic
from werkzeug.exceptions import Forbidden

from iib.exceptions import ValidationError
from iib.web import db, messaging
from iib.web.models import (
    Architecture,
    Batch,
    Image,
    Operator,
    Request,
    RequestAdd,
    RequestRegenerateBundle,
    RequestRm,
    RequestState,
    RequestStateMapping,
    get_request_query_options,
)
from iib.web.utils import pagination_metadata, str_to_bool
from iib.workers.tasks.build import (
    handle_add_request,
    handle_regenerate_bundle_request,
    handle_rm_request,
)
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
    # Create an alias class to load the polymorphic classes
    poly_request = with_polymorphic(Request, '*')
    query = poly_request.query.options(*get_request_query_options(verbose=True))
    return flask.jsonify(query.get_or_404(request_id).to_json())


@api_v1.route('/builds')
def get_builds():
    """
    Retrieve the paginated build requests.

    :rtype: flask.Response
    """
    batch_id = flask.request.args.get('batch')
    state = flask.request.args.get('state')
    verbose = str_to_bool(flask.request.args.get('verbose'))
    max_per_page = flask.current_app.config['IIB_MAX_PER_PAGE']

    # Create an alias class to load the polymorphic classes
    poly_request = with_polymorphic(Request, '*')
    query = poly_request.query.options(*get_request_query_options(verbose=verbose))
    if state:
        RequestStateMapping.validate_state(state)
        state_int = RequestStateMapping.__members__[state].value
        query = query.join(Request.state)
        query = query.filter(RequestState.state == state_int)

    if batch_id is not None:
        batch_id = Batch.validate_batch(batch_id)
        query = query.filter_by(batch_id=batch_id)

    pagination_query = query.order_by(Request.id.desc()).paginate(max_per_page=max_per_page)
    requests = pagination_query.items

    query_params = {}
    if state:
        query_params['state'] = state
    if verbose:
        query_params['verbose'] = verbose
    if batch_id:
        query_params['batch'] = batch_id

    response = {
        'items': [request.to_json(verbose=verbose) for request in requests],
        'meta': pagination_metadata(pagination_query, **query_params),
    }
    return flask.jsonify(response)


def _should_force_overwrite():
    """
    Determine if the ``overwrite_from_index`` parameter should be forced.

    This is for clients that require this functionality but do not currently use the
    ``overwrite_from_index`` parameter already.

    :return: the boolean that determines if the overwrite should be forced
    :rtype: bool
    """
    # current_user.is_authenticated is only ever False when auth is disabled
    if not current_user.is_authenticated:
        return False
    privileged_users = flask.current_app.config['IIB_PRIVILEGED_USERNAMES']
    force_ovewrite = flask.current_app.config['IIB_FORCE_OVERWRITE_FROM_INDEX']

    should_force = current_user.username in privileged_users and force_ovewrite
    if should_force:
        flask.current_app.logger.info(
            'The "overwrite_from_index" parameter is being forced to True'
        )

    return should_force


def _get_user_queue():
    """
    Return the name of the celery task queue mapped to the current user.

    :return: queue name to be used or None if the default queue should be used
    :rtype: str or None
    """
    # current_user.is_authenticated is only ever False when auth is disabled
    if not current_user.is_authenticated:
        return

    return flask.current_app.config['IIB_USER_TO_QUEUE'].get(current_user.username)


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

    request = RequestAdd.from_json(payload)
    db.session.add(request)
    db.session.commit()

    celery_queue = _get_user_queue()
    args = [
        payload['bundles'],
        payload['binary_image'],
        request.id,
        payload.get('from_index'),
        payload.get('add_arches'),
        payload.get('cnr_token'),
        payload.get('organization'),
        _should_force_overwrite() or payload.get('overwrite_from_index'),
        flask.current_app.config['IIB_GREENWAVE_CONFIG'].get(celery_queue),
    ]
    safe_args = copy.copy(args)
    if payload.get('cnr_token'):
        safe_args[safe_args.index(payload['cnr_token'])] = '*****'

    error_callback = failed_request_callback.s(request.id)
    handle_add_request.apply_async(
        args=args, link_error=error_callback, argsrepr=repr(safe_args), queue=celery_queue
    )

    messaging.send_message_for_state_change(request, new_batch_msg=True)
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
    :raise Forbidden: If the user trying to patch a request is not an IIB worker
    :raise NotFound: if the request is not found
    :raise ValidationError: if the JSON is invalid
    """
    allowed_users = flask.current_app.config['IIB_WORKER_USERNAMES']
    # current_user.is_authenticated is only ever False when auth is disabled
    if current_user.is_authenticated and current_user.username not in allowed_users:
        raise Forbidden('This API endpoint is restricted to IIB workers')

    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    if not payload:
        raise ValidationError('At least one key must be specified to update the request')

    request = Request.query.get_or_404(request_id)

    invalid_keys = payload.keys() - request.get_mutable_keys()
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

    state_updated = False
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
            state_updated = True

    image_keys = (
        'binary_image_resolved',
        'bundle_image',
        'from_bundle_image_resolved',
        'from_index_resolved',
        'index_image',
    )
    for key in image_keys:
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

    if state_updated:
        messaging.send_message_for_state_change(request)

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

    request = RequestRm.from_json(payload)
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
            _should_force_overwrite() or payload.get('overwrite_from_index'),
        ],
        link_error=error_callback,
        queue=_get_user_queue(),
    )

    messaging.send_message_for_state_change(request, new_batch_msg=True)
    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/regenerate-bundle', methods=['POST'])
@login_required
def regenerate_bundle():
    """
    Submit a request to regenerate an operator bundle image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestRegenerateBundle.from_json(payload)
    db.session.add(request)
    db.session.commit()

    error_callback = failed_request_callback.s(request.id)
    handle_regenerate_bundle_request.apply_async(
        args=[payload['from_bundle_image'], payload.get('organization'), request.id],
        link_error=error_callback,
        queue=_get_user_queue(),
    )

    messaging.send_message_for_state_change(request, new_batch_msg=True)
    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/regenerate-bundle-batch', methods=['POST'])
@login_required
def regenerate_bundle_batch():
    """
    Submit a batch of requests to regenerate operator bundle images.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payloads = flask.request.get_json()
    if not isinstance(payloads, list) or not payloads:
        raise ValidationError('The input data must be a non-empty JSON array')

    batch = Batch()
    db.session.add(batch)
    requests = []
    # Iterate through all the payloads and verify that the requests are valid before committing them
    # and scheduling the tasks
    for payload in payloads:
        try:
            request = RequestRegenerateBundle.from_json(payload, batch)
        except ValidationError as e:
            # Rollback the transaction if any of the payloads are invalid
            db.session.rollback()
            raise ValidationError(
                f'{str(e).rstrip(".")}. This occurred on the request in '
                f'index {payloads.index(payload)}.'
            )
        db.session.add(request)
        requests.append(request)

    db.session.commit()
    messaging.send_messages_for_new_batch_of_requests(requests)

    request_jsons = []
    # This list will be used for the log message below and avoids the need of having to iterate
    # through the list of requests another time
    request_id_strs = []
    for payload, request in zip(payloads, requests):
        request_jsons.append(request.to_json())
        request_id_strs.append(str(request.id))

        error_callback = failed_request_callback.s(request.id)
        handle_regenerate_bundle_request.apply_async(
            args=[payload['from_bundle_image'], payload.get('organization'), request.id],
            link_error=error_callback,
            queue=_get_user_queue(),
        )

    flask.current_app.logger.debug(
        'Successfully scheduled the batch %d with requests: %s',
        batch.id,
        ', '.join(request_id_strs),
    )
    return flask.jsonify(request_jsons), 201
