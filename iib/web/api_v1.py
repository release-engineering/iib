# SPDX-License-Identifier: GPL-3.0-or-later
import copy
import logging
import os
from datetime import datetime
import time

import flask
import kombu
from flask_login import current_user, login_required
from sqlalchemy.orm import aliased, with_polymorphic
from sqlalchemy.sql import text
from sqlalchemy import or_
from werkzeug.exceptions import Forbidden, Gone, NotFound
from typing import Any, cast, Dict, List, Optional, Tuple, Union

from iib.common.tracing import instrument_tracing
from iib.exceptions import IIBError, ValidationError
from iib.web import db, messaging
from iib.web.errors import handle_broker_error, handle_broker_batch_error
from iib.web.models import (
    Architecture,
    Batch,
    Image,
    Operator,
    Request,
    RequestAdd,
    RequestFbcOperations,
    RequestMergeIndexImage,
    RequestRecursiveRelatedBundles,
    RequestRegenerateBundle,
    RequestRm,
    RequestState,
    RequestStateMapping,
    get_request_query_options,
    RequestTypeMapping,
    RequestCreateEmptyIndex,
    User,
)
from iib.web.s3_utils import get_object_from_s3_bucket
from botocore.response import StreamingBody
from iib.web.utils import pagination_metadata, str_to_bool
from iib.workers.tasks.build import (
    handle_add_request,
    handle_rm_request,
)
from iib.workers.tasks.build_fbc_operations import handle_fbc_operation_request
from iib.workers.tasks.build_recursive_related_bundles import (
    handle_recursive_related_bundles_request,
)
from iib.workers.tasks.build_regenerate_bundle import handle_regenerate_bundle_request
from iib.workers.tasks.build_merge_index_image import handle_merge_request
from iib.workers.tasks.build_create_empty_index import handle_create_empty_index_request
from iib.workers.tasks.general import failed_request_callback
from iib.web.iib_static_types import (
    AddRequestPayload,
    AddRmBatchPayload,
    CreateEmptyIndexPayload,
    FbcOperationRequestPayload,
    MergeIndexImagesPayload,
    PayloadTypesUnion,
    RecursiveRelatedBundlesRequestPayload,
    RegenerateBundleBatchPayload,
    RegenerateBundlePayload,
    RmRequestPayload,
)

api_v1 = flask.Blueprint('api_v1', __name__)


def _get_rm_args(
    payload: RmRequestPayload,
    request: Request,
    overwrite_from_index: bool,
) -> List[Union[str, List[str], Dict[str, str], bool, None]]:
    """
    Generate arguments for remove request.

    :param RmRequestPayload payload: Payload from the remove request
    :param Request request: request saved in the database
    :param bool overwrite_from_index: determines if the overwrite should be forced
    :return: List with remove arguments
    :rtype: list
    """
    return [
        payload['operators'],
        request.id,
        payload['from_index'],
        payload.get('binary_image'),
        payload.get('add_arches'),
        overwrite_from_index,
        payload.get('overwrite_from_index_token'),
        request.distribution_scope,
        flask.current_app.config['IIB_BINARY_IMAGE_CONFIG'],
        payload.get('build_tags', []),
    ]


def _get_add_args(
    payload: AddRequestPayload,
    request: Request,
    overwrite_from_index: bool,
    celery_queue: Optional[str],
) -> List[Any]:
    """
    Generate arguments for add request.

    :param AddRequestPayload payload: Payload from the add request
    :param Request request: request saved in the database
    :param bool overwrite_from_index: determines if the overwrite should be forced
    :param str celery_queue: name of celery queue
    :return: List with add arguments
    :rtype: list
    """
    return [
        payload.get('bundles', []),
        request.id,
        payload.get('binary_image'),
        payload.get('from_index'),
        payload.get('add_arches'),
        payload.get('cnr_token'),
        payload.get('organization'),
        payload.get('force_backport'),
        overwrite_from_index,
        payload.get('overwrite_from_index_token'),
        request.distribution_scope,
        flask.current_app.config['IIB_GREENWAVE_CONFIG'].get(celery_queue),
        flask.current_app.config['IIB_BINARY_IMAGE_CONFIG'],
        payload.get('deprecation_list', []),
        payload.get('build_tags', []),
        payload.get('graph_update_mode'),
        payload.get('check_related_images', False),
    ]


def _get_safe_args(
    args: List[Any],
    payload: PayloadTypesUnion,
) -> List[Union[str, List[str], bool, Dict[str, str]]]:
    """
    Generate arguments that are safe to print to stdout or log.

    :param list args: arguments for each api, that are not safe
    :param PayloadTypesUnion payload: Payload from the IIB request
    :return: List with safe to print arguments
    :rtype: list
    """
    safe_args = copy.copy(args)

    if payload.get('cnr_token'):
        safe_args[safe_args.index(payload['cnr_token'])] = '*****'  # type: ignore
    if payload.get('overwrite_from_index_token'):
        safe_args[safe_args.index(payload['overwrite_from_index_token'])] = '*****'  # type: ignore
    if payload.get('overwrite_target_index_token'):
        safe_args[
            safe_args.index(payload['overwrite_target_index_token'])  # type: ignore
        ] = '*****'
    if payload.get('registry_auths'):
        safe_args[safe_args.index(payload['registry_auths'])] = '*****'  # type: ignore

    return safe_args


def get_artifact_file_from_s3_bucket(
    s3_key_prefix: str,
    s3_file_name: str,
    request_id: int,
    request_temp_data_expiration_date: datetime,
    s3_bucket_name: str,
) -> StreamingBody:
    """
    It's a helper function to get artifact file from S3 bucket.

    :param str s3_key_prefix: the logical location of the file in the S3 bucket
    :param str s3_file_name: the name of the file in S3 bucket
    :param int request_id: the request ID of the request in question
    :param datetime request_temp_data_expiration_date: expiration date of the temporary data
        for the request in question
    :param str s3_bucket_name: the name of the S3 bucket in AWS
    :raise NotFound: if the request is not found or there are no logs for the request
    :raise Gone: if the logs for the build request have been removed due to expiration
    :rtype: botocore.response.StreamingBody
    :return: streaming body of the file fetched from AWS S3 bucket
    """
    artifact_file = get_object_from_s3_bucket(s3_key_prefix, s3_file_name, s3_bucket_name)
    if artifact_file:
        return artifact_file

    expired = request_temp_data_expiration_date < datetime.utcnow()
    if expired:
        raise Gone(f'The data for the build request {request_id} no longer exist')
    raise NotFound()


def _get_unique_bundles(bundles: List[str]) -> List[str]:
    """
    Return list with unique bundles.

    :param list bundles: bundles given in payload from original request
    :return: list of unique bundles preserving order (python 3.6+)
    :rtype: list
    """
    if not bundles:
        return bundles

    # `dict` is preserving order of inserted keys since Python 3.6.
    # Keys in dictionary are behaving as a set() therefore can not have same key twice.
    # This will create dictionary where keys are taken from `bundles` using `dict.fromkeys()`
    # After that we have dictionary with unique keys with same order as it is in `bundles`.
    # Last step is to convert the keys from this dictionary to list using `list()`
    unique_bundles = list(dict.fromkeys(bundles).keys())

    if len(unique_bundles) != len(bundles):
        duplicate_bundles = copy.copy(bundles)
        for bundle in unique_bundles:
            duplicate_bundles.remove(bundle)

        flask.current_app.logger.info(
            f'Removed duplicate bundles from request: {duplicate_bundles}'
        )
    return unique_bundles


@api_v1.route('/builds/<int:request_id>')
@instrument_tracing(span_name="web.api_v1.get_build")
def get_build(request_id: int) -> flask.Response:
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


@api_v1.route('/builds/<int:request_id>/logs')
@instrument_tracing(span_name="web.api_v1.get_build_logs")
def get_build_logs(request_id: int) -> flask.Response:
    """
    Retrieve the logs for the build request.

    :param int request_id: the request ID that was passed in through the URL.
    :rtype: flask.Response
    :raise NotFound: if the request is not found or there are no logs for the request
    :raise Gone: if the logs for the build request have been removed due to expiration
    :raise ValidationError: if the request has not completed yet
    """
    request_log_dir = flask.current_app.config['IIB_REQUEST_LOGS_DIR']
    s3_bucket_name = flask.current_app.config['IIB_AWS_S3_BUCKET_NAME']
    if not s3_bucket_name and not request_log_dir:
        raise NotFound()

    request = Request.query.get_or_404(request_id)

    finalized = request.state.state_name in RequestStateMapping.get_final_states()
    if not finalized:
        raise ValidationError(
            f'The request {request_id} is not complete yet.'
            ' logs will be available once the request is complete.'
        )

    # If S3 bucket is configured, fetch the log file from the S3 bucket.
    # Else, check if logs are stored on the system itself and return them.
    # Otherwise, raise an IIBError.
    if s3_bucket_name:
        log_file = get_artifact_file_from_s3_bucket(
            'request_logs',
            f'{request_id}.log',
            request_id,
            request.temporary_data_expiration,
            s3_bucket_name,
        )
        return flask.Response(log_file.read(), mimetype='text/plain')

    local_log_file_path = os.path.join(request_log_dir, f'{request_id}.log')
    if not os.path.exists(local_log_file_path):
        expired = request.temporary_data_expiration < datetime.utcnow()
        if expired:
            raise Gone(f'The logs for the build request {request_id} no longer exist')
        flask.current_app.logger.warning(
            ' Please make sure either an S3 bucket is configured or the logs are'
            ' stored locally in a directory by specifying IIB_REQUEST_LOGS_DIR'
        )
        raise IIBError('IIB is done processing the request and could not find logs.')

    with open(local_log_file_path) as f:
        return flask.Response(f.read(), mimetype='text/plain')


@api_v1.route('/builds/<int:request_id>/related_bundles')
@instrument_tracing(span_name="web.api_v1.get_related_bundles")
def get_related_bundles(request_id: int) -> flask.Response:
    """
    Retrieve the related bundle images from the bundle CSV for a regenerate-bundle request.

    :param int request_id: the request ID that was passed in through the URL.
    :rtype: flask.Response
    :raise NotFound: if the request is not found or there are no related bundles for the request
    :raise Gone: if the related bundles for the build request have been removed due to expiration
    :raise ValidationError: if the request is of invalid type or is not completed yet
    """
    request_related_bundles_dir = flask.current_app.config['IIB_REQUEST_RELATED_BUNDLES_DIR']
    s3_bucket_name = flask.current_app.config['IIB_AWS_S3_BUCKET_NAME']
    if not s3_bucket_name and not request_related_bundles_dir:
        raise NotFound()

    request = Request.query.get_or_404(request_id)
    if request.type != RequestTypeMapping.regenerate_bundle.value:
        raise ValidationError(
            f'The request {request_id} is of type {request.type_name}. '
            'This endpoint is only valid for requests of type regenerate-bundle.'
        )

    finalized = request.state.state_name in RequestStateMapping.get_final_states()
    if not finalized:
        raise ValidationError(
            f'The request {request_id} is not complete yet.'
            ' related_bundles will be available once the request is complete.'
        )

    # If S3 bucket is configured, fetch the related bundles file from the S3 bucket.
    # Else, check if related bundles are stored on the system itself and return them.
    # Otherwise, raise an IIBError.
    if s3_bucket_name:
        log_file = get_artifact_file_from_s3_bucket(
            'related_bundles',
            f'{request_id}_related_bundles.json',
            request_id,
            request.temporary_data_expiration,
            s3_bucket_name,
        )
        return flask.Response(log_file.read(), mimetype='application/json')

    related_bundles_file_path = os.path.join(
        request_related_bundles_dir, f'{request_id}_related_bundles.json'
    )
    if not os.path.exists(related_bundles_file_path):
        expired = request.temporary_data_expiration < datetime.utcnow()
        if expired:
            raise Gone(f'The related_bundles for the build request {request_id} no longer exist')
        if request.organization:
            raise IIBError(
                'IIB is done processing the request and cannot find related_bundles. Please make '
                f'sure the iib_organization_customizations for organization {request.organization}'
                ' has related_bundles customization type set'
            )
        flask.current_app.logger.warning(
            ' Please make sure either an S3 bucket is configured or the logs are'
            ' stored locally in a directory by specifying IIB_REQUEST_LOGS_DIR'
        )
        raise IIBError('IIB is done processing the request and could not find related_bundles.')

    with open(related_bundles_file_path) as f:
        return flask.Response(f.read(), mimetype='application/json')


@api_v1.route('/builds')
@instrument_tracing(span_name="web.api_v1.get_builds")
def get_builds() -> flask.Response:
    """
    Retrieve the paginated build requests.

    :rtype: flask.Response
    """
    batch_id: Optional[str] = flask.request.args.get('batch')
    state = flask.request.args.get('state')
    verbose = str_to_bool(flask.request.args.get('verbose'))
    max_per_page = flask.current_app.config['IIB_MAX_PER_PAGE']
    request_type = flask.request.args.get('request_type')
    user = flask.request.args.get('user')
    index_image = flask.request.args.get('index_image')
    from_index = flask.request.args.get('from_index')
    from_index_startswith = flask.request.args.get('from_index_startswith')
    query_params = {}

    # Create an alias class to load the polymorphic classes
    poly_request = with_polymorphic(Request, '*')
    query = poly_request.query.options(*get_request_query_options(verbose=verbose))
    if state:
        query_params['state'] = state
        RequestStateMapping.validate_state(state)
        state_int = RequestStateMapping.__members__[state].value
        query = query.join(Request.state)
        query = query.filter(RequestState.state == state_int)

    if batch_id is not None:
        query_params['batch'] = batch_id
        batch_id_checked: int = Batch.validate_batch(batch_id)
        query = query.filter_by(batch_id=batch_id_checked)

    if request_type:
        query_params['request_type'] = request_type
        RequestTypeMapping.validate_type(request_type)
        request_type = request_type.replace('-', '_')
        request_type_int = RequestTypeMapping.__members__[request_type].value
        query = query.filter(Request.type == request_type_int)

    if user:
        # join with the user table and then filter on username
        # request table only has the user_id
        query_params['user'] = user
        query = query.join(Request.user).filter(User.username == user)

    if index_image or from_index or from_index_startswith:
        # https://sqlalche.me/e/20/xaj2 - Create aliases for self-join (Sqlalchemy 2.0)
        request_create_empty_index_alias = aliased(RequestCreateEmptyIndex, flat=True)
        request_add_alias = aliased(RequestAdd, flat=True)
        request_rm_alias = aliased(RequestRm, flat=True)
        request_fbc_operations_alias = aliased(RequestFbcOperations, flat=True)

        # join with the Request* tables to get the response as image_ids are stored there
        query = (
            query.outerjoin(
                request_create_empty_index_alias,
                Request.id == request_create_empty_index_alias.id,
            )
            .outerjoin(request_add_alias, Request.id == request_add_alias.id)
            .outerjoin(request_rm_alias, Request.id == request_rm_alias.id)
            .outerjoin(request_fbc_operations_alias, Request.id == request_fbc_operations_alias.id)
        )

        if from_index:
            query_params['from_index'] = from_index
            # Get the image id of the image to be searched
            from_index_result = Image.query.filter_by(pull_specification=from_index).first()
            if not from_index_result:
                # if from_index is not found in image table, then raise an error
                raise ValidationError(f'from_index {from_index} is not a valid index image')

            query = query.filter(
                or_(
                    request_create_empty_index_alias.from_index_id == from_index_result.id,
                    request_add_alias.from_index_id == from_index_result.id,
                    request_rm_alias.from_index_id == from_index_result.id,
                    request_fbc_operations_alias.from_index_id == from_index_result.id,
                )
            )

        if from_index_startswith:
            query_params['from_index_startswith'] = from_index_startswith
            from_index_startswith_results = Image.query.filter(
                Image.pull_specification.startswith(from_index_startswith)
            ).all()

            if not from_index_startswith_results:
                # if index_image is not found in image table, then raise an error
                raise ValidationError(
                    f'Can\'t find any from_index starting with {from_index_startswith}'
                )

            # Get id of the images to be searched
            from_index_result_ids = [fir.id for fir in from_index_startswith_results]

            query = query.filter(
                or_(
                    request_create_empty_index_alias.from_index_id.in_(from_index_result_ids),
                    request_add_alias.from_index_id.in_(from_index_result_ids),
                    request_rm_alias.from_index_id.in_(from_index_result_ids),
                    request_fbc_operations_alias.from_index_id.in_(from_index_result_ids),
                )
            )

        if index_image:
            # Get the image id of the image to be searched for
            image_result = Image.query.filter_by(pull_specification=index_image).first()
            if not image_result:
                # if index_image is not found in image table, then raise an error
                raise ValidationError(f'{index_image} is not a valid index image')

            request_merge_index_image_alias = aliased(RequestMergeIndexImage, flat=True)
            query_params['index_image'] = index_image

            # join with the Request* tables to get the response as image_ids are stored there
            query = query.outerjoin(
                request_merge_index_image_alias,
                Request.id == request_merge_index_image_alias.id,
            )

            query = query.filter(
                or_(
                    request_create_empty_index_alias.index_image_id == image_result.id,
                    request_add_alias.index_image_id == image_result.id,
                    request_merge_index_image_alias.index_image_id == image_result.id,
                    request_rm_alias.index_image_id == image_result.id,
                    request_fbc_operations_alias.index_image_id == image_result.id,
                )
            )

    pagination_query = query.order_by(Request.id.desc()).paginate(max_per_page=max_per_page)
    requests = pagination_query.items

    response = {
        'items': [request.to_json(verbose=verbose) for request in requests],
        'meta': pagination_metadata(pagination_query, **query_params),
    }
    return flask.jsonify(response)


@api_v1.route('/healthcheck')
@instrument_tracing(span_name="web.api_v1.get_healthcheck")
def get_healthcheck() -> flask.Response:
    """
    Respond to a health check.

    :rtype: flask.Response
    :return: json object representing the health of IIB
    :raises IIBError: if the database connection fails
    """
    # Test DB connection
    try:
        with db.engine.connect() as connection:
            connection.execute(text('SELECT 1'))
    except Exception:
        flask.current_app.logger.exception('DB test failed.')
        raise IIBError('Database health check failed.')

    return flask.jsonify({'status': 'Health check OK'})


def _get_user_queue(
    serial: Optional[bool] = False, from_index_pull_spec: Union[str, None] = None
) -> Optional[str]:
    """
    Return the name of the celery task queue mapped to the current user.

    :param bool serial: whether or not the task must run serially
    :param str from_index_pull_spec: index image pull-spec
    :return: queue name to be used or None if the default queue should be used
    :rtype: str or None
    """
    # current_user.is_authenticated is only ever False when auth is disabled
    if not current_user.is_authenticated:
        return None

    username = current_user.username
    if serial:
        labeled_username = f'SERIAL:{username}'
    else:
        labeled_username = f'PARALLEL:{username}'

    index_queue = flask.current_app.config['IIB_USER_TO_QUEUE'].get(labeled_username)
    if not index_queue:
        index_queue = flask.current_app.config['IIB_USER_TO_QUEUE'].get(username)

    if index_queue is None:
        return index_queue

    if isinstance(index_queue, str):
        # keep original behavior for IIB_USER_TO_QUEUE of type dict[str,str])
        return index_queue

    if isinstance(index_queue, dict):
        queue_default = index_queue.get('all')

        queue = index_queue.get(from_index_pull_spec)
        if not queue:
            logging.debug(
                'Queue for pull spec %s is not defined. Using default queue (all): %s',
                from_index_pull_spec,
                queue_default,
            )
            return queue_default

        return queue

    logging.warning('Unsupported type of IIB_USER_TO_QUEUE: %s', index_queue)
    return None


@api_v1.route('/builds/add', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.add_bundles")
def add_bundles() -> Tuple[flask.Response, int]:
    """
    Submit a request to add operator bundles to an index image.

    Note: Any duplicate bundle will be removed from payload.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: AddRequestPayload = cast(AddRequestPayload, flask.request.get_json())
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    # Only run `_get_unique_bundles` if it is a list. If it's not, `from_json`
    # will raise an error to the user.
    if payload.get('bundles') and isinstance(payload['bundles'], list):
        payload['bundles'] = _get_unique_bundles(payload['bundles'])

    request = RequestAdd.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    overwrite_from_index = payload.get('overwrite_from_index', False)
    from_index_pull_spec = request.from_index.pull_specification if request.from_index else None
    celery_queue = _get_user_queue(
        serial=overwrite_from_index, from_index_pull_spec=from_index_pull_spec
    )
    args = _get_add_args(payload, request, overwrite_from_index, celery_queue)
    safe_args = _get_safe_args(args, payload)
    error_callback = failed_request_callback.s(request.id)
    if current_user.is_authenticated:
        args.append(current_user.username)

    try:
        handle_add_request.apply_async(
            args=args,
            link_error=error_callback,
            argsrepr=repr(safe_args),
            queue=celery_queue,
            headers={'traceparent': flask.request.headers.get('traceparent')},
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/<int:request_id>', methods=['PATCH'])
@login_required
@instrument_tracing(span_name="web.api_v1.patch_request")
def patch_request(request_id: int) -> Tuple[flask.Response, int]:
    """
    Modify the given request.

    :param int request_id: the request ID from the URL
    :return: a Flask JSON response
    :rtype: flask.Response
    :raise Forbidden: If the user trying to patch a request is not an IIB worker
    :raise NotFound: if the request is not found
    :raise ValidationError: if the JSON is invalid
    """
    overall_start_time = time.time()
    allowed_users = flask.current_app.config['IIB_WORKER_USERNAMES']
    # current_user.is_authenticated is only ever False when auth is disabled
    if current_user.is_authenticated and current_user.username not in allowed_users:
        raise Forbidden('This API endpoint is restricted to IIB workers')

    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    if not payload:
        raise ValidationError('At least one key must be specified to update the request')

    start_time = time.time()
    request = Request.query.get_or_404(request_id)
    flask.current_app.logger.debug(
        f'Time for web/api_v1/559:Request.query.get_or_404(): {time.time() - start_time}'
        f' time from start: {time.time() - overall_start_time}'
    )

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
        elif key == 'recursive_related_bundles':
            if not isinstance(value, list):
                exc_msg = f'The value for "{key}" must be a list of non-empty strings'
                raise ValidationError(exc_msg)
            for bundle in value:
                if not isinstance(bundle, str):
                    raise ValidationError(exc_msg)
        elif key == 'bundle_replacements':
            exc_msg = f'The "{key}" key must be a dictionary object mapping from strings to strings'
            if not isinstance(value, dict):
                raise ValidationError(exc_msg)
            for k, v in value.items():
                if not isinstance(v, str) or not isinstance(k, str):
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
            start_time = time.time()
            request.add_state(new_state, new_state_reason)
            flask.current_app.logger.debug(
                f'Time for web/api_v1/614:request.add_state(): {time.time() - start_time}'
                f' time from start: {time.time() - overall_start_time}'
            )
            state_updated = True

    if 'omps_operator_version' in payload:
        # `omps_operator_version` is defined in RequestAdd only
        if request.type == RequestTypeMapping.add.value:
            start_time = time.time()
            request_add = RequestAdd.query.get(request_id)
            flask.current_app.logger.debug(
                f'Time for web/api_v1/625:RequestAdd.query.get(): {time.time() - start_time}'
                f' time from start: {time.time() - overall_start_time}'
            )
            request_add.omps_operator_version = payload.get('omps_operator_version')
        else:
            raise ValidationError(
                f'Request {request_id} is type of "{RequestTypeMapping.pretty(request.type)}" '
                f'request and does not support setting "omps_operator_version"'
            )

    image_keys = (
        'binary_image',
        'binary_image_resolved',
        'bundle_image',
        'from_bundle_image_resolved',
        'from_index_resolved',
        'fbc_fragment',
        'fbc_fragment_resolved',
        'index_image',
        'index_image_resolved',
        'internal_index_image_copy',
        'internal_index_image_copy_resolved',
        'parent_bundle_image_resolved',
        'source_from_index_resolved',
        'target_index_resolved',
    )
    start_time = time.time()
    for key in image_keys:
        if key not in payload:
            continue
        key_value = payload.get(key, None)
        key_object = Image.get_or_create(key_value)
        # SQLAlchemy will not add the object to the database if it's already present
        setattr(request, key, key_object)
    flask.current_app.logger.debug(
        f'Time for web/api_v1/661:key updates: {time.time() - start_time}'
        f' time from start: {time.time() - overall_start_time}'
    )

    start_time = time.time()
    for arch in payload.get('arches', []):
        request.add_architecture(arch)
    flask.current_app.logger.debug(
        f'Time for web/api_v1/web/api_v1/668:request arch update: {time.time() - start_time}'
        f' time from start: {time.time() - overall_start_time}'
    )

    start_time = time.time()
    for operator, bundles in payload.get('bundle_mapping', {}).items():
        operator_img = Operator.get_or_create(operator)
        for bundle in bundles:
            bundle_img = Image.get_or_create(bundle)
            bundle_img.operator = operator_img
    flask.current_app.logger.debug(
        f'Time for web/api_v1/675:bundle mapping process: {time.time() - start_time}'
        f' time from start: {time.time() - overall_start_time}'
    )

    if 'distribution_scope' in payload:
        request.distribution_scope = payload['distribution_scope']

    start_time = time.time()
    db.session.commit()
    flask.current_app.logger.debug(
        f'Time for web/api_v1/689:db commit: {time.time()-start_time}'
        f' time from start: {time.time() - overall_start_time}'
    )

    if state_updated:
        start_time = time.time()
        messaging.send_message_for_state_change(request)
        flask.current_app.logger.debug(
            f'Time for web/api_v1/697:send_message_for_state_change(): {time.time() - start_time},'
            f' time from start: {time.time()-overall_start_time}'
        )

    if current_user.is_authenticated:
        flask.current_app.logger.info(
            'The user %s patched request %d', current_user.username, request.id
        )
    else:
        flask.current_app.logger.info('An anonymous user patched request %d', request.id)

    flask.current_app.logger.debug(
        f'Overall time for web/api_v1/534:patch_request(): {time.time() - overall_start_time}'
    )
    return flask.jsonify(request.to_json()), 200


@api_v1.route('/builds/rm', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.rm_operators")
def rm_operators() -> Tuple[flask.Response, int]:
    """
    Submit a request to remove operators from an index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: RmRequestPayload = cast(RmRequestPayload, flask.request.get_json())
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestRm.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    overwrite_from_index = payload.get('overwrite_from_index', False)

    args = _get_rm_args(payload, request, overwrite_from_index)
    safe_args = _get_safe_args(args, payload)

    error_callback = failed_request_callback.s(request.id)
    from_index_pull_spec = request.from_index.pull_specification if request.from_index else None
    try:
        handle_rm_request.apply_async(
            args=args,
            link_error=error_callback,
            argsrepr=repr(safe_args),
            queue=_get_user_queue(
                serial=overwrite_from_index,
                from_index_pull_spec=from_index_pull_spec,
            ),
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/regenerate-bundle', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.regenerate_bundle")
def regenerate_bundle() -> Tuple[flask.Response, int]:
    """
    Submit a request to regenerate an operator bundle image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: RegenerateBundlePayload = cast(RegenerateBundlePayload, flask.request.get_json())
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestRegenerateBundle.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    args = [
        payload['from_bundle_image'],
        payload.get('organization'),
        request.id,
        payload.get('registry_auths'),
        payload.get('bundle_replacements', dict()),
    ]
    safe_args = _get_safe_args(args, payload)

    error_callback = failed_request_callback.s(request.id)
    try:
        handle_regenerate_bundle_request.apply_async(
            args=args,
            link_error=error_callback,
            argsrepr=repr(safe_args),
            queue=_get_user_queue(),
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/regenerate-bundle-batch', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.regenerate_bundle_batch")
def regenerate_bundle_batch() -> Tuple[flask.Response, int]:
    """
    Submit a batch of requests to regenerate operator bundle images.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: RegenerateBundleBatchPayload = cast(
        RegenerateBundleBatchPayload, flask.request.get_json()
    )
    Batch.validate_batch_request_params(payload)

    batch = Batch(annotations=payload.get('annotations'))
    db.session.add(batch)

    requests = []
    # Iterate through all the build requests and verify that the requests are valid before
    # committing them and scheduling the tasks
    for build_request in payload['build_requests']:
        try:
            request = RequestRegenerateBundle.from_json(build_request, batch)
        except ValidationError as e:
            # Rollback the transaction if any of the build requests are invalid
            db.session.rollback()
            raise ValidationError(
                f'{str(e).rstrip(".")}. This occurred on the build request in '
                f'index {payload["build_requests"].index(build_request)}.'
            )
        db.session.add(request)
        requests.append(request)

    db.session.commit()
    messaging.send_messages_for_new_batch_of_requests(requests)

    request_jsons = []
    # This list will be used for the log message below and avoids the need of having to iterate
    # through the list of requests another time
    processed_request_ids = []
    build_and_requests = zip(payload['build_requests'], requests)
    try:
        for build_request, request in build_and_requests:
            args = [
                build_request['from_bundle_image'],
                build_request.get('organization'),
                request.id,
                build_request.get('registry_auths'),
                build_request.get('bundle_replacements', dict()),
            ]
            safe_args = _get_safe_args(args, build_request)
            error_callback = failed_request_callback.s(request.id)
            handle_regenerate_bundle_request.apply_async(
                args=args,
                link_error=error_callback,
                argsrepr=repr(safe_args),
                queue=_get_user_queue(),
            )

            request_jsons.append(request.to_json())
            processed_request_ids.append(str(request.id))
    except kombu.exceptions.OperationalError:
        unprocessed_requests = [r for r in requests if str(r.id) not in processed_request_ids]
        handle_broker_batch_error(unprocessed_requests)

    flask.current_app.logger.debug(
        'Successfully scheduled the batch %d with requests: %s',
        batch.id,
        ', '.join(processed_request_ids),
    )
    return flask.jsonify(request_jsons), 201


@api_v1.route('/builds/add-rm-batch', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.add_rm_batch")
def add_rm_batch() -> Tuple[flask.Response, int]:
    """
    Submit a batch of requests to add or remove operators from an index image.

    Note: Any duplicate bundle will be removed from payload when adding operators.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: AddRmBatchPayload = cast(AddRmBatchPayload, flask.request.get_json())
    Batch.validate_batch_request_params(payload)

    batch = Batch(annotations=payload.get('annotations'))
    db.session.add(batch)

    requests: List[Union[RequestAdd, RequestRm]] = []
    # Iterate through all the build requests and verify that the requests are valid before
    # committing them and scheduling the tasks
    for build_request in payload['build_requests']:
        try:
            if build_request.get('operators'):
                # Check for the validity of a RM request
                # cast Union[AddRequestPayload, RmRequestPayload] based on presence of 'operators'
                request = RequestRm.from_json(cast(RmRequestPayload, build_request), batch)
            elif build_request.get('bundles'):
                # cast Union[AddRequestPayload, RmRequestPayload] based on presence of 'bundles'
                build_request_uniq = cast(AddRequestPayload, copy.deepcopy(build_request))
                build_request_uniq['bundles'] = _get_unique_bundles(build_request_uniq['bundles'])
                # Check for the validity of an Add request
                request = RequestAdd.from_json(build_request_uniq, batch)
            else:
                raise ValidationError('Build request is not a valid Add/Rm request.')
        except ValidationError as e:
            raise ValidationError(
                f'{str(e).rstrip(".")}. This occurred on the build request in '
                f'index {payload["build_requests"].index(build_request)}.'
            )
        db.session.add(request)
        requests.append(request)

    db.session.commit()
    messaging.send_messages_for_new_batch_of_requests(requests)

    request_jsons = []
    # This list will be used for the log message below and avoids the need of having to iterate
    # through the list of requests another time
    processed_request_ids = []
    for build_request, request in zip(payload['build_requests'], requests):
        request_jsons.append(request.to_json())

        overwrite_from_index = build_request.get('overwrite_from_index', False)
        from_index_pull_spec = request.from_index.pull_specification if request.from_index else None
        celery_queue = _get_user_queue(
            serial=overwrite_from_index, from_index_pull_spec=from_index_pull_spec
        )
        if isinstance(request, RequestAdd):
            args: List[Any] = _get_add_args(
                # cast Union[AddRequestPayload, RmRequestPayload] based on request variable
                cast(AddRequestPayload, build_request),
                request,
                overwrite_from_index,
                celery_queue,
            )
        elif isinstance(request, RequestRm):
            args = _get_rm_args(
                # cast Union[AddRequestPayload, RmRequestPayload] based on request variable
                cast(RmRequestPayload, build_request),
                request,
                overwrite_from_index,
            )

        safe_args = _get_safe_args(args, build_request)

        error_callback = failed_request_callback.s(request.id)
        try:
            if isinstance(request, RequestAdd):
                handle_add_request.apply_async(
                    args=args,
                    link_error=error_callback,
                    argsrepr=repr(safe_args),
                    queue=celery_queue,
                )
            else:
                handle_rm_request.apply_async(
                    args=args,
                    link_error=error_callback,
                    argsrepr=repr(safe_args),
                    queue=celery_queue,
                )
        except kombu.exceptions.OperationalError:
            unprocessed_requests = [r for r in requests if str(r.id) not in processed_request_ids]
            handle_broker_batch_error(unprocessed_requests)

        processed_request_ids.append(str(request.id))

    flask.current_app.logger.debug(
        'Successfully scheduled the batch %d with requests: %s',
        batch.id,
        ', '.join(processed_request_ids),
    )
    return flask.jsonify(request_jsons), 201


@api_v1.route('/builds/merge-index-image', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.merge_index_image")
def merge_index_image() -> Tuple[flask.Response, int]:
    """
    Submit a request to merge two index images.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: MergeIndexImagesPayload = cast(MergeIndexImagesPayload, flask.request.get_json())
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')
    request = RequestMergeIndexImage.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    overwrite_target_index = payload.get('overwrite_target_index', False)
    celery_queue = _get_user_queue(serial=overwrite_target_index)
    args = [
        payload['source_from_index'],
        payload.get('deprecation_list', []),
        request.id,
        payload.get('binary_image'),
        payload.get('target_index'),
        overwrite_target_index,
        payload.get('overwrite_target_index_token'),
        request.distribution_scope,
        flask.current_app.config['IIB_BINARY_IMAGE_CONFIG'],
        payload.get('build_tags', []),
        payload.get('graph_update_mode'),
        payload.get('ignore_bundle_ocp_version'),
    ]
    safe_args = _get_safe_args(args, payload)

    error_callback = failed_request_callback.s(request.id)
    try:
        handle_merge_request.apply_async(
            args=args, link_error=error_callback, argsrepr=repr(safe_args), queue=celery_queue
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/create-empty-index', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.create_empty_index")
def create_empty_index() -> Tuple[flask.Response, int]:
    """
    Submit a request to create an index image without bundles.

    Note: Any duplicate bundle will be removed from payload.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: CreateEmptyIndexPayload = cast(CreateEmptyIndexPayload, flask.request.get_json())
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestCreateEmptyIndex.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    args = [
        payload['from_index'],
        request.id,
        payload.get('output_fbc'),
        payload.get('binary_image'),
        payload.get('labels'),
        flask.current_app.config['IIB_BINARY_IMAGE_CONFIG'],
    ]
    safe_args = _get_safe_args(args, payload)
    error_callback = failed_request_callback.s(request.id)

    try:
        handle_create_empty_index_request.apply_async(
            args=args, link_error=error_callback, argsrepr=repr(safe_args), queue=_get_user_queue()
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/recursive-related-bundles', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.recursive_related_bundles")
def recursive_related_bundles() -> Tuple[flask.Response, int]:
    """
    Submit a request to get nested related bundles of an operator bundle image.

    The nested related bundles will be returned as a list in a reversed level-order traversal.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: RecursiveRelatedBundlesRequestPayload = cast(
        RecursiveRelatedBundlesRequestPayload, flask.request.get_json()
    )
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestRecursiveRelatedBundles.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    args = [
        payload['parent_bundle_image'],
        payload.get('organization'),
        request.id,
        payload.get('registry_auths'),
    ]
    safe_args = _get_safe_args(args, payload)

    error_callback = failed_request_callback.s(request.id)
    try:
        handle_recursive_related_bundles_request.apply_async(
            args=args,
            link_error=error_callback,
            argsrepr=repr(safe_args),
            queue=_get_user_queue(),
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/<int:request_id>/nested-bundles')
@instrument_tracing(span_name="web.api_v1.get_nested_bundles")
def get_nested_bundles(request_id: int) -> flask.Response:
    """
    Retrieve the nested bundle images for a recursive-related-bundle request.

    :param int request_id: the request ID that was passed in through the URL.
    :rtype: flask.Response
    :raise NotFound: if the request is not found or there are no related bundles for the request
    :raise Gone: if the related bundles for the build request have been removed due to expiration
    :raise ValidationError: if the request is of invalid type or is not completed yet
    """
    recursive_related_bundles_dir = flask.current_app.config[
        'IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR'
    ]
    s3_bucket_name = flask.current_app.config['IIB_AWS_S3_BUCKET_NAME']

    request = Request.query.get_or_404(request_id)
    if request.type != RequestTypeMapping.recursive_related_bundles.value:
        raise ValidationError(
            f'The request {request_id} is of type {request.type_name}. '
            'This endpoint is only valid for requests of type recursive-related-bundles.'
        )

    finalized = request.state.state_name in RequestStateMapping.get_final_states()
    if not finalized:
        raise ValidationError(
            f'The request {request_id} is not complete yet.'
            ' nested_bundles will be available once the request is complete.'
        )

    # If S3 bucket is configured, fetch the related bundles file from the S3 bucket.
    # Else, check if related bundles are stored on the system itself and return them.
    # Otherwise, raise an IIBError.
    if s3_bucket_name:
        log_file = get_artifact_file_from_s3_bucket(
            'recursive_related_bundles',
            f'{request_id}_recursive_related_bundles.json',
            request_id,
            request.temporary_data_expiration,
            s3_bucket_name,
        )
        return flask.Response(log_file.read(), mimetype='application/json')

    related_bundles_file_path = os.path.join(
        recursive_related_bundles_dir, f'{request_id}_recursive_related_bundles.json'
    )
    if not os.path.exists(related_bundles_file_path):
        expired = request.temporary_data_expiration < datetime.utcnow()
        if expired:
            raise Gone(f'The nested_bundles for the build request {request_id} no longer exist')
        flask.current_app.logger.warning(
            ' Please make sure either an S3 bucket is configured or the data is'
            ' stored locally in a directory by specifying'
            ' IIB_REQUEST_RECURSIVE_RELATED_BUNDLES_DIR'
        )
        raise IIBError('IIB is done processing the request and could not find nested_bundles.')

    with open(related_bundles_file_path) as f:
        return flask.Response(f.read(), mimetype='application/json')


@api_v1.route('/builds/fbc-operations', methods=['POST'])
@login_required
@instrument_tracing(span_name="web.api_v1.fbc_operations")
def fbc_operations() -> Tuple[flask.Response, int]:
    """
    Submit a request to run supported fbc operation on an FBC index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload: FbcOperationRequestPayload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestFbcOperations.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    overwrite_from_index = payload.get('overwrite_from_index', False)
    from_index_pull_spec = request.from_index.pull_specification if request.from_index else None
    celery_queue = _get_user_queue(
        serial=overwrite_from_index, from_index_pull_spec=from_index_pull_spec
    )

    args = [
        request.id,
        payload['fbc_fragment'],
        payload['from_index'],
        payload.get('binary_image'),
        payload.get('distribution_scope'),
        payload.get('overwrite_from_index'),
        payload.get('overwrite_from_index_token'),
        payload.get('build_tags'),
        payload.get('add_arches'),
        flask.current_app.config['IIB_BINARY_IMAGE_CONFIG'],
    ]
    safe_args = _get_safe_args(args, payload)
    error_callback = failed_request_callback.s(request.id)
    try:
        handle_fbc_operation_request.apply_async(
            args=args, link_error=error_callback, argsrepr=repr(safe_args), queue=celery_queue
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201
