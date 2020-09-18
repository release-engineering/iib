# SPDX-License-Identifier: GPL-3.0-or-later
from copy import deepcopy
from datetime import timedelta
from enum import Enum
import json

from flask import current_app, url_for
from flask_login import UserMixin, current_user
import sqlalchemy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import joinedload, load_only, validates
from werkzeug.exceptions import Forbidden

from iib.exceptions import ValidationError
from iib.web import db


class BaseEnum(Enum):
    """A base class for IIB enums."""

    @classmethod
    def get_names(cls):
        """
        Get a sorted list of enum names.

        :return: a sorted list of valid enum names
        :rtype: list
        """
        return sorted([e.name for e in cls])


class RequestStateMapping(BaseEnum):
    """An Enum that represents the request states."""

    in_progress = 1
    complete = 2
    failed = 3

    @staticmethod
    def get_final_states():
        """
        Get the states that are considered final for a request.

        :return: a list of states
        :rtype: list<str>
        """
        return ['complete', 'failed']

    @classmethod
    def validate_state(cls, state):
        """
        Verify that the input state is valid.

        :param str state: the state to validate
        :raises iib.exceptions.ValidationError: if the state is invalid
        """
        state_names = cls.get_names()
        if state not in state_names:
            states = ', '.join(state_names)
            raise ValidationError(
                f'{state} is not a valid build request state. Valid states are: {states}'
            )


class RequestTypeMapping(BaseEnum):
    """An Enum that represents the request types."""

    generic = 0
    add = 1
    rm = 2
    regenerate_bundle = 3
    merge_index_image = 4

    @classmethod
    def pretty(cls, num):
        """
        Return the prettified version of the enum value.

        :param int num: the enum value
        :return: the prettified string representation of the enum value
        :rtype: str
        """
        return cls(num).name.replace('_', '-')


class BundleDeprecation(db.Model):
    """An association table between index merge requests and bundle images which they deprecate."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    merge_index_image_id = db.Column(
        db.Integer,
        db.ForeignKey('request_merge_index_image.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    bundle_id = db.Column(
        db.Integer, db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True,
    )

    __table_args__ = (db.UniqueConstraint('merge_index_image_id', 'bundle_id'),)


class Architecture(db.Model):
    """An architecture associated with an image."""

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False, unique=True)

    def __repr__(self):
        return '<Architecture name={0!r}>'.format(self.name)

    @staticmethod
    def validate_architecture_json(arches):
        """
        Validate the JSON representation of architectures.

        :param list arches: the JSON representation of architectures for a build request
        :raise ValidationError: if the JSON does not match the required schema
        """
        if not isinstance(arches, list) or any(
            not arch or not isinstance(arch, str) for arch in arches
        ):
            raise ValidationError(
                'Architectures should be specified as a non-empty array of strings'
            )


class RequestArchitecture(db.Model):
    """An association table between requests and the architectures they were built for."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_id = db.Column(
        db.Integer, db.ForeignKey('request.id'), autoincrement=False, index=True, primary_key=True
    )
    architecture_id = db.Column(
        db.Integer,
        db.ForeignKey('architecture.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )

    __table_args__ = (db.UniqueConstraint('request_id', 'architecture_id'),)


class Image(db.Model):
    """
    An image that has been handled by IIB.

    This will typically point to a manifest list.
    """

    id = db.Column(db.Integer, primary_key=True)
    operator_id = db.Column(db.Integer, db.ForeignKey('operator.id'))
    pull_specification = db.Column(db.String, nullable=False, index=True, unique=True)

    operator = db.relationship('Operator')

    def __repr__(self):
        return '<Image pull_specification={0!r}>'.format(self.pull_specification)

    @classmethod
    def get_or_create(cls, pull_specification):
        """
        Get the image from the database and create it if it doesn't exist.

        :param str pull_specification: pull_specification of the image
        :return: an Image object based on the input pull_specification; the Image object will be
            added to the database session, but not committed, if it was created
        :rtype: Image
        :raise ValidationError: if pull_specification for the image is invalid
        """
        if '@' not in pull_specification and ':' not in pull_specification:
            raise ValidationError(
                f'Image {pull_specification} should have a tag or a digest specified.'
            )

        image = cls.query.filter_by(pull_specification=pull_specification).first()
        if not image:
            image = Image(pull_specification=pull_specification)
            db.session.add(image)

        return image


class Operator(db.Model):
    """An operator that has been handled by IIB."""

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False, index=True, unique=True)

    def __repr__(self):
        return '<Operator name={0!r}>'.format(self.name)

    @classmethod
    def get_or_create(cls, name):
        """
        Get the operator from the database and create it if it doesn't exist.

        :param str name: the name of the operator
        :return: an Operator object based on the input name; the Operator object will be
            added to the database session, but not committed, if it was created
        :rtype: Operator
        """
        operator = cls.query.filter_by(name=name).first()
        if not operator:
            operator = Operator(name=name)
            db.session.add(operator)

        return operator


class RequestRmOperator(db.Model):
    """An association table between rm requests and the operators they contain."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_rm_id = db.Column(
        db.Integer,
        db.ForeignKey('request_rm.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    operator_id = db.Column(
        db.Integer, db.ForeignKey('operator.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (db.UniqueConstraint('request_rm_id', 'operator_id'),)


class RequestAddBundle(db.Model):
    """An association table between add requests and the bundles they contain."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_add_id = db.Column(
        db.Integer,
        db.ForeignKey('request_add.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    image_id = db.Column(
        db.Integer, db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (db.UniqueConstraint('request_add_id', 'image_id'),)


class Request(db.Model):
    """A generic image build request."""

    __tablename__ = 'request'

    id = db.Column(db.Integer, primary_key=True)
    architectures = db.relationship(
        'Architecture', order_by='Architecture.name', secondary=RequestArchitecture.__table__
    )
    batch_id = db.Column(db.Integer, db.ForeignKey('batch.id'), index=True, nullable=False)
    batch = db.relationship('Batch', back_populates='requests')
    request_state_id = db.Column(
        db.Integer, db.ForeignKey('request_state.id'), index=True, unique=True
    )
    # This maps to a value in RequestTypeMapping
    type = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    state = db.relationship('RequestState', foreign_keys=[request_state_id])
    states = db.relationship(
        'RequestState',
        foreign_keys='RequestState.request_id',
        back_populates='request',
        order_by='RequestState.updated',
    )
    user = db.relationship('User', back_populates='requests')

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['generic'].value,
        'polymorphic_on': type,
    }

    @validates('type')
    def validate_type(self, key, type_num):
        """
        Verify the type number used is valid.

        :param str key: the name of the database column
        :param int type_num: the request type number to be verified
        :return: the request type number
        :rtype: int
        :raises ValidationError: if the request type is invalid
        """
        if not any(type_num == num.value for num in RequestTypeMapping):
            raise ValidationError(f'{type_num} is not a valid request type number')
        return type_num

    def __repr__(self):
        return '<{0} {1!r}>'.format(self.__class__.__name__, self.id)

    def add_state(self, state, state_reason):
        """
        Add a RequestState associated with the current request.

        :param str state: the state name
        :param str state_reason: the reason explaining the state transition
        :raises ValidationError: if the state is invalid
        """
        try:
            state_int = RequestStateMapping.__members__[state].value
        except KeyError:
            raise ValidationError(
                'The state "{}" is invalid. It must be one of: {}.'.format(
                    state, ', '.join(RequestStateMapping.get_names())
                )
            )

        for s in ('complete', 'failed'):
            # A complete or failed state cannot change states, but the state reason
            # can be updated
            if self.state and self.state.state_name == s and state != s:
                raise ValidationError(f'A {self.state.state_name} request cannot change states')

        request_state = RequestState(state=state_int, state_reason=state_reason)
        self.states.append(request_state)
        # Send the changes queued up in SQLAlchemy to the database's transaction buffer.
        # This will generate an ID that can be used below.
        db.session.add(request_state)
        db.session.flush()
        self.request_state_id = request_state.id

    def add_architecture(self, arch_name):
        """
        Add an architecture associated with this image.

        :param str arch_name: the architecture to add
        :raises ValidationError: if the architecture is invalid
        """
        arch = db.session.query(Architecture).filter_by(name=arch_name).first()
        if not arch:
            arch = Architecture(name=arch_name)
            db.session.add(arch)
            db.session.flush()

        if arch not in self.architectures:
            self.architectures.append(arch)

    @classmethod
    def from_json(cls, kwargs):
        """
        Handle JSON requests for a request API endpoint.

        Child classes MUST override this method.

        :param dict kwargs: the user provided parameters to create a Request
        :return: an object representation of the request
        :retype: Request
        """
        raise NotImplementedError('{} does not implement from_json'.format(cls.__name__))

    def to_json(self, verbose=True):
        """
        Provide the basic JSON representation of a build request.

        Child classes are expected to enhance the JSON representation as needed.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        rv = {
            'id': self.id,
            'arches': [arch.name for arch in self.architectures],
            'batch': self.batch.id,
            'request_type': RequestTypeMapping.pretty(self.type),
            'user': getattr(self.user, 'username', None),
        }

        def _state_to_json(state):
            return {
                'state': RequestStateMapping(state.state).name,
                'state_reason': state.state_reason,
                'updated': state.updated.isoformat() + 'Z',
            }

        latest_state = None
        if verbose:
            rv['batch_annotations'] = self.batch.annotations
            states = [_state_to_json(state) for state in self.states]
            # Reverse the list since the latest states should be first
            states = list(reversed(states))
            rv['state_history'] = states
            latest_state = states[0]
            if current_app.config['IIB_REQUEST_LOGS_DIR']:
                rv['logs'] = {
                    'expiration': self.logs_expiration.isoformat() + 'Z',
                    'url': url_for('.get_build_logs', request_id=self.id, _external=True),
                }
        rv.update(latest_state or _state_to_json(self.state))

        return rv

    def get_mutable_keys(self):
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        return {'arches', 'state', 'state_reason'}

    @property
    def type_name(self):
        """
        Get the request's type as a string.

        :return: the request's type
        :rtype: str
        """
        return RequestTypeMapping.pretty(self.type)

    @property
    def logs_expiration(self):
        """
        Return the timestamp of when logs are considered expired.

        :return: logs expiration timestamp
        :rtype: str
        """
        logs_lifetime = timedelta(days=current_app.config['IIB_REQUEST_LOGS_DAYS_TO_LIVE'])
        return self.state.updated + logs_lifetime


class Batch(db.Model):
    """A batch associated with one or more requests."""

    id = db.Column(db.Integer, primary_key=True)
    _annotations = db.Column('annotations', db.Text, nullable=True)

    requests = db.relationship(
        'Request', foreign_keys=[Request.batch_id], back_populates='batch', order_by='Request.id'
    )

    @property
    def annotations(self):
        """Return the Python representation of the JSON annotations."""
        return json.loads(self._annotations) if self._annotations else None

    @annotations.setter
    def annotations(self, annotations):
        """
        Set the annotations column to the input annotations as a JSON string.

        If ``None`` is provided, it will be simply set to ``None`` and not be converted to JSON.

        :param dict annotations: the dictionary of the annotations or ``None``
        """
        self._annotations = (
            json.dumps(annotations, sort_keys=True) if annotations is not None else None
        )

    @staticmethod
    def validate_batch_request_params(payload):
        """
        Validate batch specific parameters from the input JSON payload.

        The requests in the "build_requests" key's value are not validated. Those should be
        validated separately.

        :raises ValidationError: if the payload is invalid
        """
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get('build_requests'), list)
            or not payload['build_requests']
        ):
            raise ValidationError(
                'The input data must be a JSON object and the "build_requests" value must be a '
                'non-empty array'
            )

        if not isinstance(payload.get('annotations', {}), dict):
            raise ValidationError('The value of "annotations" must be a JSON object')

    @property
    def state(self):
        """
        Get the state of the batch.

        If one or more requests in the batch are ``in_progress``, then the batch is ``in_progress``.
        Once all the requests in the batch have completed, if one or more requests are in the
        ``failed`` state, then so is the batch. If all requests in the batch are in the ``complete``
        state, then so is the batch.

        :return: the state of the batch
        :rtype: str
        """
        contains_failure = False
        for state in self.request_states:
            # If one of the requests is still in progress, the batch is also
            if state == 'in_progress':
                return 'in_progress'
            elif state == 'failed':
                contains_failure = True

        # At this point, we know the batch is done
        if contains_failure:
            return 'failed'
        else:
            return 'complete'

    @property
    def request_states(self):
        """
        Get the states of all the requests in the batch.

        :return: the list of states
        :rtype: list<str>
        """
        # Only load the columns that are required to get the current state of the requests
        # in the batch
        requests = (
            db.session.query(Request)
            .options(joinedload(Request.state).load_only(RequestState.state), load_only())
            .filter(Request.batch_id == self.id)
            .order_by(Request.id)
            .all()
        )
        return [RequestStateMapping(request.state.state).name for request in requests]

    @property
    def user(self):
        """
        Get the ``User`` object associated with the batch.

        :return: the ``User`` object associated with the batch or ``None``
        :rtype: User or None
        """
        return (
            db.session.query(User)
            .join(User.requests)
            .join(Request.batch)
            .filter(Request.batch == self)
            .first()
        )

    @staticmethod
    def validate_batch(batch_id):
        """
        Validate the input batch ID.

        If the input batch ID is a string, it will be converted to an integer and returned.

        :param int batch_id: the ID of the batch
        :raise ValidationError: if the batch ID is invalid
        :return: the validated batch ID
        :rtype: int
        """
        rv = batch_id
        error_msg = 'The batch must be a positive integer'
        if isinstance(batch_id, str):
            try:
                rv = int(batch_id)
            except ValueError:
                raise ValidationError(error_msg)
        elif not isinstance(batch_id, int):
            raise ValidationError(error_msg)

        if rv < 1:
            raise ValidationError(error_msg)

        return rv


def get_request_query_options(verbose=False):
    """
    Get the query options for a SQLAlchemy query for one or more requests to output as JSON.

    This will add the joins ahead of time on relationships that are accessed in the ``to_json``
    methods to avoid individual select statements when the relationships are accessed.

    :param bool verbose: if the request relationships should be loaded for verbose JSON output
    :return: a list of SQLAlchemy query options
    :rtype: list
    """
    # Tell SQLAlchemy to join on the relationships that are part of the JSON to avoid
    # additional SQL queries
    query_options = [
        joinedload(Request.user),
        joinedload(RequestAdd.binary_image),
        joinedload(RequestAdd.binary_image_resolved),
        joinedload(RequestAdd.bundles),
        joinedload(RequestAdd.from_index),
        joinedload(RequestAdd.from_index_resolved),
        joinedload(RequestAdd.index_image),
        joinedload(RequestRegenerateBundle.bundle_image),
        joinedload(RequestRegenerateBundle.from_bundle_image),
        joinedload(RequestRegenerateBundle.from_bundle_image_resolved),
        joinedload(RequestRm.binary_image),
        joinedload(RequestRm.binary_image_resolved),
        joinedload(RequestRm.from_index),
        joinedload(RequestRm.from_index_resolved),
        joinedload(RequestRm.index_image),
        joinedload(RequestRm.operators),
    ]
    if verbose:
        query_options.append(joinedload(Request.states))
    else:
        query_options.append(joinedload(Request.state))

    return query_options


class RequestIndexImageMixin:
    """
    A class for shared functionality between index image requests.

    This class uses the Mixin pattern as defined in:
    https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/mixins.html
    """

    @declared_attr
    def binary_image_id(cls):
        """Return the ID of the image that the opm binary comes from."""
        return db.Column(db.Integer, db.ForeignKey('image.id'), nullable=False)

    @declared_attr
    def binary_image_resolved_id(cls):
        """Return the ID of the resolved image that the opm binary comes from."""
        return db.Column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def binary_image(cls):
        """Return the relationship to the image that the opm binary comes from."""
        return db.relationship('Image', foreign_keys=[cls.binary_image_id], uselist=False)

    @declared_attr
    def binary_image_resolved(cls):
        """Return the relationship to the resolved image that the opm binary comes from."""
        return db.relationship('Image', foreign_keys=[cls.binary_image_resolved_id], uselist=False)

    @declared_attr
    def from_index_id(cls):
        """Return the ID of the index image to base the request from."""
        return db.Column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def from_index_resolved_id(cls):
        """Return the ID of the resolved index image to base the request from."""
        return db.Column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def from_index(cls):
        """Return the relationship of the index image to base the request from."""
        return db.relationship('Image', foreign_keys=[cls.from_index_id], uselist=False)

    @declared_attr
    def from_index_resolved(cls):
        """Return the relationship of the resolved index image to base the request from."""
        return db.relationship('Image', foreign_keys=[cls.from_index_resolved_id], uselist=False)

    @declared_attr
    def index_image_id(cls):
        """Return the ID of the built index image."""
        return db.Column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def index_image(cls):
        """Return the relationship to the built index image."""
        return db.relationship('Image', foreign_keys=[cls.index_image_id], uselist=False)

    @staticmethod
    def _from_json(
        request_kwargs, additional_required_params=None, additional_optional_params=None, batch=None
    ):
        """
        Validate and process request agnostic parameters.

        As part of the processing, the input ``request_kwargs`` parameter
        is updated to reference database objects where appropriate.

        :param dict request_kwargs: copy of args provided in API request
        :param Batch batch: the batch to specify with the request. If one is not specified, one will
            be created automatically.
        """
        # Validate all required parameters are present
        required_params = {'binary_image'} | set(additional_required_params or [])
        optional_params = {
            'add_arches',
            'overwrite_from_index',
            'overwrite_from_index_token',
        } | set(additional_optional_params or [])

        validate_request_params(
            request_kwargs, required_params=required_params, optional_params=optional_params,
        )

        # following condition does not apply to merge endpoint
        if not request_kwargs.get('source_from_index'):
            # Check if both `from_index` and `add_arches` are not specified
            if not request_kwargs.get('from_index') and not request_kwargs.get('add_arches'):
                raise ValidationError('One of "from_index" or "add_arches" must be specified')

        # Verify that `overwrite_from_index` is the correct type
        overwrite = request_kwargs.pop('overwrite_from_index', False)
        if not isinstance(overwrite, bool):
            raise ValidationError('The "overwrite_from_index" parameter must be a boolean')

        # Verify that `overwrite_from_index_token` is the correct type
        overwrite_token = request_kwargs.pop('overwrite_from_index_token', None)
        if overwrite_token:
            if not isinstance(overwrite_token, str):
                raise ValidationError('The "overwrite_from_index_token" parameter must be a string')
            if overwrite_token and not overwrite:
                raise ValidationError(
                    'The "overwrite_from_index" parameter is required when'
                    ' the "overwrite_from_index_token" parameter is used'
                )

        # Verify the user is authorized to use overwrite_from_index
        # current_user.is_authenticated is only ever False when auth is disabled
        if current_user.is_authenticated:
            privileged_users = current_app.config['IIB_PRIVILEGED_USERNAMES']
            if overwrite and not overwrite_token and current_user.username not in privileged_users:
                raise Forbidden(
                    'You must be a privileged user to set "overwrite_from_index" without'
                    ' setting "overwrite_from_index_token"'
                )

        # Validate add_arches are correctly provided
        add_arches = request_kwargs.pop('add_arches', [])
        Architecture.validate_architecture_json(add_arches)

        # Validate binary_image is correctly provided
        binary_image = request_kwargs.pop('binary_image')
        if not isinstance(binary_image, str):
            raise ValidationError('"binary_image" must be a string')

        request_kwargs['binary_image'] = Image.get_or_create(pull_specification=binary_image)

        if 'from_index' in request_kwargs:
            if not isinstance(request_kwargs['from_index'], str):
                raise ValidationError('"from_index" must be a string')
            request_kwargs['from_index'] = Image.get_or_create(
                pull_specification=request_kwargs['from_index']
            )

        # current_user.is_authenticated is only ever False when auth is disabled
        if current_user.is_authenticated:
            request_kwargs['user'] = current_user

        # Add the request to a new batch
        batch = batch or Batch()
        db.session.add(batch)
        request_kwargs['batch'] = batch

    def get_common_index_image_json(self):
        """
        Return the common set of attributes for an index image request.

        For compatibility between the different types of index image
        requests, any index image request must provide the combination
        of possible attributes. For example, the "bundles" attribute is
        always included even though it's only used by RequestAdd.

        The specialized index image requests should modify the value of
        the attributes as needed.

        :return: a partial dictionary representing the JSON of the index image build request
        :rtype: dict
        """
        return {
            'binary_image': self.binary_image.pull_specification,
            'binary_image_resolved': getattr(
                self.binary_image_resolved, 'pull_specification', None
            ),
            'bundle_mapping': {},
            'bundles': [],
            'from_index': getattr(self.from_index, 'pull_specification', None),
            'from_index_resolved': getattr(self.from_index_resolved, 'pull_specification', None),
            'index_image': getattr(self.index_image, 'pull_specification', None),
            'organization': None,
            'removed_operators': [],
        }

    def get_index_image_mutable_keys(self):
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        return {
            'binary_image_resolved',
            'from_bundle_image_resolved',
            'from_index_resolved',
            'index_image',
        }


class RequestAdd(Request, RequestIndexImageMixin):
    """An "add" index image build request."""

    __tablename__ = 'request_add'

    id = db.Column(db.Integer, db.ForeignKey('request.id'), autoincrement=False, primary_key=True)
    bundles = db.relationship('Image', secondary=RequestAddBundle.__table__)
    organization = db.Column(db.String, nullable=True)

    omps_operator_version = db.Column(db.String, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['add'].value,
    }

    @classmethod
    def from_json(cls, kwargs, batch=None):
        """
        Handle JSON requests for the Add API endpoint.

        :param dict kwargs: the JSON payload of the request.
        :param Batch batch: the batch to specify with the request.
        """
        request_kwargs = deepcopy(kwargs)

        bundles = request_kwargs.get('bundles', [])
        if not isinstance(bundles, list) or any(
            not item or not isinstance(item, str) for item in bundles
        ):
            raise ValidationError(
                '"bundles" should be either an empty array or an array of non-empty strings'
            )

        # Check if no bundles `from_index and `binary_image` are specified
        # if no bundles and and no from index then a empty index will be created
        # if no binary image and just a from_index then we are not updating anything and it would
        # be a no-op
        if not request_kwargs.get('bundles') and (
            not request_kwargs.get('from_index') or not request_kwargs.get('binary_image')
        ):
            raise ValidationError(
                '"from_index" and "binary_image" must be specified if no bundles are specified'
            )

        for param in ('cnr_token', 'organization'):
            if param not in request_kwargs:
                continue

            if not isinstance(request_kwargs[param], str):
                raise ValidationError(f'"{param}" must be a string')

        if not isinstance(request_kwargs.get('force_backport', False), bool):
            raise ValidationError('"force_backport" must be a boolean')

        # Remove attributes that are not stored in the database
        request_kwargs.pop('cnr_token', None)
        request_kwargs.pop('force_backport', None)

        cls._from_json(
            request_kwargs,
            additional_optional_params=['from_index', 'organization', 'bundles'],
            batch=batch,
        )

        request_kwargs['bundles'] = [
            Image.get_or_create(pull_specification=item) for item in bundles
        ]

        request = cls(**request_kwargs)
        request.add_state('in_progress', 'The request was initiated')
        return request

    def to_json(self, verbose=True):
        """
        Provide the JSON representation of an "add" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        rv = super().to_json(verbose=verbose)
        rv.update(self.get_common_index_image_json())
        rv['organization'] = self.organization
        rv['omps_operator_version'] = {}
        if self.omps_operator_version:
            rv['omps_operator_version'] = json.loads(self.omps_operator_version)

        for bundle in self.bundles:
            if bundle.operator:
                rv['bundle_mapping'].setdefault(bundle.operator.name, []).append(
                    bundle.pull_specification
                )
            rv['bundles'].append(bundle.pull_specification)

        return rv

    def get_mutable_keys(self):
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.update(self.get_index_image_mutable_keys())
        rv.update({'bundles', 'bundle_mapping', 'omps_operator_version'})
        return rv


class RequestRm(Request, RequestIndexImageMixin):
    """A "rm" index image build request."""

    __tablename__ = 'request_rm'

    id = db.Column(db.Integer, db.ForeignKey('request.id'), autoincrement=False, primary_key=True)
    # The ID of the index image to base the request from. This is always
    # required for "rm" requests.
    from_index_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=False)
    operators = db.relationship('Operator', secondary=RequestRmOperator.__table__)

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['rm'].value,
    }

    @classmethod
    def from_json(cls, kwargs, batch=None):
        """
        Handle JSON requests for the Remove API endpoint.

        :param dict kwargs: the JSON payload of the request.
        :param Batch batch: the batch to specify with the request.
        """
        request_kwargs = deepcopy(kwargs)

        operators = request_kwargs.get('operators', [])
        if (
            not isinstance(operators, list)
            or len(operators) == 0
            or any(not item or not isinstance(item, str) for item in operators)
        ):
            raise ValidationError(f'"operators" should be a non-empty array of strings')

        cls._from_json(
            request_kwargs, additional_required_params=['operators', 'from_index'], batch=batch
        )

        request_kwargs['operators'] = [Operator.get_or_create(name=item) for item in operators]

        request = cls(**request_kwargs)
        request.add_state('in_progress', 'The request was initiated')
        return request

    def to_json(self, verbose=True):
        """
        Provide the JSON representation of an "rm" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        rv = super().to_json(verbose=verbose)
        rv.update(self.get_common_index_image_json())
        rv['removed_operators'] = [operator.name for operator in self.operators]

        return rv

    def get_mutable_keys(self):
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.update(self.get_index_image_mutable_keys())
        return rv


class RequestRegenerateBundle(Request):
    """A "regenerate_bundle" image build request."""

    __tablename__ = 'request_regenerate_bundle'

    id = db.Column(db.Integer, db.ForeignKey('request.id'), autoincrement=False, primary_key=True)
    # The ID of the regenerated bundle image
    bundle_image_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=True)
    bundle_image = db.relationship('Image', foreign_keys=[bundle_image_id], uselist=False)
    # The ID of the bundle image to be regenerated
    from_bundle_image_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=False)
    from_bundle_image = db.relationship('Image', foreign_keys=[from_bundle_image_id], uselist=False)
    # The ID of the resolved bundle image to be regenerated
    from_bundle_image_resolved_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=True)
    from_bundle_image_resolved = db.relationship(
        'Image', foreign_keys=[from_bundle_image_resolved_id], uselist=False
    )
    # The name of the organization the bundle should be regenerated for
    organization = db.Column(db.String, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['regenerate_bundle'].value,
    }

    @classmethod
    def from_json(cls, kwargs, batch=None):
        """
        Handle JSON requests for the Regenerate Bundle API endpoint.

        :param dict kwargs: the JSON payload of the request.
        :param Batch batch: the batch to specify with the request. If one is not specified, one will
            be created automatically.
        """
        batch = batch or Batch()
        request_kwargs = deepcopy(kwargs)

        validate_request_params(
            request_kwargs, required_params={'from_bundle_image'}, optional_params={'organization'},
        )

        # Validate organization is correctly provided
        organization = request_kwargs.get('organization')
        if organization and not isinstance(organization, str):
            raise ValidationError('"organization" must be a string')

        # Validate from_bundle_image is correctly provided
        from_bundle_image = request_kwargs.pop('from_bundle_image')
        if not isinstance(from_bundle_image, str):
            raise ValidationError('"from_bundle_image" must be a string')

        request_kwargs['from_bundle_image'] = Image.get_or_create(
            pull_specification=from_bundle_image
        )

        # current_user.is_authenticated is only ever False when auth is disabled
        if current_user.is_authenticated:
            request_kwargs['user'] = current_user

        # Add the request to a new batch
        db.session.add(batch)
        request_kwargs['batch'] = batch

        request = cls(**request_kwargs)
        request.add_state('in_progress', 'The request was initiated')
        return request

    def to_json(self, verbose=True):
        """
        Provide the JSON representation of a "regenerate-bundle" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        rv = super().to_json(verbose=verbose)
        rv['bundle_image'] = getattr(self.bundle_image, 'pull_specification', None)
        rv['from_bundle_image'] = self.from_bundle_image.pull_specification
        rv['from_bundle_image_resolved'] = getattr(
            self.from_bundle_image_resolved, 'pull_specification', None
        )
        rv['organization'] = self.organization

        return rv

    def get_mutable_keys(self):
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.add('bundle_image')
        rv.add('from_bundle_image_resolved')
        return rv


class RequestMergeIndexImage(Request, RequestIndexImageMixin):
    """A "merge-index-image" build request."""

    __tablename__ = 'request_merge_index_image'

    id = db.Column(db.Integer, db.ForeignKey('request.id'), autoincrement=False, primary_key=True)
    source_from_index_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=True)
    source_from_index = db.relationship('Image', foreign_keys=[source_from_index_id], uselist=False)
    target_index_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=True)
    target_index = db.relationship('Image', foreign_keys=[target_index_id], uselist=False)
    deprecation_list = db.relationship('Image', secondary=BundleDeprecation.__table__)

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['merge_index_image'].value,
    }

    @classmethod
    def from_json(cls, kwargs, batch=None):
        """
        Handle JSON requests for the merge-index-image API endpoint.

        :param dict kwargs: the JSON payload of the request.
        :param Batch batch: the batch to specify with the request.
        """
        request_kwargs = deepcopy(kwargs)

        deprecation_list = request_kwargs.pop('deprecation_list', [])
        if not isinstance(deprecation_list, list) or any(
            not item or not isinstance(item, str) or '@' not in item for item in deprecation_list
        ):
            raise ValidationError(
                '"deprecation_list" should be an array of strings. '
                'Each pull specification has to be defined via digest.'
            )

        request_kwargs['deprecation_list'] = [
            Image.get_or_create(pull_specification=item) for item in deprecation_list
        ]

        source_from_index = request_kwargs.pop('source_from_index')
        if not isinstance(source_from_index, str):
            raise ValidationError('"source_from_index" must be a string')
        request_kwargs['source_from_index'] = Image.get_or_create(
            pull_specification=source_from_index
        )

        target_index = request_kwargs.pop('target_index')
        if not isinstance(target_index, str):
            raise ValidationError('"target_index" must be a string')
        request_kwargs['target_index'] = Image.get_or_create(pull_specification=target_index)

        cls._from_json(
            request_kwargs,
            additional_required_params=['source_from_index', 'target_index'],
            additional_optional_params=['deprecation_list'],
            batch=batch,
        )

        request = cls(**request_kwargs)
        request.add_state('in_progress', 'The request was initiated')
        return request

    def to_json(self, verbose=True):
        """
        Provide the JSON representation of an "add" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        rv = super().to_json(verbose=verbose)
        rv.update(self.get_common_index_image_json())
        rv['source_from_index'] = self.source_from_index.pull_specification
        rv['target_index'] = self.target_index.pull_specification
        rv['deprecation_list'] = [bundle.pull_specification for bundle in self.deprecation_list]

        return rv

    def get_mutable_keys(self):
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.update(self.get_index_image_mutable_keys())
        rv.update({'source_from_index_resolved', 'target_index_resolved'})
        return rv


class RequestState(db.Model):
    """Represents a state (historical or present) of a request."""

    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.Integer, db.ForeignKey('request.id'), index=True, nullable=False)
    # This maps to a value in RequestStateMapping
    state = db.Column(db.Integer, nullable=False)
    state_reason = db.Column(db.String, nullable=False)
    updated = db.Column(db.DateTime(), nullable=False, default=sqlalchemy.func.now())

    request = db.relationship('Request', foreign_keys=[request_id], back_populates='states')

    @property
    def state_name(self):
        """Get the state's display name."""
        if self.state:
            return RequestStateMapping(self.state).name

    def __repr__(self):
        return '<RequestState id={} state="{}" request_id={}>'.format(
            self.id, self.state_name, self.request_id
        )


class User(db.Model, UserMixin):
    """Represents an external user that owns an IIB request."""

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String, index=True, unique=True, nullable=False)
    requests = db.relationship('Request', foreign_keys=[Request.user_id], back_populates='user')

    @classmethod
    def get_or_create(cls, username):
        """
        Get the user from the database and create it if it doesn't exist.

        :param str username: the username of the user
        :return: a User object based on the input username; the User object will be
            added to the database session, but not committed, if it was created
        :rtype: User
        """
        user = cls.query.filter_by(username=username).first()
        if not user:
            user = User(username=username)
            db.session.add(user)

        return user


def validate_request_params(request_params, required_params, optional_params):
    """
    Validate parameters for a build request.

    All required parameters must be set in the request_params and
    unknown parameters are not allowed.

    :param dict request_params: the request parameters provided by the user
    :param set required_params: the set of required parameters
    :param set optional_params: the set of optional parameters
    :raises iib.exceptions.ValidationError: if validation of parameters fails
    """
    missing_params = required_params - request_params.keys()
    if missing_params:
        raise ValidationError('Missing required parameter(s): {}'.format(', '.join(missing_params)))

    # Don't allow the user to set arbitrary columns or relationships
    invalid_params = request_params.keys() - required_params - optional_params
    if invalid_params:
        raise ValidationError(
            'The following parameters are invalid: {}'.format(', '.join(invalid_params))
        )

    # Verify that all the required parameters are set and not empty
    for param in required_params:
        if not request_params.get(param):
            raise ValidationError(f'"{param}" must be set')

    # If any optional parameters are set but are empty, just remove them since they are
    # treated as null values
    for param in optional_params:
        if (
            param in request_params
            and not isinstance(request_params.get(param), bool)
            and not request_params[param]
        ):
            del request_params[param]
