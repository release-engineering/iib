# SPDX-License-Identifier: GPL-3.0-or-later
from copy import deepcopy
from enum import Enum

from flask_login import UserMixin, current_user
import sqlalchemy
from sqlalchemy.orm import joinedload

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

    add = 1
    rm = 2


class Architecture(db.Model):
    """
    An architecture associated with an image.
    """

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
    """
    An operator that has been handled by IIB.
    """

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


class RequestOperator(db.Model):
    """An association table between requests and the bundles they contain."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_id = db.Column(
        db.Integer, db.ForeignKey('request.id'), autoincrement=False, index=True, primary_key=True
    )
    operator_id = db.Column(
        db.Integer, db.ForeignKey('operator.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (db.UniqueConstraint('request_id', 'operator_id'),)


class RequestBundle(db.Model):
    """An association table between requests and the bundles they contain."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_id = db.Column(
        db.Integer, db.ForeignKey('request.id'), autoincrement=False, index=True, primary_key=True
    )
    image_id = db.Column(
        db.Integer, db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (db.UniqueConstraint('request_id', 'image_id'),)


class Request(db.Model):
    """An index image build request."""

    id = db.Column(db.Integer, primary_key=True)
    architectures = db.relationship(
        'Architecture', order_by='Architecture.name', secondary=RequestArchitecture.__table__
    )
    # The image that opm binary comes from
    binary_image_id = db.Column(db.Integer, db.ForeignKey('image.id'), nullable=False)
    binary_image_resolved_id = db.Column(db.Integer, db.ForeignKey('image.id'))
    # An optional index image to base the request from
    from_index_id = db.Column(db.Integer, db.ForeignKey('image.id'))
    from_index_resolved_id = db.Column(db.Integer, db.ForeignKey('image.id'))
    # The built index image
    index_image_id = db.Column(db.Integer, db.ForeignKey('image.id'))
    request_state_id = db.Column(
        db.Integer, db.ForeignKey('request_state.id'), index=True, unique=True
    )
    # This maps to a value in RequestTypeMapping
    type = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    binary_image = db.relationship('Image', foreign_keys=[binary_image_id], uselist=False)
    binary_image_resolved = db.relationship(
        'Image', foreign_keys=[binary_image_resolved_id], uselist=False
    )
    bundles = db.relationship('Image', secondary=RequestBundle.__table__)
    from_index = db.relationship('Image', foreign_keys=[from_index_id], uselist=False)
    from_index_resolved = db.relationship(
        'Image', foreign_keys=[from_index_resolved_id], uselist=False
    )
    index_image = db.relationship('Image', foreign_keys=[index_image_id], uselist=False)
    operators = db.relationship('Operator', secondary=RequestOperator.__table__)
    organization = db.Column(db.String, nullable=True)
    state = db.relationship('RequestState', foreign_keys=[request_state_id])
    states = db.relationship(
        'RequestState',
        foreign_keys='RequestState.request_id',
        back_populates='request',
        order_by='RequestState.updated',
    )
    user = db.relationship('User', back_populates='requests')

    def __repr__(self):
        return '<Request {0!r}>'.format(self.id)

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

    @staticmethod
    def get_query_options(verbose=False):
        """
        Get the query options for a SQLAlchemy query for one or more requests to output as JSON.

        This will add the joins ahead of time on relationships that are accessed in the ``to_json``
        method to avoid individual select statements when the relationships are accessed.

        :param bool verbose: if the request relationships should be loaded for verbose JSON output
        :return: a list of SQLAlchemy query options
        :rtype: list
        """
        # Tell SQLAlchemy to join on the relationships that are part of the JSON to avoid
        # additional SQL queries
        query_options = [
            joinedload(Request.binary_image),
            joinedload(Request.binary_image_resolved),
            joinedload(Request.bundles),
            joinedload(Request.from_index),
            joinedload(Request.from_index_resolved),
            joinedload(Request.index_image),
            joinedload(Request.operators),
            joinedload(Request.user),
        ]
        if verbose:
            query_options.append(joinedload(Request.states))
        else:
            query_options.append(joinedload(Request.state))

        return query_options

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

    def to_json(self, verbose=True):
        def _state_to_json(state):
            return {
                'state': RequestStateMapping(state.state).name,
                'state_reason': state.state_reason,
                'updated': state.updated.isoformat() + 'Z',
            }

        rv = {
            'id': self.id,
            'arches': [arch.name for arch in self.architectures],
            'binary_image': self.binary_image.pull_specification,
            'binary_image_resolved': getattr(
                self.binary_image_resolved, 'pull_specification', None
            ),
            'bundle_mapping': {},
            'bundles': [],
            'from_index': getattr(self.from_index, 'pull_specification', None),
            'from_index_resolved': getattr(self.from_index_resolved, 'pull_specification', None),
            'index_image': getattr(self.index_image, 'pull_specification', None),
            'organization': self.organization,
            'removed_operators': [],
            'user': getattr(self.user, 'username', None),
        }

        if self.type == RequestTypeMapping.__members__['add'].value:
            for bundle in self.bundles:
                if bundle.operator:
                    rv['bundle_mapping'].setdefault(bundle.operator.name, []).append(
                        bundle.pull_specification
                    )
                rv['bundles'].append(bundle.pull_specification)
        else:
            rv['removed_operators'] = [operator.name for operator in self.operators]

        latest_state = None
        if verbose:
            states = [_state_to_json(state) for state in self.states]
            # Reverse the list since the latest states should be first
            states = list(reversed(states))
            rv['state_history'] = states
            latest_state = states[0]
        rv.update(latest_state or _state_to_json(self.state))

        return rv

    @staticmethod
    def _from_json(
        request_kwargs, additional_required_params=None, additional_optional_params=None
    ):
        """
        Validate and process request agnostic parameters

        As part of the processing, the input ``request_kwargs`` parameter
        is updated to reference database objects where appropriate.

        :param dict request_kwargs: copy of args provided in API request
        """
        # Validate all required parameters are present
        required_params = {'binary_image'} | set(additional_required_params or [])
        optional_params = {'add_arches'} | set(additional_optional_params or [])

        missing_params = required_params - request_kwargs.keys()
        if missing_params:
            raise ValidationError(
                'Missing required parameter(s): {}'.format(', '.join(missing_params))
            )

        # Don't allow the user to set arbitrary columns or relationships
        invalid_params = request_kwargs.keys() - required_params - optional_params
        if invalid_params:
            raise ValidationError(
                'The following parameters are invalid: {}'.format(', '.join(invalid_params))
            )

        # Verify that all the required paramters are set and not empty
        for param in required_params:
            if not request_kwargs.get(param):
                raise ValidationError(f'"{param}" must be set')

        # If any optional parameters are set but are empty, just remove them since they are
        # treated as null values
        for param in optional_params:
            if (
                param in request_kwargs
                and not isinstance(request_kwargs.get(param), bool)
                and not request_kwargs[param]
            ):
                del request_kwargs[param]

        # Check if both `from_index` and `add_arches` are not specified
        if not request_kwargs.get('from_index') and not request_kwargs.get('add_arches'):
            raise ValidationError('One of "from_index" or "add_arches" must be specified')

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

    @classmethod
    def from_add_json(cls, kwargs):
        """ Handles JSON requests for Add API endpoint """
        request_kwargs = deepcopy(kwargs)

        bundles = request_kwargs.get('bundles', [])
        if (
            not isinstance(bundles, list)
            or len(bundles) == 0
            or any(not item or not isinstance(item, str) for item in bundles)
        ):
            raise ValidationError(f'"bundles" should be a non-empty array of strings')

        for param in ('cnr_token', 'organization'):
            if param not in request_kwargs:
                continue

            if not isinstance(request_kwargs[param], str):
                raise ValidationError(f'"{param}" must be a string')

        # Always remove cnr_token from request_kwargs since it's not stored in the database
        request_kwargs.pop('cnr_token', None)

        cls._from_json(
            request_kwargs,
            additional_required_params=['bundles'],
            additional_optional_params=['from_index', 'organization'],
        )

        request_kwargs['bundles'] = [
            Image.get_or_create(pull_specification=item) for item in bundles
        ]

        request_kwargs['type'] = RequestTypeMapping.__members__['add'].value
        request = cls(**request_kwargs)
        request.add_state('in_progress', 'The request was initiated')
        return request

    @classmethod
    def from_remove_json(cls, kwargs):
        """ Handles JSON requests for Remove API endpoint """
        request_kwargs = deepcopy(kwargs)

        operators = request_kwargs.get('operators', [])
        if (
            not isinstance(operators, list)
            or len(operators) == 0
            or any(not item or not isinstance(item, str) for item in operators)
        ):
            raise ValidationError(f'"operators" should be a non-empty array of strings')

        cls._from_json(request_kwargs, additional_required_params=['operators', 'from_index'])

        request_kwargs['operators'] = [Operator.get_or_create(name=item) for item in operators]

        request_kwargs['type'] = RequestTypeMapping.__members__['rm'].value
        request = cls(**request_kwargs)
        request.add_state('in_progress', 'The request was initiated')
        return request


class RequestState(db.Model):
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
