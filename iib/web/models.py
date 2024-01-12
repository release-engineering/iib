# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
import json
from typing import Any, cast, Dict, List, Literal, Optional, Sequence, Set, Union
from abc import abstractmethod

from flask import current_app, url_for
from flask_login import UserMixin, current_user
from flask_sqlalchemy.model import DefaultMeta
import sqlalchemy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import joinedload, load_only, Mapped, validates
from sqlalchemy.orm.strategy_options import _AbstractLoad
from werkzeug.exceptions import Forbidden

from iib.exceptions import ValidationError
from iib.web import db
from iib.common.pydantic_models import (
    UnionPydanticRequestType,
)

from iib.web.iib_static_types import (
    AddRequestResponse,
    AddRmRequestResponseBase,
    BaseClassRequestResponse,
    BuildRequestState,
    CommonIndexImageResponseBase,
    CreateEmptyIndexRequestResponse,
    FbcOperationRequestResponse,
    MergeIndexImageRequestResponse,
    RecursiveRelatedBundlesRequestResponse,
    RegenerateBundleRequestResponse,
)


class BaseEnum(Enum):
    """A base class for IIB enums."""

    @classmethod
    def get_names(cls) -> List[str]:
        """
        Get a sorted list of enum names.

        :return: a sorted list of valid enum names
        :rtype: list
        """
        return sorted([e.name for e in cls])


class RequestStateMapping(BaseEnum):
    """An Enum that represents the request states."""

    in_progress: int = 1
    complete: int = 2
    failed: int = 3

    @staticmethod
    def get_final_states() -> List[str]:
        """
        Get the states that are considered final for a request.

        :return: a list of states
        :rtype: list<str>
        """
        return ['complete', 'failed']

    @classmethod
    def validate_state(cls, state: str) -> None:
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

    generic: int = 0
    add: int = 1
    rm: int = 2
    regenerate_bundle: int = 3
    merge_index_image: int = 4
    create_empty_index: int = 5
    recursive_related_bundles: int = 6
    fbc_operations: int = 7

    @classmethod
    def pretty(cls, num: int) -> str:
        """
        Return the prettified version of the enum value.

        :param int num: the enum value
        :return: the prettified string representation of the enum value
        :rtype: str
        """
        return cls(num).name.replace('_', '-')

    @classmethod
    def validate_type(cls, request_type: str) -> None:
        """
        Verify that the input request_type is valid.

        :param str request_type: the request_type to validate
        :raises iib.exceptions.ValidationError: if the request_type is invalid
        """
        prettified_request_types = [
            RequestTypeMapping.pretty(request_type.value) for request_type in RequestTypeMapping
        ]
        if request_type not in prettified_request_types:
            valid_request_types = ', '.join(prettified_request_types)
            raise ValidationError(
                f'{request_type} is not a valid build request type. Valid request_types'
                f' are: {valid_request_types}'
            )


class RequestMergeBundleDeprecation(db.Model):
    """An association table between index merge requests and bundle images which they deprecate."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    merge_index_image_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request_merge_index_image.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    bundle_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (
        db.UniqueConstraint(
            'merge_index_image_id', 'bundle_id', name='merge_index_bundle_constraint'
        ),
    )


class RequestAddBundleDeprecation(db.Model):
    """An association table between add requests and bundle images which they deprecate."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_add_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request_add.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    bundle_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (
        db.UniqueConstraint(
            'request_add_id', 'bundle_id', name='request_add_bundle_deprecation_constraint'
        ),
    )


class Architecture(db.Model):
    """An architecture associated with an image."""

    id: Mapped[int] = db.mapped_column(primary_key=True)
    name: Mapped[str] = db.mapped_column(unique=True)

    def __repr__(self) -> str:
        return '<Architecture name={0!r}>'.format(self.name)

    @staticmethod
    def validate_architecture_json(arches: List[str]) -> None:
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
    request_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, index=True, primary_key=True
    )
    architecture_id: Mapped[int] = db.mapped_column(
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

    id: Mapped[int] = db.mapped_column(primary_key=True)
    operator_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('operator.id'))
    pull_specification: Mapped[str] = db.mapped_column(index=True, unique=True)

    operator: Mapped['Operator'] = db.relationship('Operator')

    def __repr__(self) -> str:
        return '<Image pull_specification={0!r}>'.format(self.pull_specification)

    @classmethod
    def get_or_create(cls, pull_specification: str) -> Image:
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

        # cls.query triggers an auto-flush of the session by default. So if there are
        # multiple requests with same parameters submitted to IIB, call to query pre-maturely
        # flushes the contents of the session not allowing our handlers to resolve conflicts.
        # https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.params.autoflush
        with db.session.no_autoflush:
            image = cls.query.filter_by(pull_specification=pull_specification).first()

        if not image:
            image = Image(pull_specification=pull_specification)
            try:
                # This is a SAVEPOINT so that the rest of the session is not rolled back when
                # adding the image conflicts with an already existing row added by another request
                # with similar pullspecs is submitted at the same time. When the context manager
                # completes, the objects local to it are committed. If an error is raised, it
                # rolls back objects local to it while keeping the parent session unaffected.
                # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#using-savepoint
                with db.session.begin_nested():
                    db.session.add(image)
            except sqlalchemy.exc.IntegrityError:
                current_app.logger.info(
                    'Image pull specification is already in database. "%s"', pull_specification
                )
            image = cls.query.filter_by(pull_specification=pull_specification).first()

        return image


class Operator(db.Model):
    """An operator that has been handled by IIB."""

    id: Mapped[int] = db.mapped_column(primary_key=True)
    name: Mapped[str] = db.mapped_column(index=True, unique=True)

    def __repr__(self) -> str:
        return '<Operator name={0!r}>'.format(self.name)

    @classmethod
    def get_or_create(cls, name: str) -> Operator:
        """
        Get the operator from the database and create it if it doesn't exist.

        :param str name: the name of the operator
        :return: an Operator object based on the input name; the Operator object will be
            added to the database session, but not committed, if it was created
        :rtype: Operator
        """
        # cls.query triggers an auto-flush of the session by default. So if there are
        # multiple requests with same parameters submitted to IIB, call to query pre-maturely
        # flushes the contents of the session not allowing our handlers to resolve conflicts.
        # https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.params.autoflush
        with db.session.no_autoflush:
            operator = cls.query.filter_by(name=name).first()
        if not operator:
            operator = Operator(name=name)
            try:
                # This is a SAVEPOINT so that the rest of the session is not rolled back when
                # adding the image conflicts with an already existing row added by another request
                # with similar pullspecs is submitted at the same time. When the context manager
                # completes, the objects local to it are committed. If an error is raised, it
                # rolls back objects local to it while keeping the parent session unaffected.
                # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#using-savepoint
                with db.session.begin_nested():
                    db.session.add(operator)
            except sqlalchemy.exc.IntegrityError:
                current_app.logger.info('Operators is already in database. "%s"', name)
            operator = cls.query.filter_by(name=name).first()

        return operator


class BuildTag(db.Model):
    """Extra tag associated with built index image."""

    id: Mapped[int] = db.mapped_column(primary_key=True)
    name: Mapped[str] = db.mapped_column(unique=False)


class RequestBuildTag(db.Model):
    """Association table for extra build tags and build request."""

    request_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), index=True, primary_key=True
    )
    tag_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('build_tag.id'), autoincrement=False, index=True, primary_key=True
    )
    __table_args__ = (db.UniqueConstraint('request_id', 'tag_id'),)


class RequestRmOperator(db.Model):
    """An association table between rm requests and the operators they contain."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_rm_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request_rm.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    operator_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('operator.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (db.UniqueConstraint('request_rm_id', 'operator_id'),)


class RequestAddBundle(db.Model):
    """An association table between add requests and the bundles they contain."""

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    request_add_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request_add.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )
    image_id: Mapped[int] = db.mapped_column(
        db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True
    )

    __table_args__ = (db.UniqueConstraint('request_add_id', 'image_id'),)


class Request(db.Model):
    """A generic image build request."""

    __tablename__ = 'request'

    id: Mapped[int] = db.mapped_column(primary_key=True)
    architectures: Mapped[List['Architecture']] = db.relationship(
        'Architecture', order_by='Architecture.name', secondary=RequestArchitecture.__table__
    )
    batch_id: Mapped[int] = db.mapped_column(db.ForeignKey('batch.id'), index=True)
    batch: Mapped['Batch'] = db.relationship('Batch', back_populates='requests')
    request_state_id: Mapped[Optional[int]] = db.mapped_column(
        db.ForeignKey('request_state.id'), index=True, unique=True
    )
    # This maps to a value in RequestTypeMapping
    type: Mapped[int]
    user_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('user.id'))

    state: Mapped['RequestState'] = db.relationship('RequestState', foreign_keys=[request_state_id])
    states: Mapped[List['RequestState']] = db.relationship(
        'RequestState',
        foreign_keys='RequestState.request_id',
        back_populates='request',
        order_by='RequestState.updated',
    )
    user: Mapped['User'] = db.relationship('User', back_populates='requests')
    build_tags: Mapped[List['BuildTag']] = db.relationship(
        'BuildTag', order_by='BuildTag.name', secondary=RequestBuildTag.__table__
    )

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['generic'].value,
        'polymorphic_on': 'type',
    }


    @validates('type')
    def validate_type(self, key: Optional[str], type_num: int) -> int:
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

    def __repr__(self) -> str:
        return '<{0} {1!r}>'.format(self.__class__.__name__, self.id)

    def add_state(self, state: str, state_reason: str) -> None:
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

    def add_build_tag(self, name: str) -> None:
        """
        Add a RequestBuildTag associated with the current request.

        :param str name: tag name
        """
        bt = db.session.query(BuildTag).filter_by(name=name).first()
        if not bt:
            bt = BuildTag(name=name)
            db.session.add(bt)
            db.session.flush()

        if bt not in self.build_tags:
            self.build_tags.append(bt)

    def add_architecture(self, arch_name: str) -> None:
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
    def from_json_replacement(
        cls,
        payload: UnionPydanticRequestType,
        batch: Optional[Batch] = None,
        build_tags_allowed: Optional[bool] = False,
    ):
        """
        Handle JSON requests for the builds/* API endpoint.

        :param UnionPydanticRequestType payload: the Pydantic model representing the request.
        :param Batch batch: the batch to specify with the request.
        """

        keys_to_check = payload.get_keys_to_check_in_db()
        for key in keys_to_check:
            if key in [
                'binary_image',
                'fbc_fragment',
                'from_index',
                'from_bundle_image',
                'source_from_index',
                'target_index',
                'parent_bundle_image',
            ]:
                payload.__setattr__(key, Image.get_or_create(pull_specification=payload.__getattribute__(key)))

            elif key in ["bundles", "deprecation_list"]:
                payload.__setattr__(key, [
                    Image.get_or_create(pull_specification=image) for image in payload.__getattribute__(key)
                ])

            elif key == "operators":
                payload.__setattr__(key, [Operator.get_or_create(name=item) for item in payload.__getattribute__(key)])

            else:
                raise ValidationError(f"Unexpected key: {key} during from_json() method.")

        request_kwargs = payload.get_json_for_request()

        # current_user.is_authenticated is only ever False when auth is disabled
        if current_user.is_authenticated:
            request_kwargs['user'] = current_user

        # Add the request to a new batch

        batch = batch or Batch()
        db.session.add(batch)
        request_kwargs['batch'] = batch

        request = cls(**request_kwargs)

        if build_tags_allowed:
            for bt in payload.build_tags:
                request.add_build_tag(bt)

        request.add_state('in_progress', 'The request was initiated')
        return request


    # return value is BaseClassRequestResponse, however because of LSP, we need other types here too
    def to_json(
        self,
        verbose: Optional[bool] = True,
    ) -> Union[
        AddRequestResponse,
        AddRmRequestResponseBase,
        BaseClassRequestResponse,
        CreateEmptyIndexRequestResponse,
        MergeIndexImageRequestResponse,
        RecursiveRelatedBundlesRequestResponse,
        RegenerateBundleRequestResponse,
        FbcOperationRequestResponse,
    ]:
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

        def _state_to_json(state: RequestState) -> BuildRequestState:
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
            if (
                current_app.config['IIB_REQUEST_LOGS_DIR']
                or current_app.config['IIB_AWS_S3_BUCKET_NAME']
            ):
                rv['logs'] = {
                    'expiration': self.temporary_data_expiration.isoformat() + 'Z',
                    'url': url_for('.get_build_logs', request_id=self.id, _external=True),
                }
        rv.update(latest_state or _state_to_json(self.state))

        # cast from Dict[str, Any] - sooner cast would require less strict types
        return cast(BaseClassRequestResponse, rv)

    def get_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        return {'arches', 'state', 'state_reason'}

    @property
    def type_name(self) -> str:
        """
        Get the request's type as a string.

        :return: the request's type
        :rtype: str
        """
        return RequestTypeMapping.pretty(self.type)

    @property
    def temporary_data_expiration(self) -> datetime:
        """
        Return the timestamp of when logs and related_bundles are considered expired.

        :return: temporary data expiration timestamp
        :rtype: str
        """
        data_lifetime = timedelta(days=current_app.config['IIB_REQUEST_DATA_DAYS_TO_LIVE'])
        return self.state.updated + data_lifetime


class Batch(db.Model):
    """A batch associated with one or more requests."""

    id: Mapped[int] = db.mapped_column(primary_key=True)
    _annotations: Mapped[Optional[str]] = db.mapped_column('annotations', db.Text)

    requests: Mapped[List['Request']] = db.relationship(
        'Request', foreign_keys=[Request.batch_id], back_populates='batch', order_by='Request.id'
    )

    @property
    def annotations(self) -> Optional[Dict[str, Any]]:
        """Return the Python representation of the JSON annotations."""
        return json.loads(self._annotations) if self._annotations else None

    @annotations.setter
    def annotations(self, annotations: Optional[Dict[str, Any]]) -> None:
        """
        Set the annotations column to the input annotations as a JSON string.

        If ``None`` is provided, it will be simply set to ``None`` and not be converted to JSON.

        :param dict annotations: the dictionary of the annotations or ``None``
        """
        self._annotations = (
            json.dumps(annotations, sort_keys=True) if annotations is not None else None
        )

    @property
    def state(self) -> str:
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
    def request_states(self) -> List[str]:
        """
        Get the states of all the requests in the batch.

        :return: the list of states
        :rtype: list<str>
        """
        # Only load the columns that are required to get the current state of the requests
        # in the batch
        requests = (
            db.session.query(Request)
            .options(
                joinedload(Request.state).load_only(RequestState.state),
                load_only(Request.id, Request.batch_id),
            )
            .filter(Request.batch_id == self.id)
            .order_by(Request.id)
            .all()
        )

        return [RequestStateMapping(request.state.state).name for request in requests]

    @property
    def user(self) -> Optional[User]:
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
    def validate_batch(batch_id: Union[Optional[str], int]) -> int:
        """
        Validate the input batch ID.

        If the input batch ID is a string, it will be converted to an integer and returned.

        :param int batch_id: the ID of the batch
        :raise ValidationError: if the batch ID is invalid
        :return: the validated batch ID
        :rtype: int
        """
        rv: int
        error_msg = 'The batch must be a positive integer'
        if isinstance(batch_id, str):
            try:
                rv = int(batch_id)
            except ValueError:
                raise ValidationError(error_msg)
        elif isinstance(batch_id, int):
            rv = batch_id
        else:
            raise ValidationError(error_msg)

        if rv < 1:
            raise ValidationError(error_msg)

        return rv


def get_request_query_options(verbose: Optional[bool] = False) -> List[_AbstractLoad]:
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
        joinedload(RequestAdd.index_image_resolved),
        joinedload(RequestAdd.internal_index_image_copy),
        joinedload(RequestAdd.internal_index_image_copy_resolved),
        joinedload(RequestAdd.build_tags),
        joinedload(RequestRegenerateBundle.bundle_image),
        joinedload(RequestRegenerateBundle.from_bundle_image),
        joinedload(RequestRegenerateBundle.from_bundle_image_resolved),
        joinedload(RequestRm.binary_image),
        joinedload(RequestRm.binary_image_resolved),
        joinedload(RequestRm.from_index),
        joinedload(RequestRm.from_index_resolved),
        joinedload(RequestRm.index_image),
        joinedload(RequestRm.index_image_resolved),
        joinedload(RequestRm.internal_index_image_copy),
        joinedload(RequestRm.internal_index_image_copy_resolved),
        joinedload(RequestRm.operators),
        joinedload(RequestRm.build_tags),
        joinedload(RequestMergeIndexImage.build_tags),
        joinedload(RequestFbcOperations.fbc_fragment),
        joinedload(RequestFbcOperations.fbc_fragment_resolved),
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
    def binary_image_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of the image that the opm binary comes from."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def binary_image_resolved_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of the resolved image that the opm binary comes from."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def binary_image(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship to the image that the opm binary comes from."""
        return db.relationship('Image', foreign_keys=[cls.binary_image_id], uselist=False)

    @declared_attr
    def binary_image_resolved(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship to the resolved image that the opm binary comes from."""
        return db.relationship('Image', foreign_keys=[cls.binary_image_resolved_id], uselist=False)

    @declared_attr
    def from_index_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of the index image to base the request from."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def from_index_resolved_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of the resolved index image  to base the request from."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def from_index(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship of the index image to base the request from."""
        return db.relationship('Image', foreign_keys=[cls.from_index_id], uselist=False)

    @declared_attr
    def from_index_resolved(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship of the resolved index image to base the request from."""
        return db.relationship('Image', foreign_keys=[cls.from_index_resolved_id], uselist=False)

    @declared_attr
    def index_image_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of the built index image."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def index_image(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship to the built index image."""
        return db.relationship('Image', foreign_keys=[cls.index_image_id], uselist=False)

    @declared_attr
    def index_image_resolved_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of the resolved built index image."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def index_image_resolved(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship to the built index image."""
        return db.relationship('Image', foreign_keys=[cls.index_image_resolved_id], uselist=False)

    @declared_attr
    def internal_index_image_copy_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of IIB's internal copy of the built index image."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def internal_index_image_copy(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship to IIB's internal copy of the built index image."""
        return db.relationship(
            'Image', foreign_keys=[cls.internal_index_image_copy_id], uselist=False
        )

    @declared_attr
    def internal_index_image_copy_resolved_id(cls: DefaultMeta) -> Mapped[Optional[int]]:
        """Return the ID of resolved IIB's internal copy of the built index image."""
        return db.mapped_column(db.Integer, db.ForeignKey('image.id'))

    @declared_attr
    def internal_index_image_copy_resolved(cls: DefaultMeta) -> Mapped['Image']:
        """Return the relationship to resolved IIB's internal copy of the built index image."""
        return db.relationship(
            'Image', foreign_keys=[cls.internal_index_image_copy_resolved_id], uselist=False
        )

    @declared_attr
    def distribution_scope(cls: DefaultMeta) -> Mapped[str]:
        """Return the distribution_scope for the request."""
        return db.mapped_column(db.String, nullable=True)


    def get_common_index_image_json(self) -> CommonIndexImageResponseBase:
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
            'binary_image': getattr(self.binary_image, 'pull_specification', None),
            'binary_image_resolved': getattr(
                self.binary_image_resolved, 'pull_specification', None
            ),
            'bundle_mapping': {},
            'bundles': [],
            'deprecation_list': [],
            'from_index': getattr(self.from_index, 'pull_specification', None),
            'from_index_resolved': getattr(self.from_index_resolved, 'pull_specification', None),
            'index_image': getattr(self.index_image, 'pull_specification', None),
            'index_image_resolved': getattr(self.index_image_resolved, 'pull_specification', None),
            'internal_index_image_copy': getattr(
                self.internal_index_image_copy, 'pull_specification', None
            ),
            'internal_index_image_copy_resolved': getattr(
                self.internal_index_image_copy_resolved, 'pull_specification', None
            ),
            'organization': None,
            'removed_operators': [],
            'distribution_scope': self.distribution_scope,
            # Mypy Error: "RequestIndexImageMixin" has no attribute "build_tags"
            'build_tags': [tag.name for tag in self.build_tags],  # type: ignore
        }

    def get_index_image_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        return {
            'binary_image',
            'binary_image_resolved',
            'distribution_scope',
            'from_bundle_image_resolved',
            'from_index_resolved',
            'index_image',
            'index_image_resolved',
            'internal_index_image_copy',
            'internal_index_image_copy_resolved',
        }


class RequestAdd(Request, RequestIndexImageMixin):
    """An "add" index image build request."""

    __tablename__ = 'request_add'

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )
    bundles: Mapped[List['Image']] = db.relationship('Image', secondary=RequestAddBundle.__table__)
    check_related_images: Mapped[Optional[bool]]
    deprecation_list: Mapped[List['Image']] = db.relationship(
        'Image', secondary=RequestAddBundleDeprecation.__table__
    )
    graph_update_mode: Mapped[Optional[str]]
    organization: Mapped[Optional[str]]

    omps_operator_version: Mapped[Optional[str]]

    __mapper_args__ = {'polymorphic_identity': RequestTypeMapping.__members__['add'].value}

    def to_json(self, verbose: Optional[bool] = True) -> AddRequestResponse:
        """
        Provide the JSON representation of an "add" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast to result type, super-type returns Union
        rv = cast(AddRequestResponse, super().to_json(verbose=verbose))
        rv.update(self.get_common_index_image_json())  # type: ignore
        rv['organization'] = self.organization
        rv['omps_operator_version'] = {}
        if self.omps_operator_version:
            rv['omps_operator_version'] = json.loads(self.omps_operator_version)
        rv['graph_update_mode'] = self.graph_update_mode
        rv['check_related_images'] = self.check_related_images

        for bundle in self.bundles:
            if bundle.operator:
                rv['bundle_mapping'].setdefault(bundle.operator.name, []).append(
                    bundle.pull_specification
                )
            rv['bundles'].append(bundle.pull_specification)

        rv['deprecation_list'] = [bundle.pull_specification for bundle in self.deprecation_list]

        return rv

    def get_mutable_keys(self) -> Set[str]:
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

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )
    # The ID of the index image to base the request from. This is always
    # required for "rm" requests.
    from_index_id: Mapped[int] = db.mapped_column(db.ForeignKey('image.id'))
    operators: Mapped[List['Operator']] = db.relationship(
        'Operator', secondary=RequestRmOperator.__table__
    )

    __mapper_args__ = {'polymorphic_identity': RequestTypeMapping.__members__['rm'].value}

    def to_json(self, verbose: Optional[bool] = True) -> AddRmRequestResponseBase:
        """
        Provide the JSON representation of an "rm" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast to result type, super-type returns Union
        rv = cast(AddRmRequestResponseBase, super().to_json(verbose=verbose))
        rv.update(self.get_common_index_image_json())  # type: ignore
        rv['removed_operators'] = [operator.name for operator in self.operators]

        return rv

    def get_mutable_keys(self) -> Set[str]:
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

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )
    # The ID of the regenerated bundle image
    bundle_image_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    bundle_image: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[bundle_image_id], uselist=False
    )
    # The ID of the bundle image to be regenerated
    from_bundle_image_id: Mapped[int] = db.mapped_column(db.ForeignKey('image.id'))
    from_bundle_image: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[from_bundle_image_id], uselist=False
    )
    # The ID of the resolved bundle image to be regenerated
    from_bundle_image_resolved_id: Mapped[Optional[int]] = db.mapped_column(
        db.ForeignKey('image.id')
    )
    from_bundle_image_resolved: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[from_bundle_image_resolved_id], uselist=False
    )
    # The name of the organization the bundle should be regenerated for
    organization: Mapped[Optional[str]]
    # The mapping of bundle replacements to apply to the regeneration request
    _bundle_replacements: Mapped[Optional[str]] = db.mapped_column(
        'bundle_replacements', db.VARCHAR
    )

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['regenerate_bundle'].value
    }
    build_tags = None

    @property
    def bundle_replacements(self) -> Optional[Dict[str, str]]:
        """Return the Python representation of the JSON bundle_replacements."""
        return json.loads(self._bundle_replacements) if self._bundle_replacements else {}

    @bundle_replacements.setter
    def bundle_replacements(self, bundle_replacements: Dict[str, str]) -> None:
        """
        Set the bundle_replacements column to the input bundle_replacements as a JSON string.

        If ``None`` is provided, it will be simply set to ``None`` and not be converted to JSON.

        :param dict bundle_replacements: the dictionary of the bundle_replacements or ``None``
        """
        self._bundle_replacements = (
            json.dumps(bundle_replacements, sort_keys=True) if bundle_replacements else None
        )

    def to_json(self, verbose: Optional[bool] = True) -> RegenerateBundleRequestResponse:
        """
        Provide the JSON representation of a "regenerate-bundle" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast to result type, super-type returns Union
        rv = cast(RegenerateBundleRequestResponse, super().to_json(verbose=verbose))
        rv['bundle_image'] = getattr(self.bundle_image, 'pull_specification', None)
        rv['from_bundle_image'] = self.from_bundle_image.pull_specification
        rv['from_bundle_image_resolved'] = getattr(
            self.from_bundle_image_resolved, 'pull_specification', None
        )
        rv['organization'] = self.organization
        rv['bundle_replacements'] = self.bundle_replacements
        if (
            current_app.config['IIB_REQUEST_RELATED_BUNDLES_DIR']
            or current_app.config['IIB_AWS_S3_BUCKET_NAME']
        ):
            rv['related_bundles'] = {
                'expiration': self.temporary_data_expiration.isoformat() + 'Z',
                'url': url_for('.get_related_bundles', request_id=self.id, _external=True),
            }

        return rv

    def get_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.add('bundle_image')
        rv.add('from_bundle_image_resolved')
        rv.add('bundle_replacements')
        return rv


class RequestMergeIndexImage(Request):
    """A "merge-index-image" build request."""

    __tablename__ = 'request_merge_index_image'

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )
    binary_image_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    binary_image_resolved_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    binary_image: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[binary_image_id], uselist=False
    )
    binary_image_resolved: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[binary_image_resolved_id], uselist=False
    )

    deprecation_list: Mapped[List['Image']] = db.relationship(
        'Image', secondary=RequestMergeBundleDeprecation.__table__
    )

    index_image_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    index_image: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[index_image_id], uselist=False
    )

    source_from_index_id: Mapped[int] = db.mapped_column(db.ForeignKey('image.id'))
    source_from_index_resolved_id: Mapped[Optional[int]] = db.mapped_column(
        db.ForeignKey('image.id')
    )
    source_from_index: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[source_from_index_id], uselist=False
    )
    source_from_index_resolved: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[source_from_index_resolved_id], uselist=False
    )

    target_index_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    target_index_resolved_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    target_index: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[target_index_id], uselist=False
    )
    target_index_resolved: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[target_index_resolved_id], uselist=False
    )
    distribution_scope: Mapped[Optional[str]]
    graph_update_mode: Mapped[Optional[str]]
    ignore_bundle_ocp_version: Mapped[Optional[bool]]

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['merge_index_image'].value
    }

    def to_json(self, verbose: Optional[bool] = True) -> MergeIndexImageRequestResponse:
        """
        Provide the JSON representation of an "merge-index-image" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast to result type, super-type returns Union
        rv = cast(MergeIndexImageRequestResponse, super().to_json(verbose=verbose))
        rv['binary_image'] = getattr(self.binary_image, 'pull_specification', None)
        rv['binary_image_resolved'] = getattr(
            self.binary_image_resolved, 'pull_specification', None
        )
        rv['deprecation_list'] = [bundle.pull_specification for bundle in self.deprecation_list]
        rv['graph_update_mode'] = self.graph_update_mode
        rv['ignore_bundle_ocp_version'] = self.ignore_bundle_ocp_version
        rv['index_image'] = getattr(self.index_image, 'pull_specification', None)
        rv['source_from_index'] = self.source_from_index.pull_specification
        rv['source_from_index_resolved'] = getattr(
            self.source_from_index_resolved, 'pull_specification', None
        )
        rv['target_index'] = getattr(self.target_index, 'pull_specification', None)
        rv['target_index_resolved'] = getattr(
            self.target_index_resolved, 'pull_specification', None
        )
        rv['distribution_scope'] = self.distribution_scope
        rv['build_tags'] = [t.name for t in self.build_tags]

        return rv

    def get_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.update(
            {
                'binary_image',
                'binary_image_resolved',
                'distribution_scope',
                'index_image',
                'source_from_index_resolved',
                'target_index_resolved',
            }
        )
        return rv


class RequestState(db.Model):
    """Represents a state (historical or present) of a request."""

    id: Mapped[int] = db.mapped_column(primary_key=True)
    request_id: Mapped[int] = db.mapped_column(db.ForeignKey('request.id'), index=True)
    # This maps to a value in RequestStateMapping
    state: Mapped[int]
    state_reason: Mapped[str]
    updated: Mapped[datetime] = db.mapped_column(db.DateTime(), default=sqlalchemy.func.now())

    request: Mapped['Request'] = db.relationship(
        'Request', foreign_keys=[request_id], back_populates='states'
    )

    @property
    def state_name(self) -> Optional[str]:
        """Get the state's display name."""
        if self.state:
            return RequestStateMapping(self.state).name
        return None

    def __repr__(self) -> str:
        return '<RequestState id={} state="{}" request_id={}>'.format(
            self.id, self.state_name, self.request_id
        )


class User(db.Model, UserMixin):
    """Represents an external user that owns an IIB request."""

    id: Mapped[int] = db.mapped_column(primary_key=True)
    username: Mapped[str] = db.mapped_column(index=True, unique=True)
    requests: Mapped[List['Request']] = db.relationship(
        'Request', foreign_keys=[Request.user_id], back_populates='user'
    )

    @classmethod
    def get_or_create(cls, username: str) -> User:
        """
        Get the user from the database and create it if it doesn't exist.

        :param str username: the username of the user
        :return: a User object based on the input username; the User object will be
            added to the database session, but not committed, if it was created
        :rtype: User
        """
        # cls.query triggers an auto-flush of the session by default. So if there are
        # multiple requests with same parameters submitted to IIB, call to query pre-maturely
        # flushes the contents of the session not allowing our handlers to resolve conflicts.
        # https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.params.autoflush
        with db.session.no_autoflush:
            user = cls.query.filter_by(username=username).first()
        if not user:
            user = User(username=username)
            try:
                # This is a SAVEPOINT so that the rest of the session is not rolled back when
                # adding the image conflicts with an already existing row added by another request
                # with similar pullspecs is submitted at the same time. When the context manager
                # completes, the objects local to it are committed. If an error is raised, it
                # rolls back objects local to it while keeping the parent session unaffected.
                # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#using-savepoint
                with db.session.begin_nested():
                    db.session.add(user)
            except sqlalchemy.exc.IntegrityError:
                current_app.logger.info('User is already in database. "%s"', username)
            user = cls.query.filter_by(username=username).first()

        return user


class RequestCreateEmptyIndex(Request, RequestIndexImageMixin):
    """An "create-empty-index" image build request."""

    __tablename__ = 'request_create_empty_index'

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )
    _labels: Mapped[Optional[str]] = db.mapped_column('labels', db.Text)

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['create_empty_index'].value
    }
    build_tags = None
    output_fbc = False

    @property
    def labels(self) -> Optional[Dict[str, str]]:
        """Return the Python representation of the JSON labels."""
        return json.loads(self._labels) if self._labels else None

    @labels.setter
    def labels(self, labels: Optional[Dict[str, Any]]) -> None:
        """
        Set the labels column to the input labels as a JSON string.

        If ``None`` is provided, it will be simply set to ``None`` and not be converted to JSON.

        :param dict labels: the dictionary of the labels or ``None``
        """
        self._labels = json.dumps(labels, sort_keys=True) if labels is not None else None

    def to_json(self, verbose: Optional[bool] = True) -> CreateEmptyIndexRequestResponse:
        """
        Provide the JSON representation of an "create-empty-index" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast from Union, see Request.to_json,
        # because of pop methods in the method BaseClassRequestResponse is better
        rv = cast(BaseClassRequestResponse, super().to_json(verbose=verbose))
        rv.update(self.get_common_index_image_json())  # type: ignore
        rv.pop('bundles')
        rv.pop('bundle_mapping')
        rv.pop('organization')
        rv.pop('deprecation_list')
        rv.pop('removed_operators')
        rv.pop('build_tags')
        rv.pop('internal_index_image_copy')
        rv.pop('internal_index_image_copy_resolved')
        # cast to result type
        result = cast(CreateEmptyIndexRequestResponse, rv)
        result['labels'] = self.labels
        return result

    def get_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.update(self.get_index_image_mutable_keys())
        rv.update('labels')
        rv.remove('from_bundle_image_resolved')
        rv.remove('internal_index_image_copy')
        rv.remove('internal_index_image_copy_resolved')
        return rv


class RequestRecursiveRelatedBundles(Request):
    """A "recursive_related_bundles" image build request."""

    __tablename__ = 'request_recursive_related_bundles'

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )
    # The ID of the parent bundle image
    parent_bundle_image_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    parent_bundle_image: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[parent_bundle_image_id], uselist=False
    )
    # The ID of the resolved parent bundle image
    parent_bundle_image_resolved_id: Mapped[Optional[int]] = db.mapped_column(
        db.ForeignKey('image.id')
    )
    parent_bundle_image_resolved: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[parent_bundle_image_resolved_id], uselist=False
    )
    # The name of the organization the related bundles should be found for
    organization: Mapped[Optional[str]]

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['recursive_related_bundles'].value
    }
    build_tags = None

    def to_json(self, verbose: Optional[bool] = True) -> RecursiveRelatedBundlesRequestResponse:
        """
        Provide the JSON representation of a "recursive-related-bundles" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast to result type, super-type returns Union
        rv = cast(RecursiveRelatedBundlesRequestResponse, super().to_json(verbose=verbose))
        rv['parent_bundle_image'] = self.parent_bundle_image.pull_specification
        rv['parent_bundle_image_resolved'] = getattr(
            self.parent_bundle_image_resolved, 'pull_specification', None
        )

        rv['organization'] = self.organization
        rv['nested_bundles'] = {
            'expiration': self.temporary_data_expiration.isoformat() + 'Z',
            'url': url_for('.get_nested_bundles', request_id=self.id, _external=True),
        }

        return rv

    def get_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.add('parent_bundle_image_resolved')
        return rv


class RequestFbcOperations(Request, RequestIndexImageMixin):
    """FBC operation build request."""

    __tablename__ = 'request_fbc_operations'

    id: Mapped[int] = db.mapped_column(
        db.ForeignKey('request.id'), autoincrement=False, primary_key=True
    )

    fbc_fragment_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    fbc_fragment_resolved_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
    fbc_fragment: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[fbc_fragment_id], uselist=False
    )
    fbc_fragment_resolved: Mapped['Image'] = db.relationship(
        'Image', foreign_keys=[fbc_fragment_resolved_id], uselist=False
    )

    __mapper_args__ = {
        'polymorphic_identity': RequestTypeMapping.__members__['fbc_operations'].value
    }

    def to_json(self, verbose: Optional[bool] = True) -> FbcOperationRequestResponse:
        """
        Provide the JSON representation of a "fbc-operation" build request.

        :param bool verbose: determines if the JSON output should be verbose
        :return: a dictionary representing the JSON of the build request
        :rtype: dict
        """
        # cast to result type, super-type returns Union
        rv = cast(FbcOperationRequestResponse, super().to_json(verbose=verbose))
        rv.update(self.get_common_index_image_json())  # type: ignore
        rv['fbc_fragment'] = self.fbc_fragment.pull_specification
        rv['fbc_fragment_resolved'] = getattr(
            self.fbc_fragment_resolved, 'pull_specification', None
        )

        rv.pop('bundles')
        rv.pop('bundle_mapping')
        rv.pop('deprecation_list')
        rv.pop('organization')
        rv.pop('removed_operators')

        return rv

    def get_mutable_keys(self) -> Set[str]:
        """
        Return the set of keys representing the attributes that can be modified.

        :return: a set of key names
        :rtype: set
        """
        rv = super().get_mutable_keys()
        rv.update(self.get_index_image_mutable_keys())
        rv.add('fbc_fragment_resolved')
        return rv
