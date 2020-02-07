# SPDX-License-Identifier: GPL-3.0-or-later
from enum import Enum

import sqlalchemy

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


class ImageArchitecture(db.Model):
    """
    An association table between images and the architectures they were built for.

    This will only be used for images built by IIB.
    """

    # A primary key is required by SQLAlchemy when using declaritive style tables, so a composite
    # primary key is used on the two required columns
    image_id = db.Column(
        db.Integer, db.ForeignKey('image.id'), autoincrement=False, index=True, primary_key=True
    )
    architecture_id = db.Column(
        db.Integer,
        db.ForeignKey('architecture.id'),
        autoincrement=False,
        index=True,
        primary_key=True,
    )

    __table_args__ = (db.UniqueConstraint('image_id', 'architecture_id'),)


class Image(db.Model):
    """
    An image that has been handled by IIB.

    This will typically point to a manifest list.
    """

    id = db.Column(db.Integer, primary_key=True)
    pull_specification = db.Column(db.String, nullable=False, unique=True)

    architectures = db.relationship('Architecture', secondary=ImageArchitecture.__table__)

    def __repr__(self):
        return '<Image pull_specification={0!r}>'.format(self.pull_specification)

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


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String, index=True, unique=True, nullable=False)
    requests = db.relationship('Request', foreign_keys=[Request.user_id], back_populates='user')
