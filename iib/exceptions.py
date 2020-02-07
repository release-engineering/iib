# SPDX-License-Identifier: GPL-3.0-or-later


class BaseException(Exception):
    """The base class for all IIB exceptions."""


class ValidationError(BaseException):
    """Denote invalid input."""
