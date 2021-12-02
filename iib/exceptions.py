# SPDX-License-Identifier: GPL-3.0-or-later


class BaseException(Exception):
    """The base class for all IIB exceptions."""


class ConfigError(BaseException):
    """The configuration is invalid."""


class IIBError(BaseException):
    """An error was encountered in IIB."""


class ValidationError(BaseException):
    """Denote invalid input."""


class AddressAlreadyInUse(BaseException):
    """Adress is already used by other service."""


class ExternalServiceError(IIBError):
    """An external service error occurred with HTTP 50X."""
