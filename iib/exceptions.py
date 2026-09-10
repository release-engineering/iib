# SPDX-License-Identifier: GPL-3.0-or-later


class BaseException(Exception):
    """The base class for all IIB exceptions."""


class ConfigError(BaseException):
    """The configuration is invalid."""


class IIBError(BaseException):
    """An error was encountered in IIB."""


class FileNotFoundInImageError(IIBError):
    """
    A requested path was not present in a container image.

    Subclasses IIBError so existing "except IIBError" handlers still catch it, while
    letting callers distinguish a genuinely absent path from a real extraction failure
    (registry, OCI parsing, layer, or tar error).
    """


class ArtifactNotFoundError(IIBError):
    """
    A requested OCI artifact was not present in the registry.

    Subclasses IIBError for the same reason as FileNotFoundInImageError: callers that
    only care that the pull failed keep working, while callers that must tell "this
    image was never onboarded" apart from a registry outage or an auth failure can
    catch this specifically.
    """


class ValidationError(BaseException):
    """Denote invalid input."""


class AddressAlreadyInUse(BaseException):
    """Adress is already used by other service."""


class ExternalServiceError(BaseException):
    """An external service error occurred with HTTP 403 or HTTP 50X."""


class FinalStateOverwriteError(BaseException):
    """Unable to update state if current state is "complete" or "failed"."""
