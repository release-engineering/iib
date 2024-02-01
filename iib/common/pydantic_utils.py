from typing import List, Optional, Any, Literal

import copy
from werkzeug.exceptions import Forbidden
from flask import current_app
from flask_login import current_user

from iib.exceptions import ValidationError


GRAPH_MODE_LITERAL = Literal['replaces', 'semver', 'semver-skippatch']
DISTRIBUTION_SCOPE_LITERAL = Literal['prod', 'stage', 'dev']


# TODO add regex in future to not allow following values ":s", "s:", ":"?
def image_format_check(image_name: str) -> str:
    """Check format of the index image."""
    if '@' not in image_name and ':' not in image_name:
        raise ValidationError(f'Image {image_name} should have a tag or a digest specified.')
    return image_name


def images_format_check(image_list: List[str]) -> List[str]:
    """Check multiple image names."""
    for image_name in image_list:
        image_format_check(image_name)
    return image_list


def get_unique_bundles(bundles: List[str]) -> List[str]:
    """Check and possibly remove duplicates from a list of bundles."""
    if not bundles:
        return bundles

    unique_bundles = list(set(bundles))
    if len(unique_bundles) != len(bundles):
        duplicate_bundles = copy.copy(bundles)
        for bundle in unique_bundles:
            duplicate_bundles.remove(bundle)

    #      flask.current_app.logger.info(
    #          f'Removed duplicate bundles from request: {duplicate_bundles}'
    #      )
    return unique_bundles


# RequestIndexImageMixin
def get_unique_deprecation_list_items(deprecation_list: List[str]) -> List[str]:
    """Return a list of unique items."""
    return list(set(deprecation_list))


def validate_graph_mode_index_image(
    graph_update_mode: Optional[GRAPH_MODE_LITERAL],
    index_image: Optional[str],
) -> Optional[str]:
    """
    Validate graph mode and check if index image is allowed to use different graph mode.

    :param str graph_update_mode: one of the graph mode options
    :param str index_image: pullspec of index image to which graph mode should be applied to
    :raises: ValidationError when incorrect graph_update_mode is set
    :raises: Forbidden when graph_mode can't be used for given index image
    """
    if graph_update_mode:
        allowed_from_indexes: List[str] = current_app.config['IIB_GRAPH_MODE_INDEX_ALLOW_LIST']
        if index_image not in allowed_from_indexes:
            raise Forbidden(
                '"graph_update_mode" can only be used on the'
                f' following index image: {allowed_from_indexes}'
            )
    return graph_update_mode


# RequestIndexImageMixin
def from_index_add_arches(from_index: Optional[str], add_arches: Optional[List[str]]) -> None:
    """Check if both `from_index` and `add_arches` are not specified."""
    if not from_index and not add_arches:
        raise ValidationError('One of "from_index" or "add_arches" must be specified')


# RequestIndexImageMixin
def binary_image_check(binary_image: str) -> str:
    """Validate binary_image is correctly provided."""
    if not binary_image and not current_app.config['IIB_BINARY_IMAGE_CONFIG']:
        raise ValidationError('The "binary_image" value must be a non-empty string')
    return binary_image


# RequestIndexImageMixin
def validate_overwrite_params(
    overwrite_index_image: Optional[bool],
    overwrite_index_image_token: Optional[str],
    disable_auth_check: Optional[bool] = False,
) -> None:
    """Check if both `overwrite_index_image` and `overwrite_index_image_token` are specified."""
    if overwrite_index_image_token and not overwrite_index_image:
        raise ValidationError(
            'The "overwrite_from_index" parameter is required when'
            ' the "overwrite_from_index_token" parameter is used'
        )

    # Verify the user is authorized to use overwrite_from_index
    # current_user.is_authenticated is only ever False when auth is disabled
    # TODO Remove "1 or"
    if 1 or disable_auth_check or current_user.is_authenticated:
        if overwrite_index_image and not overwrite_index_image_token:
            raise Forbidden(
                'You must set "overwrite_from_index_token" to use "overwrite_from_index"'
            )


# RequestIndexImageMixin
def distribution_scope_lower(distribution_scope: str) -> str:
    """Transform distribution_scope parameter to lowercase."""
    return distribution_scope.lower()


def length_validator(model_property: Any) -> Any:
    """Validate length of the given model property."""
    if len(model_property) == 0:
        raise ValidationError(
            f"The {type(model_property)} {model_property} should have at least 1 item."
        )
    return model_property
