from typing import Any, Dict, List, Optional, Union
from typing_extensions import Annotated

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    model_validator,
    SecretStr,
)

from iib.exceptions import ValidationError
from iib.common.pydantic_utils import (
    DISTRIBUTION_SCOPE_LITERAL,
    GRAPH_MODE_LITERAL,
    binary_image_check,
    distribution_scope_lower,
    get_unique_bundles,
    get_unique_deprecation_list_items,
    image_format_check,
    images_format_check,
    length_validator,
    from_index_add_arches,
    validate_graph_mode_index_image,
    validate_overwrite_params,
)

UnionPydanticRequestType = Union[
    'AddPydanticModel',
    'CreateEmptyIndexPydanticModel',
    'FbcOperationsPydanticModel',
    'MergeIndexImagePydanticModel',
    'RecursiveRelatedBundlesPydanticModel',
    'RegenerateBundlePydanticModel',
    'RmPydanticModel',
]


class PydanticModel(BaseModel):

    @classmethod
    def _get_all_keys_to_check_in_db(cls):
        raise NotImplementedError("Not implemented")

    def get_keys_to_check_in_db(self):
        """Filter keys, which need to be checked in db. Return only a keys that are set to values."""
        return [
            k for k in self._get_all_keys_to_check_in_db() if getattr(self, k, None)
        ]


class AddPydanticModel(PydanticModel):
    """Datastructure of the request to /builds/add API point."""

    add_arches: Optional[List[str]] = None
    binary_image: Annotated[
        Optional[str],
        AfterValidator(length_validator),
        AfterValidator(binary_image_check),
    ] = None
    build_tags: Optional[List[str]] = []
    bundles: Annotated[
        List[str],
        AfterValidator(length_validator),
        AfterValidator(get_unique_bundles),
        AfterValidator(images_format_check),
    ]
    cnr_token: Optional[SecretStr] = None  # deprecated
    check_related_images: Optional[bool] = False
    deprecation_list: Annotated[
        Optional[List[str]],
        AfterValidator(get_unique_deprecation_list_items),
        AfterValidator(images_format_check),
    ] = []  # deprecated
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL], BeforeValidator(distribution_scope_lower),
    ] = None
    force_backport: Optional[bool] = False  # deprecated
    from_index: Annotated[str, AfterValidator(image_format_check)]
    graph_update_mode: Optional[GRAPH_MODE_LITERAL] = None
    organization: Optional[str] = None  # deprecated
    overwrite_from_index: Optional[bool] = False
    overwrite_from_index_token: Optional[SecretStr] = None

    _from_index_add_arches_check = model_validator(mode='after')(from_index_add_arches)

    # TODO remove this comment -> Validator from RequestIndexImageMixin class
    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'AddPydanticModel':
        """Check the 'overwrite_from_index' parameter in combination with 'overwrite_from_index_token' parameter."""
        validate_overwrite_params(self.overwrite_from_index, self.overwrite_from_index_token)
        return self

    # TODO remove this comment -> Validator from RequestAdd class
    @model_validator(mode='after')
    def verify_graph_update_mode_with_index_image(self) -> 'AddPydanticModel':
        """Validate graph mode and check if index image is allowed to use different graph mode."""
        validate_graph_mode_index_image(self.graph_update_mode, self.from_index)
        return self

    # TODO remove this comment -> Validator from RequestAdd class
    @model_validator(mode='after')
    def from_index_needed_if_no_bundles(self) -> 'AddPydanticModel':
        """
        Check if no bundles and `from_index is specified

        if no bundles and no from index then an empty index will be created which is a no-op
        """
        if not (self.bundles or self.from_index):
            raise ValidationError('"from_index" must be specified if no bundles are specified')
        return self

    # TODO remove this comment -> Validator from RequestADD class
    @model_validator(mode='after')
    def bundles_needed_with_check_related_images(self) -> 'AddPydanticModel':
        """Verify that `check_related_images` is specified when bundles are specified"""
        if self.check_related_images and not self.bundles:
            raise ValidationError(
                '"check_related_images" must be specified only when bundles are specified'
            )
        return self

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=[
                "add_arches",
                "build_tags",
                "cnr_token",
                "force_backport",
                "overwrite_from_index",
                "overwrite_from_index_token",
            ],
            exclude_defaults=True,
        )


    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "bundles", "deprecation_list", "from_index"]


class RmPydanticModel(PydanticModel):
    """Datastructure of the request to /builds/rm API point."""

    add_arches: Optional[List[str]] = None
    binary_image: Annotated[
        Optional[str],
        AfterValidator(binary_image_check),
    ] = None
    build_tags: Optional[List[str]] = []
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL], BeforeValidator(distribution_scope_lower),
    ] = None
    from_index: Annotated[str, AfterValidator(image_format_check)]
    operators: Annotated[List[str], AfterValidator(length_validator)]
    overwrite_from_index: Optional[bool] = False
    overwrite_from_index_token: Optional[SecretStr] = None

    _from_index_add_arches_check = model_validator(mode='after')(from_index_add_arches)

    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'RmPydanticModel':
        validate_overwrite_params(self.overwrite_from_index, self.overwrite_from_index_token)
        return self

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=["add_arches", "build_tags", "overwrite_from_index", "overwrite_from_index_token"],
            exclude_defaults=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "from_index", "operators"]


class AddRmBatchPydanticModel(BaseModel):
    annotations: Dict[str, Any]
    build_requests: List[Union[AddPydanticModel, RmPydanticModel]]


class RegistryAuth(BaseModel):
    auth: SecretStr


class RegistryAuths(BaseModel):  # is {"auths":{}} allowed?
    auths: Annotated[Dict[SecretStr, RegistryAuth], AfterValidator(length_validator)]


class RegenerateBundlePydanticModel(PydanticModel):
    """Datastructure of the request to /builds/regenerate-bundle API point."""

    # BUNDLE_IMAGE, from_bundle_image_resolved, build_tags?
    bundle_replacements: Optional[Dict[str, str]] = {}
    from_bundle_image: Annotated[str, AfterValidator(image_format_check)]
    organization: Optional[str] = None
    registry_auths: Optional[RegistryAuths] = None  # not in db

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=["registry_auths"],
            exclude_defaults=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["from_bundle_image"]


class RegenerateBundleBatchPydanticModel(BaseModel):
    build_requests: List[RegenerateBundlePydanticModel]
    annotations: Dict[str, Any]


class MergeIndexImagePydanticModel(PydanticModel):
    """Datastructure of the request to /builds/regenerate-bundle API point."""

    binary_image: Annotated[
        Optional[str],
        AfterValidator(image_format_check),
        AfterValidator(binary_image_check),
    ] = None
    build_tags: Optional[List[str]] = []
    deprecation_list: Annotated[
        Optional[List[str]],
        AfterValidator(get_unique_deprecation_list_items),
        AfterValidator(images_format_check),
    ] = []
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL], BeforeValidator(distribution_scope_lower),
    ] = None
    graph_update_mode: Optional[GRAPH_MODE_LITERAL] = None
    overwrite_target_index: Optional[bool] = False  # Why do we need this bool? Isn't the token enough?
    overwrite_target_index_token: Optional[SecretStr] = None
    source_from_index: Annotated[str, AfterValidator(image_format_check)]
    target_index: Annotated[Optional[str], AfterValidator(image_format_check)] = None
    batch: Optional[str] = None  # TODO Not sure with presence
    user: Optional[str] = None  # TODO Not sure with presence

    @model_validator(mode='after')
    def verify_graph_update_mode_with_target_index(self) -> 'MergeIndexImagePydanticModel':
        validate_graph_mode_index_image(self.graph_update_mode, self.target_index)
        return self

    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'MergeIndexImagePydanticModel':
        validate_overwrite_params(
            self.overwrite_target_index,
            self.overwrite_target_index_token,
            disable_auth_check=True,
        )
        return self

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=["build_tags", "overwrite_target_index", "overwrite_target_index_token"],
            exclude_defaults=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "deprecation_list", "source_from_index", "target_index", "target_index"]


class CreateEmptyIndexPydanticModel(PydanticModel):
    """Datastructure of the request to /builds/regenerate-bundle API point."""

    binary_image: Annotated[
        Optional[str],
        AfterValidator(image_format_check),
        AfterValidator(binary_image_check),
    ] = None
    from_index: Annotated[
        str,
        AfterValidator(image_format_check),
        AfterValidator(length_validator),
    ]
    labels: Optional[Dict[str, str]] = {}
    output_fbc: Optional[bool] = False

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude_defaults=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "from_index"]


class RecursiveRelatedBundlesPydanticModel(PydanticModel):
    organization: Optional[str] = None
    parent_bundle_image: Annotated[
        str,
        AfterValidator(image_format_check),
        AfterValidator(length_validator),
    ]
    registry_auths: Optional[RegistryAuths] = None  # not in db

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=["registry_auths"],
            exclude_defaults=True,
        )


    def _get_all_keys_to_check_in_db(self):
        return ["parent_bundle_image"]


class FbcOperationsPydanticModel(PydanticModel):
    add_arches: Optional[List[str]] = []
    binary_image: Annotated[
        Optional[str],
        AfterValidator(image_format_check),
        AfterValidator(binary_image_check),
    ] = None
    bundles: Annotated[
        Optional[List[str]],
        AfterValidator(length_validator),
        AfterValidator(get_unique_bundles),
        AfterValidator(images_format_check),
    ] = []
    build_tags: Optional[List[str]] = []
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL], BeforeValidator(distribution_scope_lower),
    ] = None
    fbc_fragment: Annotated[
        str,
        AfterValidator(image_format_check),
        AfterValidator(length_validator),
    ]
    from_index: Annotated[
        str,
        AfterValidator(image_format_check),
        AfterValidator(length_validator),
    ]
    organization: Optional[str] = None
    overwrite_from_index: Optional[bool] = False
    overwrite_from_index_token: Optional[SecretStr] = None

    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'FbcOperationsPydanticModel':
        validate_overwrite_params(self.overwrite_from_index, self.overwrite_from_index_token)
        return self

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=["add_arches", "build_tags", "overwrite_from_index", "overwrite_from_index_token"],
            exclude_defaults=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "bundles", "fbc_fragment", "from_index"]
