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


class PydanticRequestBaseModel(BaseModel):
    """Base model representing IIB request."""

    @classmethod
    def _get_all_keys_to_check_in_db(cls):
        """Class that returns request specific keys to check."""
        raise NotImplementedError("Not implemented")

    def get_keys_to_check_in_db(self):
        """
        Filter keys, which need to be checked in db.

        Return only a keys that are set to values.
        """
        return [k for k in self._get_all_keys_to_check_in_db() if getattr(self, k, None)]


class AddPydanticModel(PydanticRequestBaseModel):
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
    # TODO remove this comment -> old request without this parameter will not have False but None
    check_related_images: Optional[bool] = None
    deprecation_list: Annotated[
        Optional[List[str]],
        AfterValidator(get_unique_deprecation_list_items),
        AfterValidator(images_format_check),
    ] = []  # deprecated
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL],
        BeforeValidator(distribution_scope_lower),
    ] = None
    force_backport: Optional[bool] = False  # deprecated
    from_index: Annotated[Optional[str], AfterValidator(image_format_check)] = None
    graph_update_mode: Optional[GRAPH_MODE_LITERAL] = None
    organization: Optional[str] = None  # deprecated
    overwrite_from_index: Optional[bool] = False
    overwrite_from_index_token: Optional[SecretStr] = None

    @model_validator(mode='after')
    def verify_from_index_add_arches_combination(self) -> 'AddPydanticModel':
        """Check the 'overwrite_from_index' parameter with 'overwrite_from_index_token' param."""
        from_index_add_arches(self.from_index, self.add_arches)
        return self

    # TODO remove this comment -> Validator from RequestIndexImageMixin class
    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'AddPydanticModel':
        """Check the 'overwrite_from_index' parameter with 'overwrite_from_index_token' param."""
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
        Check if no bundles and `from_index is specified.

        if no bundles and no from index then an empty index will be created which is a no-op
        """
        if not (self.bundles or self.from_index):
            raise ValidationError('"from_index" must be specified if no bundles are specified')
        return self

    # TODO remove this comment -> Validator from RequestADD class
    @model_validator(mode='after')
    def bundles_needed_with_check_related_images(self) -> 'AddPydanticModel':
        """Verify that `check_related_images` is specified when bundles are specified."""
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
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "bundles", "deprecation_list", "from_index"]


class RmPydanticModel(PydanticRequestBaseModel):
    """Datastructure of the request to /builds/rm API point."""

    add_arches: Optional[List[str]] = None
    binary_image: Annotated[
        Optional[str],
        AfterValidator(binary_image_check),
    ] = None
    build_tags: Optional[List[str]] = []
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL],
        BeforeValidator(distribution_scope_lower),
    ] = None
    from_index: Annotated[Optional[str], AfterValidator(image_format_check)] = None
    operators: Annotated[List[str], AfterValidator(length_validator)]
    overwrite_from_index: Optional[bool] = False
    overwrite_from_index_token: Optional[SecretStr] = None

    @model_validator(mode='after')
    def verify_from_index_add_arches_combination(self) -> 'AddPydanticModel':
        """Check the 'overwrite_from_index' parameter with 'overwrite_from_index_token' param."""
        from_index_add_arches(self.from_index, self.add_arches)
        return self

    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'RmPydanticModel':
        """Validate overwrite_from_index and overwrite_from_index_token param combination."""
        validate_overwrite_params(
            self.overwrite_from_index,
            self.overwrite_from_index_token,
        )
        return self

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=[
                "add_arches",
                "build_tags",
                "overwrite_from_index",
                "overwrite_from_index_token",
            ],
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "from_index", "operators"]


class AddRmBatchPydanticModel(BaseModel):
    """Datastructure of the request to /builds/add-rm-batch API point."""

    annotations: Dict[str, Any]
    build_requests: List[Union[AddPydanticModel, RmPydanticModel]]


class RegistryAuth(BaseModel):
    """Datastructure representing private registry token."""

    auth: SecretStr


class RegistryAuths(BaseModel):
    """
    Datastructure used within recursive-related-bundles.

    Provide the dockerconfig.json for authentication to private registries.
    Non-auth information in the dockerconfig.json is not allowed.
    """

    auths: Annotated[Dict[SecretStr, RegistryAuth], AfterValidator(length_validator)]


class RegenerateBundlePydanticModel(PydanticRequestBaseModel):
    """Datastructure of the request to /builds/regenerate-bundle API point."""

    # BUNDLE_IMAGE, from_bundle_image_resolved, build_tags?
    bundle_replacements: Optional[Dict[str, str]] = None
    from_bundle_image: Annotated[str, AfterValidator(image_format_check)]
    organization: Optional[str] = None
    registry_auths: Optional[RegistryAuths] = None  # not in db

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=["registry_auths"],
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["from_bundle_image"]


class RegenerateBundleBatchPydanticModel(BaseModel):
    """Datastructure of the request to /builds/regenerate-bundle-batch API point."""

    build_requests: List[RegenerateBundlePydanticModel]
    annotations: Dict[str, Any]


class MergeIndexImagePydanticModel(PydanticRequestBaseModel):
    """Datastructure of the request to /builds/merge-index-image API point."""

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
        Optional[DISTRIBUTION_SCOPE_LITERAL],
        BeforeValidator(distribution_scope_lower),
    ] = None
    graph_update_mode: Optional[GRAPH_MODE_LITERAL] = None
    overwrite_target_index: Optional[bool] = False
    overwrite_target_index_token: Optional[SecretStr] = None
    source_from_index: Annotated[str, AfterValidator(image_format_check)]
    target_index: Annotated[Optional[str], AfterValidator(image_format_check)] = None
    batch: Optional[str] = None  # TODO Not sure with presence
    user: Optional[str] = None  # TODO Not sure with presence

    @model_validator(mode='after')
    def verify_graph_update_mode_with_target_index(self) -> 'MergeIndexImagePydanticModel':
        """Validate graph_update_mode with target_index param combination."""
        validate_graph_mode_index_image(self.graph_update_mode, self.target_index)
        return self

    @model_validator(mode='after')
    def verify_overwrite_from_index_token(self) -> 'MergeIndexImagePydanticModel':
        """Validate overwrite_target_index with overwrite_target_index_token param combination."""
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
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return [
            "binary_image",
            "deprecation_list",
            "source_from_index",
            "target_index",
            "target_index",
        ]


class CreateEmptyIndexPydanticModel(PydanticRequestBaseModel):
    """Datastructure of the request to /builds/create-empty-index API point."""

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
    # TODO (remove comment) old request without this parameter will not have empty labels
    labels: Optional[Dict[str, str]] = None
    # TODO (remove comment) old request without this parameter will not have empty output_fbc
    output_fbc: Optional[bool] = None

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "from_index"]


class RecursiveRelatedBundlesPydanticModel(PydanticRequestBaseModel):
    """Datastructure of the request to /builds/recursive-related-bundles API point."""

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
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["parent_bundle_image"]


class FbcOperationsPydanticModel(PydanticRequestBaseModel):
    """Datastructure of the request to /builds/fbc-operations API point."""

    add_arches: Optional[List[str]] = []
    binary_image: Annotated[
        Optional[str],
        AfterValidator(image_format_check),
        AfterValidator(binary_image_check),
    ] = None
    # TODO (remove comment) old request without this parameter will not have empty list but None
    bundles: Annotated[
        Optional[List[str]],
        AfterValidator(length_validator),
        AfterValidator(get_unique_bundles),
        AfterValidator(images_format_check),
    ] = None
    build_tags: Optional[List[str]] = []
    distribution_scope: Annotated[
        Optional[DISTRIBUTION_SCOPE_LITERAL],
        BeforeValidator(distribution_scope_lower),
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
        """Validate overwrite_from_index and overwrite_from_index_token param combination."""
        validate_overwrite_params(self.overwrite_from_index, self.overwrite_from_index_token)
        return self

    def get_json_for_request(self):
        """Return json with the parameters we store in the db."""
        return self.model_dump(
            exclude=[
                "add_arches",
                "build_tags",
                "overwrite_from_index",
                "overwrite_from_index_token",
            ],
            exclude_none=True,
        )

    def _get_all_keys_to_check_in_db(self):
        return ["binary_image", "bundles", "fbc_fragment", "from_index"]
