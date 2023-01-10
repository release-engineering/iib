# SPDX-License-Identifier: GPL-3.0-or-later
from typing import Any, Dict, List, NamedTuple, Optional, Union, Sequence, Set
from typing_extensions import NotRequired, TypedDict, Literal

from proton._message import Message

IIB_BINARY_IMAGE_CONFIG_TYPE = Dict[str, Dict[str, str]]


class PaginationMetadata(TypedDict):
    """Datastructure of the metadata about the paginated query."""

    first: str
    last: str
    next: Optional[str]
    page: int
    pages: int
    per_page: int
    previous: Optional[str]
    total: int


class AddressMessageEnvelope(NamedTuple):
    """Datastructure of the tuple with target address and proton message."""

    address: str
    message: Message


class RelatedBundlesMetadata(TypedDict):
    """Datastructure of Metadata associated with related bundles."""

    expiration: str
    url: str


#  Start of the Payloads Part

# try inheritance from other payloads

PayloadTags = Literal[
    'AddRequestPayload',
    'RmRequestPayload',
    'RegenerateBundlePayload',
    'RegenerateBundleBatchPayload',
    'AddRmBatchPayload',
    'MergeIndexImagesPayload',
    'CreateEmptyIndexPayload',
]


PossiblePayloadParameters = Sequence[
    Literal[
        'add_arches',
        'annotations',
        'batch',
        'binary_image',
        'build_requests',
        'build_tags',
        'bundles',
        'cnr_token',
        'deprecation_list',
        'distribution_scope',
        'force_backport',
        'from_bundle_image',
        'from_index',
        'labels',
        'operators',
        'organization',
        'output_fbc',
        'overwrite_from_index',
        'overwrite_from_index_token',
        'registry_auths',
        'related_bundles',
        'source_from_index',
        'target_index',
        'user',
    ]
]


class AddRequestPayload(TypedDict):
    """Datastructure of the request to /builds/add API point."""

    add_arches: NotRequired[List[str]]
    binary_image: NotRequired[str]
    build_tags: NotRequired[List[str]]
    bundles: List[str]
    cnr_token: NotRequired[str]
    deprecation_list: NotRequired[List[str]]
    distribution_scope: NotRequired[str]
    force_backport: NotRequired[bool]
    from_index: NotRequired[str]
    organization: NotRequired[str]
    overwrite_from_index: NotRequired[bool]
    overwrite_from_index_token: NotRequired[str]


class RmRequestPayload(TypedDict):
    """Datastructure of the request to /builds/rm API point."""

    add_arches: NotRequired[List[str]]
    binary_image: NotRequired[str]
    build_tags: NotRequired[List[str]]
    distribution_scope: NotRequired[str]
    from_index: str
    operators: List[str]
    overwrite_from_index: NotRequired[bool]
    overwrite_from_index_token: Optional[str]


class RegenerateBundlePayload(TypedDict):
    """Datastructure of the request to /builds/regenerate-bundle API point."""

    from_bundle_image: str
    organization: NotRequired[str]
    registry_auths: NotRequired[Dict[str, Any]]
    related_bundles: NotRequired[RelatedBundlesMetadata]
    user: NotRequired[str]
    batch: NotRequired[str]


class RegenerateBundleBatchPayload(TypedDict):
    """Datastructure of the request to /builds/regenerate-bundle-batch API point."""

    annotations: NotRequired[Dict[str, Any]]
    build_requests: List[RegenerateBundlePayload]


class AddRmBatchPayload(TypedDict):
    """Datastructure of the request to /builds/add-rm-batch API point."""

    annotations: NotRequired[Dict[str, Any]]
    build_requests: List[Union[AddRequestPayload, RmRequestPayload]]


class MergeIndexImagesPayload(TypedDict):
    """Datastructure of the request to /builds/merge-index-image API point."""

    binary_image: NotRequired[str]
    build_tags: NotRequired[List[str]]
    deprecation_list: NotRequired[List[str]]
    distribution_scope: NotRequired[str]
    overwrite_target_index: NotRequired[bool]
    overwrite_target_index_token: NotRequired[str]
    source_from_index: str
    target_index: NotRequired[str]
    batch: NotRequired[str]
    user: NotRequired[str]


class CreateEmptyIndexPayload(TypedDict):
    """Datastructure of the request to /builds/create-empty-index API point."""

    binary_image: NotRequired[str]
    from_index: str
    labels: NotRequired[Dict[str, str]]
    output_fbc: NotRequired[bool]


class RecursiveRelatedBundlesRequestPayload(TypedDict):
    """Datastructure of the request to /builds/recursive-related-bundles API point."""

    batch: NotRequired[int]
    organization: NotRequired[str]
    parent_bundle_image: str
    registry_auths: NotRequired[Dict[str, Any]]
    user: NotRequired[str]


class RequestPayload(TypedDict):
    """Datastructure with all the possible keys that can API points receive."""

    add_arches: NotRequired[List[str]]
    annotations: NotRequired[Dict[str, Any]]
    batch: NotRequired[int]
    binary_image: NotRequired[str]
    build_requests: NotRequired[
        List[Union[AddRequestPayload, RmRequestPayload, RegenerateBundlePayload]]
    ]
    build_tags: NotRequired[List[str]]
    bundles: NotRequired[Optional[List[str]]]
    cnr_token: NotRequired[str]
    deprecation_list: NotRequired[List[str]]
    distribution_scope: NotRequired[str]
    force_backport: NotRequired[bool]
    from_bundle_image: NotRequired[str]
    from_index: NotRequired[str]
    labels: NotRequired[Dict[str, str]]
    operators: NotRequired[List[str]]
    organization: NotRequired[str]
    output_fbc: NotRequired[bool]
    overwrite_from_index: NotRequired[bool]
    overwrite_from_index_token: NotRequired[str]
    overwrite_target_index: NotRequired[bool]
    overwrite_target_index_token: NotRequired[str]
    registry_auths: NotRequired[Dict[str, Any]]
    related_bundles: NotRequired[RelatedBundlesMetadata]
    source_from_index: NotRequired[str]
    target_index: NotRequired[str]
    user: NotRequired[str]


PayloadTypesUnion = Union[
    AddRequestPayload,
    CreateEmptyIndexPayload,
    MergeIndexImagesPayload,
    RecursiveRelatedBundlesRequestPayload,
    RegenerateBundlePayload,
    RmRequestPayload,
]

#  End of the Payloads Part
#  Start of the RequestResponses Part


class BatchRequestResponseItem(TypedDict):
    """Datastructure of the item specifyng batch request."""

    id: int
    organization: Optional[str]
    request_type: str


class BatchRequestResponseList(TypedDict):
    """Datastructure of the group of requests."""

    batch: int
    annotations: Optional[Dict[str, Any]]
    requests: List[BatchRequestResponseItem]
    state: str
    user: Optional[str]


class BuildRequestState(TypedDict):
    """Datastructure of the state of the response to build request."""

    state: str
    state_reason: str
    updated: str


class APIPartImageBuildRequestResponse(BuildRequestState):
    """Datastructure of the API part of the responses to requests."""

    arches: Set[str]
    batch: int
    id: int
    request_type: str
    user: str
    batch_annotations: NotRequired[Dict[str, Any]]
    state_history: NotRequired[BuildRequestState]
    logs: NotRequired[RelatedBundlesMetadata]


class CommonIndexImageResponseBase(TypedDict):
    """Datastructure returned by get_common_index_image_json method."""

    binary_image: NotRequired[Optional[str]]
    binary_image_resolved: NotRequired[Optional[str]]
    build_tags: NotRequired[List[str]]
    bundle_mapping: NotRequired[Dict[str, List[str]]]
    bundles: NotRequired[List[str]]
    deprecation_list: NotRequired[List[str]]
    distribution_scope: NotRequired[str]
    from_index: NotRequired[Optional[str]]
    from_index_resolved: NotRequired[Optional[str]]
    index_image: NotRequired[Optional[str]]
    index_image_resolved: NotRequired[Optional[str]]
    internal_index_image_copy: NotRequired[Optional[str]]
    internal_index_image_copy_resolved: NotRequired[Optional[str]]
    organization: NotRequired[Optional[str]]
    removed_operators: NotRequired[List[str]]


COMMON_INDEX_IMAGE_KEYS: Sequence[
    Literal[
        'binary_image',
        'binary_image_resolved',
        'build_tags',
        'bundle_mapping',
        'bundles',
        'deprecation_list',
        'distribution_scope',
        'from_index',
        'from_index_resolved',
        'index_image',
        'index_image_resolved',
        'internal_index_image_copy',
        'internal_index_image_copy_resolved',
        'organization',
        'removed_operators',
    ]
] = (
    'binary_image',
    'binary_image_resolved',
    'build_tags',
    'bundle_mapping',
    'bundles',
    'deprecation_list',
    'distribution_scope',
    'from_index',
    'from_index_resolved',
    'index_image',
    'index_image_resolved',
    'internal_index_image_copy',
    'internal_index_image_copy_resolved',
    'organization',
    'removed_operators',
)


class AddRmRequestResponseBase(CommonIndexImageResponseBase, APIPartImageBuildRequestResponse):
    """
    Datastructure of the base class of responses to requests on multiple API points.

    Common API points are /builds/add, builds/rm, builds/add-rm-batch.

    API point builds/create-empty-index uses this type too as return value
    from get_common_index_image_json method, however bundles, bundle_mapping,
    organization, deprecation_list, removed_operators, build_tags,
    internal_index_image_copy and internal_index_image_copy_resolved keys are removed.
    """


class AddRequestResponse(AddRmRequestResponseBase):
    """Datastructure of the response to request from /builds/add API point."""

    omps_operator_version: Dict[str, Any]


class AddRmRequestResponse(AddRmRequestResponseBase):
    """Datastructure of the response to request from /builds/add-rm-batch API point."""

    omps_operator_version: Dict[str, Any]


class RegenerateBundleRequestResponse(APIPartImageBuildRequestResponse):
    """Datastructure of the response to request from /builds/regenerate-bundle API point."""

    bundle_image: Optional[str]
    bundle_replacements: Optional[Dict[str, str]]
    from_bundle_image: str
    from_bundle_image_resolved: Optional[str]
    organization: str
    related_bundles: NotRequired[RelatedBundlesMetadata]


class RegenerateBundleBatchRequestResponse(APIPartImageBuildRequestResponse):
    """Datastructure of the response to request from /builds/regenerate-bundle-batch API point."""

    bundle_image: str
    from_bundle_image: str
    from_bundle_image_resolved: str
    organization: str


class MergeIndexImageRequestResponse(APIPartImageBuildRequestResponse):
    """Datastructure of the response to request from /builds/merge-index-image API point."""

    binary_image: Optional[str]
    binary_image_resolved: Optional[str]
    build_tags: List[str]
    deprecation_list: List[str]
    distribution_scope: str
    index_image: Optional[str]
    source_from_index: str
    source_from_index_resolved: Optional[str]
    target_index: Optional[str]
    target_index_resolved: Optional[str]


class CreateEmptyIndexRequestResponse(APIPartImageBuildRequestResponse):
    """Datastructure of the response to request from /builds/create-empty-index API point."""

    binary_image: str
    binary_image_resolved: str
    distribution_scope: str
    from_index: str
    from_index_resolved: str
    index_image: str
    index_image_resolved: str
    labels: Optional[Dict[str, str]]


class RecursiveRelatedBundlesRequestResponse(APIPartImageBuildRequestResponse):
    """Datastructure of the response to request from /builds/recursive-related-bundles API point."""

    nested_bundles: RelatedBundlesMetadata
    organization: str
    parent_bundle_image: str
    parent_bundle_image_resolved: Optional[str]


class BaseClassRequestResponse(APIPartImageBuildRequestResponse, CommonIndexImageResponseBase):
    """Datastructure representing data returned by Request class to_json method."""


#  End of the RequestResponses Part
