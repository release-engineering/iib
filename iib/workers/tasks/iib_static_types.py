# SPDX-License-Identifier: GPL-3.0-or-later
from typing import Dict, List, Optional, Set, Union
from typing_extensions import NotRequired, TypedDict

from operator_manifest.operator import ImageName, OperatorCSV

# Note: When IIB will be used only with python 3.11,
# we can remove Classes ending with ...Base, and those keys mark as NotRequired[...]


class BundleMetadata(TypedDict):
    """Type class referencing data of bundle used for mypy checking."""

    found_pullspecs: Set[ImageName]
    operator_csvs: List[OperatorCSV]


class IndexImageInfo(TypedDict):
    """Type class referencing data related to index image used for mypy checking."""

    arches: Set[str]
    ocp_version: str
    resolved_distribution_scope: str
    resolved_from_index: Optional[str]


class AllIndexImagesInfo(TypedDict):
    """Type class referencing group of IndexImageInfo classes used for mypy checking."""

    from_index: IndexImageInfo
    source_from_index: IndexImageInfo
    target_index: IndexImageInfo


class PrebuildInfo(TypedDict):
    """Type class referencing data related to preparation of request for building the image."""

    arches: Set[str]
    binary_image: str
    binary_image_resolved: str
    bundle_mapping: NotRequired[Dict[str, List[str]]]
    distribution_scope: str
    extra: NotRequired[str]
    from_index_resolved: NotRequired[str]
    labels: NotRequired[Optional[Dict[str, str]]]
    ocp_version: NotRequired[str]
    source_from_index_resolved: NotRequired[str]
    source_ocp_version: NotRequired[str]
    target_index_resolved: NotRequired[str]
    target_ocp_version: NotRequired[str]


class BundleImage(TypedDict):
    """Base type class referencing data related to bundle image used for mypy checking."""

    bundlePath: str
    csvName: NotRequired[str]
    packageName: str
    version: str


class UpdateRequestPayload(TypedDict, total=False):
    """Type class referencing possible parameters used with IIB API."""

    arches: NotRequired[Set[str]]
    binary_image: NotRequired[str]
    binary_image_resolved: NotRequired[str]
    bundle_image: NotRequired[str]
    bundle_mapping: NotRequired[Dict[str, List[str]]]
    distribution_scope: NotRequired[str]
    from_bundle_image_resolved: NotRequired[str]
    from_index_resolved: NotRequired[str]
    index_image: NotRequired[str]
    index_image_resolved: NotRequired[str]
    internal_index_image_copy: NotRequired[str]
    internal_index_image_copy_resolved: NotRequired[str]
    omps_operator_version: NotRequired[str]
    parent_bundle_image_resolved: NotRequired[str]
    source_from_index_resolved: NotRequired[str]
    state: NotRequired[str]
    state_reason: NotRequired[str]
    target_index_resolved: NotRequired[str]


class GreenwaveConfig(TypedDict):
    """Type class referencing configuration of Greenwawe app."""

    decision_context: str
    product_version: str
    subject_type: str


class IIBOrganizationCustomizations(TypedDict):
    """TypedDict class for typing the DevelopmentConfig class."""

    type: str


class CSVAnnotations(IIBOrganizationCustomizations):
    """TypedDict class for typing the DevelopmentConfig class."""

    annotations: Dict[str, str]


class PackageNameSuffix(IIBOrganizationCustomizations):
    """TypedDict class for typing the DevelopmentConfig class."""

    suffix: str


class ImageNameFromLabels(IIBOrganizationCustomizations):
    """TypedDict class for typing the DevelopmentConfig class."""

    template: str


class RegistryReplacements(IIBOrganizationCustomizations):
    """TypedDict class for typing the DevelopmentConfig class."""

    replacements: Dict[str, str]


class EncloseRepo(IIBOrganizationCustomizations):
    """TypedDict class for typing the DevelopmentConfig class."""

    enclosure_glue: str
    namespace: str


iib_organization_customizations_type = Dict[
    str,
    List[
        Union[
            CSVAnnotations,
            EncloseRepo,
            IIBOrganizationCustomizations,
            ImageNameFromLabels,
            PackageNameSuffix,
            RegistryReplacements,
        ]
    ],
]
