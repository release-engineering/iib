# Endpoint Parameter Reference (Containerized Request Types)

## `POST /builds/add`
| Parameter | Required | Type |
|---|---|---|
| `bundles` | Yes | `List[str]` — bundle image pull specs |
| `from_index` | Yes | `str` — index image to add to |
| `binary_image` | No | `str` — builder image |
| `add_arches` | No | `List[str]` — architectures |
| `overwrite_from_index` | No | `bool` |
| `overwrite_from_index_token` | No | `str` |
| `distribution_scope` | No | `str` — `dev`, `stage`, or `prod` |
| `deprecation_list` | No | `List[str]` |
| `build_tags` | No | `List[str]` |
| `graph_update_mode` | No | `str` — `replaces`, `semver`, or `semver-skippatch` |
| `check_related_images` | No | `bool` |

## `POST /builds/rm`
| Parameter | Required | Type |
|---|---|---|
| `from_index` | Yes | `str` — index image to remove from |
| `operators` | Yes | `List[str]` — operator names to remove |
| `binary_image` | No | `str` |
| `add_arches` | No | `List[str]` |
| `overwrite_from_index` | No | `bool` |
| `overwrite_from_index_token` | No | `str` |
| `distribution_scope` | No | `str` |
| `build_tags` | No | `List[str]` |

## `POST /builds/regenerate-bundle`
| Parameter | Required | Type |
|---|---|---|
| `from_bundle_image` | Yes | `str` — bundle image to regenerate |
| `organization` | No | `str` |
| `registry_auths` | No | `Dict[str, Any]` |

## `POST /builds/merge-index-image`
| Parameter | Required | Type |
|---|---|---|
| `source_from_index` | Yes | `str` — source index image |
| `binary_image` | No | `str` |
| `target_index` | No | `str` |
| `overwrite_target_index` | No | `bool` |
| `overwrite_target_index_token` | No | `str` |
| `deprecation_list` | No | `List[str]` |
| `build_tags` | No | `List[str]` |
| `graph_update_mode` | No | `str` |
| `distribution_scope` | No | `str` |
| `ignore_bundle_ocp_version` | No | `bool` |

## `POST /builds/create-empty-index`
| Parameter | Required | Type |
|---|---|---|
| `from_index` | Yes | `str` — index image to base on |
| `binary_image` | No | `str` |
| `labels` | No | `Dict[str, str]` |
| `output_fbc` | No | `bool` |

## `POST /builds/fbc-operations`
| Parameter | Required | Type |
|---|---|---|
| `from_index` | Yes | `str` — FBC index image |
| `fbc_fragment` | No* | `str` — single fragment (legacy) |
| `fbc_fragments` | No* | `List[str]` — fragment images |
| `binary_image` | No | `str` |
| `add_arches` | No | `List[str]` |
| `overwrite_from_index` | No | `bool` |
| `overwrite_from_index_token` | No | `str` |
| `build_tags` | No | `List[str]` |
| `distribution_scope` | No | `str` |

*At least one of `fbc_fragment` or `fbc_fragments` is required.
