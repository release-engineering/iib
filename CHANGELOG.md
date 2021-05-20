# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 4.0.0
- fixed use of token when inspecting source and target indexes in merge-index-image endpoint
- added support for substitutes-for functionality supported in OPM 1.17.0
- upgraded py from 1.9.0 to 1.10.0
- changed format of iib_organization_customizations to make it more generic
- fixed bug to re-add labels to indexes if deprecation is run in add requests
- upgraded pytest from 6.2.3 to 6.2.4
- fixed a bug where a variable isn't assigned in handle_add_request
- fixed bug to preserve double quotes when parsing YAML files in regenerate-bundle
- added support for image_name_from_labels and enclose_repo customizations in regenerate-bundle

## 3.11.2
- fixed bug to filter unique bundles from listBundles response
- added support for private registry pull secrets to regenerate bundle requests
- added podman container-tool when merging indexes
- upgraded pytest from 6.2.2 to 6.2.3
- upgraded opm in dev environment to v1.16.1
- fixed skopeo pull to retry when mediatype is none
- fixed index_image_resolved bug for merge index image endpoint
- fixed ocp_version range filter
- stopped setting content-encoding on AMQP messages

## 3.11.1
- fixed bugs for deprecating bundles in add requests
- upgraded cryptography from 3.3.1 to 3.3.2
- upgraded jinja2 from 2.11.1 to 2.11.3
- added new attribute index_image_resolved to add and rm response

## 3.11.0
- fixed docker-compose quirks
- added support for parsing bundle version in merge-index-image before adding it to the index
- added better error handling for regenerate-bundle requests
- added support for deprecation list in add requests

## 3.10.1
- fix distribution_scope bug

## 3.10.0
- added propagation of validated distribution_scope
- added ability to turn on caching for skopeo inspect of images with same digest
- updated API documentation

## 3.9.2
### Added
- Ignoring duplicate bundles in payload for add operator request
