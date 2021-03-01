# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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
