# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 8.7.8
* Fix OTEL issues when dealing with unsupported objects by @JAVGan in https://github.com/release-engineering/iib/pull/698
* Fix opm registry serve command by @chandwanitulsi in https://github.com/release-engineering/iib/pull/700
* Fix bug in port locking by @xDaile in https://github.com/release-engineering/iib/pull/702

## 8.7.7
* Update dependency requests to v2.32.2 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/690
* Update dependency urllib3 to v1.26.19 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/692
* Fix OPM port locking mechanism bug by @xDaile in https://github.com/release-engineering/iib/pull/695

## 8.7.6
* Fix bug introduced by e1393a2 commit by @xDaile in https://github.com/release-engineering/iib/pull/689

## 8.7.5
* Fix buildah retry mechanism by @xDaile in https://github.com/release-engineering/iib/pull/681
* Allow non tagged index image in IIB_GRAPH_MODE_INDEX_ALLOW_LIST by @xDaile in https://github.com/release-engineering/iib/pull/661
* Add atomic port locking mechanism by @xDaile in https://github.com/release-engineering/iib/pull/669
* Update dependency click-didyoumean to v0.3.1 by @renovate in https://github.com/release-engineering/iib/pull/659
* Update dependency flask-migrate to v4.0.7 by @renovate in https://github.com/release-engineering/iib/pull/664
* Update dependency botocore to v1.34.123 by @renovate in https://github.com/release-engineering/iib/pull/679
* Add OPM v1.40.0 to dev env by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/673
* Update dependency sqlalchemy to v2.0.30 by @renovate in https://github.com/release-engineering/iib/pull/665

## 8.7.4
* verify_operators_exists returns only unique packages by @lipoja in https://github.com/release-engineering/iib/pull/685

## 8.7.3
* Downgrade of qpid-proton by @lipoja in https://github.com/release-engineering/iib/pull/683

## 8.7.2
* Fix bug on `fbc_utils.enforce_json_config_dir` by @JAVGan in https://github.com/release-engineering/iib/pull/675
* Update dependency cryptography to v42.0.7 by @renovate in https://github.com/release-engineering/iib/pull/674
* Update dependency flask to v3.0.3 by @renovate in https://github.com/release-engineering/iib/pull/663
* Use BlockingConnection class from proton.utils by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/676

## 8.7.1
* Update dependency botocore to v1.34.81 by @renovate in https://github.com/release-engineering/iib/pull/662
* Update dependency idna to v3.7 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/666
* Update dependency botocore to v1.34.85 by @renovate in https://github.com/release-engineering/iib/pull/667
* Update dependency boto3 to v1.34.85 by @renovate in https://github.com/release-engineering/iib/pull/657
* Adding support of multiple operators in one fragment by @lipoja in https://github.com/release-engineering/iib/pull/660
* Fix bug with FBC YAML Operators by @JAVGan in https://github.com/release-engineering/iib/pull/655
* Update dependency werkzeug to v3.0.3 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/671
* Update dependency jinja2 to v3.1.4 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/670

## 8.7.0
* Adding support for new ocp_version_ranges by @lipoja in https://github.com/release-engineering/iib/pull/653
* Fix comma bundle version validation by @xDaile in https://github.com/release-engineering/iib/pull/654
* Update python Docker tag to v3.12.2 by @renovate in https://github.com/release-engineering/iib/pull/620
* Use multiple OPM versions in IIB by @chandwanitulsi in https://github.com/release-engineering/iib/pull/637
* Update actions/setup-python action to v5 by @renovate in https://github.com/release-engineering/iib/pull/648
* Update dependency importlib-resources to v6.4.0 by @renovate in https://github.com/release-engineering/iib/pull/645
* Update dependency botocore to v1.34.79 by @renovate in https://github.com/release-engineering/iib/pull/658


## 8.6.0
* Fixing logging in tracing.py by @lipoja in https://github.com/release-engineering/iib/pull/628
* Update dependency cryptography to v42.0.4 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/630
* Adding attributes to span by @lipoja in https://github.com/release-engineering/iib/pull/631
* Update actions/checkout action to v4 by @renovate in https://github.com/release-engineering/iib/pull/626
* Update dependency botocore to v1.34.55 by @renovate in https://github.com/release-engineering/iib/pull/638
* Update dependency boto3 to v1.34.55 by @renovate in https://github.com/release-engineering/iib/pull/636
* Update dependency coverage to v7.4.3 by @renovate in https://github.com/release-engineering/iib/pull/639
* Update postgres Docker tag to v14.11 by @renovate in https://github.com/release-engineering/iib/pull/619
* Update redhat-actions/push-to-registry action to v2.7.1 by @renovate in https://github.com/release-engineering/iib/pull/623
* Update dependency botocore to v1.34.57 by @renovate in https://github.com/release-engineering/iib/pull/641
* Update dependency boto3 to v1.34.57 by @renovate in https://github.com/release-engineering/iib/pull/640
* Update dependency cryptography to v42.0.5 by @renovate in https://github.com/release-engineering/iib/pull/642
* Update dependency flask to v3.0.2 by @renovate in https://github.com/release-engineering/iib/pull/644
* Bump version of requirements by @lipoja in https://github.com/release-engineering/iib/pull/647
* Backup index.db to prevent opm bug consequence by @xDaile in https://github.com/release-engineering/iib/pull/635

## 8.5.3
* Update dependency botocore to v1.34.29 by @renovate in https://github.com/release-engineering/iib/pull/615
* Update dependency boto3 to v1.34.29 by @renovate in https://github.com/release-engineering/iib/pull/614
* Fixing Python3.8 compatibility by @lipoja in https://github.com/release-engineering/iib/pull/624

## 8.5.2
* Adding result_backend config by @lipoja in https://github.com/release-engineering/iib/pull/610
* Update dependency jinja2 to v3.1.3 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/609
* Update dependency backoff to v1.11.1 by @renovate in https://github.com/release-engineering/iib/pull/608
* Update dependency alembic to v1.13.1 by @renovate in https://github.com/release-engineering/iib/pull/606
* Update dependency amqp to v5.2.0 by @renovate in https://github.com/release-engineering/iib/pull/607
* Update dependency MarkupSafe to v2.1.4 by @renovate in https://github.com/release-engineering/iib/pull/612
* Update dependency wcwidth to v0.2.13 by @renovate in https://github.com/release-engineering/iib/pull/613
* Batch update of requirements, Changed version of Python for GitHub Actions. by @lipoja in https://github.com/release-engineering/iib/pull/617

## 8.5.1
* Check if source_from_index is in iib_no_ocp_label_allow_list by @lipoja in https://github.com/release-engineering/iib/pull/599
* Use container-tool podman for _opm_index_add commands by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/600
* Fixing logging for allow_no_ocp_version by @lipoja in https://github.com/release-engineering/iib/pull/601
* Update dependency tomli to v1.2.3 by @renovate in https://github.com/release-engineering/iib/pull/585
* Update dependency flask-login to v0.6.3 by @renovate in https://github.com/release-engineering/iib/pull/584
* Update dependency wcwidth to v0.2.12 by @renovate in https://github.com/release-engineering/iib/pull/586
* Update dependency cryptography to v41.0.7 by @renovate in https://github.com/release-engineering/iib/pull/602
* Update dependency prompt-toolkit to v3.0.43 by @renovate in https://github.com/release-engineering/iib/pull/603
* Update dependency Mako to v1.3.0 by @renovate in https://github.com/release-engineering/iib/pull/604

## 8.5.0
* Add guardrails for UniqueViolation caused by parallel requests by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/592
* Adding parameter ignore_bundle_ocp_version to merge-index API by @lipoja in https://github.com/release-engineering/iib/pull/593
* Update dependency cryptography to v41.0.6 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/595

## 8.4.4
* Fixing github actions by @lipoja in https://github.com/release-engineering/iib/pull/590
* Adding config variable to allow adding bundles without ocp version label by @lipoja in https://github.com/release-engineering/iib/pull/589
* Allow skopeo inspect to check for images for any arch @chandwanitulsi in https://github.com/release-engineering/iib/pull/588

## 8.4.3
* Update dependency gssapi to v1.8.3 by @renovate in https://github.com/release-engineering/iib/pull/583
* Update dependency cryptography to v41.0.5 by @renovate in https://github.com/release-engineering/iib/pull/582
* Update dependency urllib3 to v1.26.18 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/580
* urllib3 is solo package by @lipoja in https://github.com/release-engineering/iib/pull/579
* Adding graph update mode for merge index image API endpoint by @lipoja in https://github.com/release-engineering/iib/pull/578
* Upgrade OPM to v1.26.4 by @chandwanitulsi in https://github.com/release-engineering/iib/pull/577

## 8.4.2
* Update dependency flask-sqlalchemy to v3.1.1 by @renovate in https://github.com/release-engineering/iib/pull/541
* Update dependency markupsafe to v2.1.3 by @renovate in https://github.com/release-engineering/iib/pull/544
* Update dependency prompt-toolkit to v3.0.39 by @renovate in https://github.com/release-engineering/iib/pull/545
* Fixing IIB KeyError when updating request state by @lipoja in https://github.com/release-engineering/iib/pull/569
* Adding OpenTelemetry collector/exporter for dev-env by @lipoja in https://github.com/release-engineering/iib/pull/565
* Pinning werkzeug to allow generation of docs. by @lipoja in https://github.com/release-engineering/iib/pull/572
* Flush request to database  by @lipoja in https://github.com/release-engineering/iib/pull/566
* Use py3.9 for Renovate bot runs by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/574
* Update dependency cryptography to v41.0.4 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/570
* Update dependency urllib3 to v1.26.17 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/571
* Upgrade opentelemetry-instrumentation to 0.41b0 by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/575

## 8.4.1
- Fix normalization bug when return value is non-dict by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/563

## 8.4.0
- Enable celery task deduplication by @lipoja in https://github.com/release-engineering/iib/pull/558
- Remove Dependabot and Reconfigure Renovate bot by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/552
- Fix opentelemetry span type bug by @xDaile in https://github.com/release-engineering/iib/pull/557
- Update dependency sqlalchemy to v2.0.19 by @renovate in https://github.com/release-engineering/iib/pull/549
- Upgrade PostgreSQL development version by @xDaile in https://github.com/release-engineering/iib/pull/559
- Log a warning message when "architecture" label is missing under "Labels" by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/561
- Add related_images check in IIB by @chandwanitulsi in https://github.com/release-engineering/iib/pull/556
- Move get_bundle_metadata to utils by @chandwanitulsi in https://github.com/release-engineering/iib/pull/560
- Update dependency deprecated to v1.2.14 by @renovate in https://github.com/release-engineering/iib/pull/540
- Update dependency grpcio to v1.57.0 by @renovate in https://github.com/release-engineering/iib/pull/542
- Update dependency mako to v1.2.4 by @renovate in https://github.com/release-engineering/iib/pull/543

## 8.3.0
- Retry buildah if incorrect arch created by @chandwanitulsi in https://github.com/release-engineering/iib/pull/529
- Update Grpcio and Protobuf versions by @xDaile in https://github.com/release-engineering/iib/pull/533
- Upgrade OPM to 1.28.0 by @chandwanitulsi in https://github.com/release-engineering/iib/pull/530
- Retry update_request with back-off factor  on failure by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/531
- Revert "Upgrade OPM to 1.28.0" by @chandwanitulsi in https://github.com/release-engineering/iib/pull/534
- Update Sphinx documentation by @xDaile in https://github.com/release-engineering/iib/pull/532
- Configure Renovate by @renovate in https://github.com/release-engineering/iib/pull/535
- Update dependency cryptography to v41 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/538
- Update dependency requests to v2.31.0 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/536
- Update dependency certifi to v2023 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/537
- Fix bugs in models post flask-sqlalchemy 3.0 and SQLAlchemy 2.0 migration by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/550
- Add support for "graph_update_mode" parameter in Add request by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/551
- Update dependency cryptography to v41.0.3 [SECURITY] by @renovate in https://github.com/release-engineering/iib/pull/553

## 8.2.0
- Adding from_index_startswith builds search filter by @lipoja in https://github.com/release-engineering/iib/pull/526
- Convert IIB_OTEL_TRACING to an env variable by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/527

## 8.1.1
- Add init file to iib/common module by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/524

## 8.1.0
- Adding missing 's' in fbc-operations endpoint documentation by @lipoja in https://github.com/release-engineering/iib/pull/519
- Adding IIB Instrumentation by @raks-tt in https://github.com/release-engineering/iib/pull/508
- Add Config variable which controls OpenTelemetry tracing by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/522
- Fixing deprecation of operator in FBC image by @lipoja in https://github.com/release-engineering/iib/pull/520

## 8.0.0
- Add logs for time tracking by @xDaile in https://github.com/release-engineering/iib/pull/507
- Bump sphinx from 5.3.0 to 6.1.3 by @dependabot in https://github.com/release-engineering/iib/pull/483
- Add token overwrite in build.py by @xDaile in https://github.com/release-engineering/iib/pull/510
- Bump sphinx from 6.1.3 to 6.2.0 by @dependabot in https://github.com/release-engineering/iib/pull/511
- Adding image filtering for fbc_operations requests by @lipoja in https://github.com/release-engineering/iib/pull/514
- Changing loading of user queue to accept new structure by @lipoja in https://github.com/release-engineering/iib/pull/509
- Adding filtering builds based on `from_index` by @lipoja in https://github.com/release-engineering/iib/pull/513
- Upgrade flask to 2.2.5 by @yashvardhannanavati in https://github.com/release-engineering/iib/pull/516

## 7.1.1
- Fix RM API endpoint - remove operator from FBC 
- Move cleanup function to the end of the request
- Fixing Bandid test

## 7.1.0
- Update dependabot.yml
- Bump actions/checkout from 2 to 3
- Add request id to prefix of iib temporary file
- Upgrade deps : sqlalchemy 2.0.0 major migration and others

## 7.0.1
- Fix fbc-operations bug where the requests failed when binary_image is not submitted

## 7.0.0
- Fix flake8 and mypy bugs
- Tweak Add and Rm worker logic to support the new fbc-operations endpoint
- Add new fbc-operations API endpoint

## 6.8.4
- Generate catalog cache right after opm migrate

## 6.8.3
- Fixed setting GID 0 for cache directory

## 6.8.2
- Fix user permissions on the generated cache

## 6.8.1
- Fix context issues for copying cache into the Dockerfile for FBC builds

## 6.8.0
- Add static types to IIB
- Upgrade dependencies to apply security fixes
- Increase default grpc wait time, retry delay and retry jitter
- Enable weekly rebuild of IIB images to patch old images regularly
- Use UBI8 for messaging
- Replace Travis CI with Github actions
- Build index image cache locally to enable multi-arch builds on amd64 machines
- Improved logging in the API response
- Unsupported retry package replaced by tenacity

## 6.7.2
- Fix bundle_replacements bug when the user doesn't provide it in regenerate-bundle requests

## 6.7.1
- Fix dev env compose files
- Fix bundle_replacements bug in regenerate-bundle request
- Upgrade OPM to 1.26.2 in dev env
- Fix buildah bud command retries
- Adding retry and port check when opm serve/opm registry serve is called

## 6.7.0
- Add bundle_replacements parameter to regenerate_bundle API and worker
- Bump mako version
- Increase iib_api_timeout to 120 seconds
- Fix RM request private registry bug

## 6.6.1
- Add recursive-related-bundles endpoint 
- Adding ability to create single-active-consumer queues
- Add static types and mypy checks
- Append overwrite index token to current docker config.

## 6.5.0
- Print version of binary files in log files
- Improve traceability in _get_present_bundles and fix tests
- Increase iib_api_timeout, iib_retry_delay and iib_retry_jitter
- Keep Gating feature in IIB and improve gating logging
- Adding fallback from SIGTERM to SIGKILL
- Fix dependency issues for python 3.9

## 6.4.0
- Add internal_index_image_copy and internal_index_image_copy_resolved to Add and Rm response
- Add a warning for when gating is disabled

## 6.3.0
- Fix create-empty-index endpoint to not accept build_tags
- Update black to stable version 22.3.0
- Build container image for message broker and push to quay.io
- Upgrade OPM to 1.21.0
- Remove FIXME comments for issues caught by bandit
- Bump pytest from 7.1.0 to 7.1.2 

## 6.2.0
- Fix bug of missing related_bundles param and logs param in API response
- Modify permissions on logs files 

## 6.1.0 
- Add support for Python3.9
- Enable send events to the broker
- Bump pytest from 6.2.5 to 7.0.0
- Use terminate function to shut down a process instead of kill
- Pretty print (log) of RequestConfig classes
- Logging improvements

## 6.0.2
- Added support for File-Based Catalog
- Dropped support for Python3.6
- Fixed bug for failing opm index deprecatetruncate
- Fixed loosing olm.maxOpenShiftVersion property
- Fixed bug for Buildah retries on 5XX
- Added Static Application Security Testing
- Updated Celery to 5.2.2 for Python 3.8
- Fixed mod_wsgi package in iib-api

## 5.0.0
 - Fix issue of deprecation list with duplicated bundles
 - Add mod_wsgi dependency to Dockerfile-api
 - Update opm in dev env to v1.19.5
 - Add framework to support FBC indexes
 - Upgrade Flask 2.0.2, Werkzeug 2.0.2, Celery 5.1.2, Kombu 5.1.0
 - Add support for Python 3.8
 - Add retries and minor fixes for buildah commands
 - Add REGISTRY_AUTH_FILE support in dev env
 - Add support for AWS S3 buckets for artifacts storage
 - Add github action to build API image on tag push

## 4.9.0
 - Fix the comparison of index image and bundle
 - Avoid failing to create-empty-index when labels are not set
 - Generate registry certificates automatically for dev env
 - Remove x509ignoreCN workaround
 - Update OPM, grpcurl and operator-sdk in dev-env

## 4.8.0
- pinning is now a customization. If customization is used for an organization, it will not be
  done automatically unless explicitly specified
- renamed Declarative Config to File Based Config
- added support for filtering on user, request_type and index_image on builds endpoint

## 4.7.0
- having relatedImages and RELATED_IMAGES_* in the bundle image is now valid for regeneration
- fixed inconsistencies in arch selection code for different endpoints

## 4.6.2
- Attempt adding maxOpenshiftVersion property only when adding bundles to index

## 4.6.1
- fixed bug to use registry token while inspecting image

## 4.6.0
- added build and push iib-worker and iib-api images to quay.io
- fixed bug to clean local manifest lists before creating a new one
- used ubi8 as base images for api and worker
- fixed merge-index-image bug that created the same manifest list twice
- added setting OcpMaxVersion property in merge-index-image
- added new attribute 'build_tags' for index_image operations
- bumped pytest-cov from 2.12.1 to 3.0.0
- bumped coverage from 5.5 to 6.0.1

## 4.5.0
- replaced manifest-tool with buildah to build manifest lists
- added declarative config migrator to worker dockerfile

## 4.4.1
- fixed merge-index-image bugs
- fixed deprecatetruncate command

## 4.4.0
- deprecated legacy support for OMPS

## 4.3.0
- fixed memcached key length error
- fixed registry_auth in batch regenerate-bundle requests
- added related_bundles api endpoint for regenerate-bundle requests

## 4.2.0
- Add create-empty-index functionality
- Replace custom retry with retry package and setting backoff interval for skopeo_inspect
- Set --arch when building images
- Bump pytest-cov from 2.12.0 to 2.12.1
- Remove retry delay during tests
- Request and urllib3 update
- Use pinned runtime requirements in tests
- Fix apply_repo_enclosure bug when namespace is None

## 4.1.0
- removed support for privileged users
- added retries for OMPS pushes
- fixed bug in merge-index-image endpoint to deprecate invalid bundles instead of filtering them
- added functionality to add olm.maxOpenShiftVersion property to bundles being added

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
