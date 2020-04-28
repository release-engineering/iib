# Index Image Build (IIB) Service

A REST API to manage operator index container images

## External Documentation

* [API Documentation](http://release-engineering.github.io/iib)
* [General Documentation](https://iib.readthedocs.io/en/latest/)
* [Python Module Documentation](https://iib.readthedocs.io/en/latest/module_documentation/index.html)

## Coding Standards

The codebase conforms to the style enforced by `flake8` with the following exceptions:

* The maximum line length allowed is 100 characters instead of 80 characters

In addition to `flake8`, docstrings are also enforced by the plugin `flake8-docstrings` with
the following exemptions:

* D100: Missing docstring in public module
* D104: Missing docstring in public package
* D105: Missing docstring in magic method

The format of the docstrings should be in the
[reStructuredText](https://docs.python-guide.org/writing/documentation/#restructuredtext-ref) style
such as:

```python
"""
Get the IIB build request from the REST API.

:param int request_id: the ID of the IIB request
:return: the request
:rtype: dict
:raises IIBError: if the HTTP request fails
"""
```

Additionally, `black` is used to enforce other coding standards with the following exceptions:

* Single quotes are used instead of double quotes

To verify that your code meets these standards, you may run `tox -e black,flake8`.

## Running the Unit Tests

The testing environment is managed by [tox](https://tox.readthedocs.io/en/latest/). Simply run
`tox` and all the linting and unit tests will run.

If you'd like to run a specific unit test, you can do the following:

```bash
tox -e py37 tests/test_web/test_api_v1.py::test_add_bundle_invalid_param
```

## Development Environment

[docker-compose](https://docs.docker.com/compose/) is the supported mechanism for setting up a
development environment. This will automatically run the following containers:

* **iib-api** - the IIB REST API. This is accessible at [http://localhost:8080](http://localhost:8080).
* **iib-worker** - the IIB Celery worker.
* **rabbitmq** - the RabbitMQ instance for communicating between the API and the worker. The
  management UI is accessible at [http://localhost:8081](http://localhost:8081). The username is
  `iib` and the password is `iib`.
* **db** - the Postgresql database used by the IIB REST API.
* **registry** - the Docker Registry where the worker pushes its build index images to. This is
  accessible at docker://localhost:8443.
* **message-broker** - the Apache ActiveMQ instance for publishing messages for external consumers.
  The web console is accessible at [http://localhost:8161/admin](http://localhost:8161/admin). The
  username is `admin` and the password is `admin`. The docker-compose environment is configured for
  IIB to publish AMQP 1.0 messages to the Apache ActiveMQ instance at the destinations
  `topic://VirtualTopic.eng.iib.batch.state` and `topic://VirtualTopic.eng.iib.build.state`.

The Flask application will automatically reload if there is a change in the codebase. If invalid
syntax is added in the code, the `iib-api` container may shutdown. The Celery worker will
automatically restart if there is a change under the `iib/workers` directory.

To run a built index image from the development registry, you can perform the following:

```bash
podman login --tls-verify=false -u iib -p iibpassword localhost:8443
podman pull --tls-verify=false localhost:8443/iib-build:1
```

If you are using Docker (a modern version is required), you can perform the following:

```bash
sudo docker login -u iib -p iibpassword localhost:8443
sudo docker run localhost:8443/iib-build:1
```

If your development environment requires accessing a private container registry, please read
the section titled Registry Authentication.

## Dependency Management

To manage dependencies, this project uses [pip-tools](https://github.com/jazzband/pip-tools) so that
the production dependencies are pinned and the hashes of the dependencies are verified during
installation.

The unpinned dependencies are recorded in `setup.py`, and to generate the `requirements.txt` file,
run `pip-compile --generate-hashes --output-file=requirements.txt`. This is only necessary when
adding a new package. To upgrade a package, use the `-P` argument of the `pip-compile` command.

To update `requirements-test.txt`, run
`pip-compile --generate-hashes requirements-test.in -o requirements-test.txt`.

When installing the dependencies in a production environment, run
`pip install --require-hashes -r requirements.txt`. Alternatively, you may use
`pip-sync requirements.txt`, which will make sure your virtualenv only has the packages listed in
`requirements.txt`.

To ensure the pinned dependencies are not vulnerable, this project uses
[safety](https://github.com/pyupio/safety), which runs on every pull-request.

## Registry Authentication

IIB does not handle authentication with container registries directly. If authentication is needed,
configure the `~/.docker/config.json` for the user running the IIB worker.

During development, you may choose to add a volume entry of `- /root/.docker:/root/.docker:z` on the
workers in `docker-compose.yml` so that your host's root user's Docker configuration with
authentication is used by the workers. This is only needed if you are working with private images.
Please note that the containers will modify this configuration since they authenticate with the
registry created by docker-compose on startup.

## Configuring the REST API

To configure the IIB REST API, create a Python file at `/etc/iib/settings.py`. Any variables set in
this configuration file will be applied to the Celery worker when running in production mode
(default).

The custom configuration options for the REST API are listed below:

* `IIB_ADDITIONAL_LOGGERS` - a list of Python loggers that should have the same log level that is
  set for `IIB_LOG_LEVEL`. This defaults to `[]`.
* `IIB_FORCE_OVERWRITE_FROM_INDEX` - a boolean that determines if privileged users should be forced
  to have `overwrite_from_index` set to `True`. This defaults to `False`.
* `IIB_GREENWAVE_CONFIG` - the mapping, `dict(<str>: dict(<str>:<str>))`, of celery task queues to
  another dictionary of [Greenwave](https://docs.pagure.org/greenwave/) query parameters to their
  values. This is useful in setting up customized gating for each queue. This defaults to `{}`. Use
  the task queue name as `None` to configure Greenwave config for the default Celery task queue.
* `IIB_LOG_FORMAT` - the format of the logs. This defaults to
  `%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s`.
* `IIB_LOG_LEVEL` - the Python log level of the REST API (Flask). This defaults to `INFO`.
* `IIB_MAX_PER_PAGE` - the maximum number of build requests that can be shown on a single page.
  This defaults to `20`.
* `IIB_PRIVILEGED_USERNAMES` - the list of users that can perform privileged actions such
  as overwriting the input index image with the built index image. This defaults to `[]`.
* `IIB_USER_TO_QUEUE` - the mapping, `dict(<str>: <str>)`, of usernames to celery task queues.
  This is useful in isolating the workload from certain users. The default queue is used for tasks
  from users not found in the mapping. This defaults to `{}`.
* `IIB_WORKER_USERNAMES` - the list of case-sensitve Kerberos principals that are allowed to update
  build requests using the PATCH API endpoint. This defaults to `[]`.
* `LOGIN_DISABLED` - determines if authentication is required. This defaults to `False`
  (i.e. authentication is required).
* `SQLALCHEMY_DATABASE_URI` - the database URI of the database the REST API connects to. See the
  [Flask-SQLAlchemy configuration](https://flask-sqlalchemy.palletsprojects.com/en/2.x/config/#configuration-keys)
  documentation.

The custom configuration options for AMQP 1.0 messaging are listed below:

* `IIB_MESSAGING_BATCH_STATE_DESTINATION` - the AMQP 1.0 destination to send the batch state change
  messages. If this is not set, IIB will not send these types of messages. If this is set,
  `IIB_MESSAGING_URLS` must also be set.
* `IIB_MESSAGING_BUILD_STATE_DESTINATION` - the AMQP 1.0 destination to send the build request state
  change messages. If this is not set, IIB will not send these types of messages. If this is set,
  `IIB_MESSAGING_URLS` must also be set.
* `IIB_MESSAGING_CA` - the path to a file with the certificate authority that signed the certificate
  of the AMQP 1.0 message broker. This defaults to `/etc/pki/tls/certs/ca-bundle.crt`.
* `IIB_MESSAGING_CERT` - the path to the identity certificate used for authentication with the
  AMQP 1.0 message broker. This defaults to `/etc/iib/messaging.crt`.
* `IIB_MESSAGING_DURABLE` - determines if the messages are durable and cannot be lost due to an
  unexpected termination or restart by the AMQP 1.0 broker. If the broker is not capable of
  guaranteeing this, it may not accept the message. In that case, set this configuration option to
  `False`. This defaults to `True`.
* `IIB_MESSAGING_KEY` - the path to the private key of the identity certificate used for
  authentication with the AMQP 1.0 message broker. This defaults to `/etc/iib/messaging.key`.
* `IIB_MESSAGING_TIMEOUT` - the number of seconds before a messaging operation times out.
  Examples of messaging operations include connecting to the broker and sending a message to the
  broker. In this case, if the timeout is set to `30`, then it could take a maximum of 60 seconds
  before the operation times out. This is because it can take up to 30 seconds to connect to the
  broker and also up to 30 seconds for the message to be sent. This defaults to `30`.
* `IIB_MESSAGING_URLS` - a list of AMQP(S) URLs to use when connecting to the AMQP 1.0 broker. This
  must be set if messaging is enabled.

## Configuring the Worker(s)

To configure an IIB Celery worker, create a Python file at `/etc/iib/celery.py`. The location
can be overridden with the `IIB_CELERY_CONFIG` environment variable. This is useful if the worker is
running on the same host as another worker or the REST API.

Any variables set in this configuration file will be applied to the Celery worker when running in
production mode (default).

The custom configuration options for the Celery workers are listed below:

* `broker_url` - the AMQP(S) URL to connect to RabbitMQ. See the
  [broker_url](https://docs.celeryproject.org/en/latest/userguide/configuration.html#std:setting-broker_url)
  configuration documentation.
* `iib_api_timeout` - the timeout in seconds for HTTP requests to the REST API. This defaults to
  `30` seconds.
* `iib_api_url` - the URL to the IIB REST API (e.g. `https://iib.domain.local/api/v1/`).
* `iib_greenwave_url` - the URL to the Greenwave REST API if gating is desired
  (e.g. `https://greenwave.domain.local/api/v1.0/`). This defaults to `None`.
* `iib_index_image_output_registry` - if set, that value will replace the value from `iib_registry`
  in the output `index_image` pull specification. This is useful if you'd like users of IIB to
  pull from a proxy to a registry instead of the registry directly.
* `iib_image_push_template` - the Python string template of the push destination for the resulting
  manifest list. The available variables are `registry` and `request_id`. The default value is
  `{registry}/iib-build:{request_id}`.
* `iib_log_level` - the Python log level for `iib.workers` logger. This defaults to `INFO`.
* `iib_registry` - the container registry to push images to (e.g. `quay.io`).
* `iib_skopeo_timeout` - the command timeout for skopeo commands run by IIB. This defaults to
  `30s` (30 seconds).
* `iib_total_attempts` - the total number of attempts to make at trying a function relating to the
  container registry before erroring out. This defaults to `5`.

## Regenerating Bundle Images

In addition to building operator index images, IIB can also be used to regenerate operator bundle
images. This is useful for applying modifications to the manifests embedded in the bundle image.
IIB uses the [operator-manifest](https://github.com/containerbuildsystem/operator-manifest) library
to assist in these modifications.

Currently, IIB will not perform any modifications on a ClusterServiceVersion file if
[spec.relatedImages](https://access.redhat.com/documentation/en-us/openshift_container_platform/4.3/html-single/operators/index#olm-enabling-operator-for-restricted-network_osdk-generating-csvs)
is set.

If it's not set, IIB will pin any container image pull specification and set
[spec.relatedImages](https://access.redhat.com/documentation/en-us/openshift_container_platform/4.3/html-single/operators/index#olm-enabling-operator-for-restricted-network_osdk-generating-csvs).
See the different
[pull specifications](https://github.com/containerbuildsystem/operator-manifest#pull-specifications)
to which this process applies to.

## Messaging

IIB has support to send messages to an AMQP 1.0 broker. If configured to do so, IIB will send
messages when a build request state changes and when a batch state changes.

The build request state change message body is the JSON representation of the build request in
the non-verbose format like in the `/builds` API endpoint. The message has the following keys set in
the application properties: `batch`, `id`, `state`, and `user`.

The batch state change message body is a JSON object with the following keys: `batch`,
`request_ids`, `state`, and `user`. The message has the following keys set in the application
properties: `batch`, `state`, and `user`.

## Gating Bundle Images

In addition to building operator index images, IIB can also gate your bundle images before adding
them to the index image. If a Greenwave configuration is setup for your queue, IIB will query
Greenwave to check if your bundle image builds have passed the tests in the Greenwave policy you
have defined. The IIB request submitted to that queue will succeed only if the policy is satisfied.

## Read the Docs Documentation

### Build the Docs

To build and serve the docs, run the following commands:

```bash
tox -e docs
google-chrome .tox/docs_out/index.html
```

### Expanding the Docs

To document a new Python module, find the `rst` file of the corresponding Python package that
contains the module. Once found, add a section under "Submodules" in alphabetical order such as:

```rst
iib.workers.tasks.build module
------------------------------

.. automodule:: iib.workers.tasks.build
   :ignore-module-all:
   :members:
   :private-members:
   :show-inheritance:
```

Some of the options include:

* `ignore-module-all` - include all members regardless of the definition of `__all__`.
* `members` - automatically document the members in that Python module.
* `private-members` - include private functions and methods.
* `show-inheritance` - show the class inheritance.
