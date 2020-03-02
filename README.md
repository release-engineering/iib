# Index Image Build (IIB) Service
A REST API to manage operator index container images

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
```
Get the IIB build request from the REST API.

:param int request_id: the ID of the IIB request
:return: the request
:rtype: dict
:raises IIBError: if the HTTP request fails
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
`pip-compile --generate-hashes setup.py requirements-test.in -o requirements-test.txt`.

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
* `IIB_LOG_FORMAT` - the format of the logs. This defaults to
  `%(asctime)s %(name)s %(levelname)s %(module)s.%(funcName)s %(message)s`.
* `IIB_LOG_LEVEL` - the Python log level of the REST API (Flask). This defaults to `INFO`.
* `IIB_MAX_PER_PAGE` - the maximum number of build requests that can be shown on a single page.
  This defaults to `20`.
* `IIB_WORKER_USERNAMES` - the list of case-sensitve Kerberos principals that are allowed to update
  build requests using the PATCH API endpoint. This defaults to `[]`.
* `LOGIN_DISABLED` - determines if authentication is required. This defaults to `False`
  (i.e. authentication is required).
* `SQLALCHEMY_DATABASE_URI` - the database URI of the database the REST API connects to. See the
  [Flask-SQLAlchemy configuration](https://flask-sqlalchemy.palletsprojects.com/en/2.x/config/#configuration-keys)
  documentation.

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
* `iib_image_push_template` - the Python string template of the push destination for the resulting
  manifest list. The available variables are `registry` and `request_id`. The default value is
  `{registry}/iib-build:{request_id}`.
* `iib_log_level` - the Python log level for `iib.workers` logger. This defaults to `INFO`.
* `iib_registry` - the container registry to push images to (e.g. `quay.io`).
