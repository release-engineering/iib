# iib
A REST API to manage operator index container images

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
