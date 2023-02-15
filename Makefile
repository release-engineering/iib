# Set the default composer while allowing user to overwrite via the
# environment variable IIB_COMPOSE_ENGINE.
IIB_COMPOSE_ENGINE ?= docker-compose
IIB_COMPOSE_RUNNER = ${IIB_COMPOSE_ENGINE} -f ${PWD}/compose-files/${IIB_COMPOSE_ENGINE}.yml
OTEL_RESOURCE_ATTRIBUTES=service.name=iib
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector-http-traces.apps.ocp-c2.prod.psi.redhat.com

# Declare non-file targets to avoid potential conflict with files
# of the same name.
.PHONY: all up test

# Older versions of podman-compose do not support deleting volumes via -v
COMPOSER_DOWN_OPTS := -v
COMPOSER_DOWN_HELP := $(shell ${IIB_COMPOSE_ENGINE} down --help)
ifeq (,$(findstring volume,$(COMPOSER_DOWN_HELP)))
	COMPOSER_DOWN_OPTS :=
endif

all:
	@echo 'Available make targets:'
	@echo ''
	@echo 'down:'
	@echo '  Destroy the local development instance of IIB.'
	@echo ''
	@echo 'up:'
	@echo '  Run a local development instance of IIB.'
	@echo ''
	@echo 'build:'
	@echo '  Build the container images used in the local development instance of IIB.'
	@echo '  This is useful for forcing the images to be rebuilt.'
	@echo ''
	@echo 'test:'
	@echo '  Execute unit tests and linters. Use the command "tox" directly for more options.'
	@echo ''
	@echo 'NOTE: By default, the targets use docker-compose. Alternatively, set the'
	@echo '  IIB_COMPOSE_ENGINE environment variable to another compose system, e.g.'
	@echo '  "podman-compose".'

up: ca-bundle.crt iib-data
	@echo "Starting the local development instance..."
	${IIB_COMPOSE_RUNNER} up -d

down:
	@echo "Destroying the local development instance..."
	${IIB_COMPOSE_RUNNER} down $(COMPOSER_DOWN_OPTS)
	@rm -rf iib_data

build:
	@echo "Building the container images for the local development instance..."
	${IIB_COMPOSE_RUNNER} build

test:
	@tox

ca-bundle.crt:
	@cp -f /etc/pki/tls/certs/ca-bundle.crt .

iib-data:
	@mkdir -p iib_data/registry
