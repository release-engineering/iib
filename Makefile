# Set the default composer while allowing user to overwrite via the
# environment variable IIB_COMPOSE_ENGINE.
IIB_COMPOSE_ENGINE ?= docker-compose

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
	@echo '  Destroy the local development instance of IIB. By default, this uses docker-compose.'
	@echo '  Alternatively, set the IIB_COMPOSE_ENGINE environment variable to "podman-compose".'
	@echo ''
	@echo 'up:'
	@echo '  Run a local development instance of IIB. By default, this uses docker-compose.'
	@echo '  Alternatively, set the IIB_COMPOSE_ENGINE environment variable to "podman-compose".'
	@echo ''
	@echo 'test:'
	@echo '  Execute unit tests and linters. Use the command "tox" directly for more options.'

up: ca-bundle.crt
	@echo "Starting the local development instance..."
	${IIB_COMPOSE_ENGINE} up -d

down:
	@echo "Destroying the local development instance..."
	${IIB_COMPOSE_ENGINE} down $(COMPOSER_DOWN_OPTS)
	@rm -rf iib_data

test:
	@tox

ca-bundle.crt:
	@cp -f /etc/pki/tls/certs/ca-bundle.crt .
