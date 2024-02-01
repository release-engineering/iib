# SPDX-License-Identifier: GPL-3.0-or-later
import random

import flask_migrate
import pytest

from iib.web.models import (
    Request,
    RequestAdd,
    RequestMergeIndexImage,
    RequestRegenerateBundle,
    RequestRm,
    RequestCreateEmptyIndex,
)
from iib.common.pydantic_models import (
    AddPydanticModel,
    RmPydanticModel,
    MergeIndexImagePydanticModel,
    RegenerateBundlePydanticModel,
    CreateEmptyIndexPydanticModel,
)

INITIAL_DB_REVISION = '274ba38408e8'


def test_migrate_to_polymorphic_requests(app, auth_env, client, db):
    total_requests = 20
    # flask_login.current_user is used in RequestAdd.from_json and RequestRm.from_json,
    # which requires a request context
    with app.test_request_context(environ_base=auth_env):
        # Generate some data to verify migration
        for i in range(total_requests):
            request_class = random.choice((RequestAdd, RequestRm))
            if request_class == RequestAdd:
                data = AddPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    bundles=[f'quay.io/namespace/bundle:{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestAdd.from_json_replacement(payload=data)
            elif request_class == RequestRm:
                data = RmPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    operators=[f'operator-{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestRm.from_json_replacement(data)

            if i % 5 == 0:
                # Simulate failed request
                request.add_state('failed', 'Failed due to an unknown error')
            db.session.add(request)
        db.session.commit()

    expected_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    flask_migrate.downgrade(revision=INITIAL_DB_REVISION)
    flask_migrate.upgrade()

    actual_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    assert expected_rv_json == actual_rv_json


def test_migrate_to_merge_index_endpoints(app, auth_env, client, db):
    merge_index_revision = '4c9db41195ec'
    total_requests = 20
    # flask_login.current_user is used in RequestAdd.from_json and RequestRm.from_json,
    # which requires a request context
    with app.test_request_context(environ_base=auth_env):
        # Generate some data to verify migration
        for i in range(total_requests):
            request_class = random.choice((RequestAdd, RequestMergeIndexImage, RequestRm))
            if request_class == RequestAdd:
                data = AddPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    bundles=[f'quay.io/namespace/bundle:{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestAdd.from_json_replacement(data)
            elif request_class == RequestRm:
                data = RmPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    operators=[f'operator-{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestRm.from_json_replacement(data)
            elif request_class == RequestMergeIndexImage:
                data = MergeIndexImagePydanticModel(
                    source_from_index=f'quay.io/namespace/repo:{i}',
                    target_index=f'quay.io/namespace/repo:{i}',
                    binary_image='quay.io/namespace/binary_image:latest',
                )
                request = RequestMergeIndexImage.from_json_replacement(data)

            if i % 5 == 0:
                # Simulate failed request
                request.add_state('failed', 'Failed due to an unknown error')
            db.session.add(request)
        db.session.commit()

    expected_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    flask_migrate.downgrade(revision=merge_index_revision)
    flask_migrate.upgrade()

    actual_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    assert expected_rv_json == actual_rv_json


def test_abort_when_downgrading_from_regenerate_bundle_request(app, auth_env, client, db):
    """Verify downgrade is prevented if "regenerate-bundle" requests exist."""
    total_requests = 20
    # flask_login.current_user is used in Request*.from_json which requires a request context
    with app.test_request_context(environ_base=auth_env):
        # Always add a RequestRegenerateBundle to ensure sufficient test data is available
        data = RegenerateBundlePydanticModel(
            from_bundle_image='quay.io/namespace/bundle-image:latest'
        )
        request = RequestRegenerateBundle.from_json_replacement(data)
        db.session.add(request)

        # One request was already added, let's add the remaining ones
        for i in range(total_requests - 1):
            request_class = random.choice((RequestAdd, RequestRm, RequestRegenerateBundle))
            if request_class == RequestAdd:
                data = AddPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    bundles=[f'quay.io/namespace/bundle:{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestAdd.from_json_replacement(data)

            elif request_class == RequestRm:
                data = RmPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    operators=[f'operator-{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestRm.from_json_replacement(data)
            else:
                data = RegenerateBundlePydanticModel(
                    from_bundle_image='quay.io/namespace/bundle-image:latest'
                )
                request = RequestRegenerateBundle.from_json_replacement(data)
            db.session.add(request)

        db.session.commit()

    # flask_migrate raises a SystemExit exception regardless of what's raised from the
    # downgrade function. This exception doesn't hold a reference to the RuntimeError
    # we expect from the downgrade function in the migration script. The best we can
    # do is catch the SystemExit exception.
    with pytest.raises(SystemExit):
        flask_migrate.downgrade(revision=INITIAL_DB_REVISION)


def test_create_empty_index_image_request(app, auth_env, client, db):
    total_requests = 20
    empty_index_revision = 'e16a8cd2e028'
    # flask_login.current_user is used in RequestAdd.from_json and RequestRm.from_json,
    # which requires a request context
    with app.test_request_context(environ_base=auth_env):
        # Generate some data to verify migration
        data = CreateEmptyIndexPydanticModel(
            from_index='quay.io/namespace/index_image:latest',
            binary_image='quay.io/namespace/binary_image:latest',
        )
        request = RequestCreateEmptyIndex.from_json_replacement(data)
        db.session.add(request)

        for i in range(total_requests):
            request_class = random.choice((RequestAdd, RequestRm, RequestCreateEmptyIndex))
            if request_class == RequestAdd:
                data = AddPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    bundles=[f'quay.io/namespace/bundle:{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestAdd.from_json_replacement(data)
            elif request_class == RequestRm:
                data = RmPydanticModel(
                    binary_image='quay.io/namespace/binary_image:latest',
                    operators=[f'operator-{i}'],
                    from_index=f'quay.io/namespace/repo:{i}',
                )
                request = RequestRm.from_json_replacement(data)
            elif request_class == RequestCreateEmptyIndex:
                data = CreateEmptyIndexPydanticModel(
                    from_index=f'quay.io/namespace/index_image:{i}',
                    binary_image='quay.io/namespace/binary_image:latest',
                )
                request = RequestCreateEmptyIndex.from_json_replacement(data)

            if i % 5 == 0:
                # Simulate failed request
                request.add_state('failed', 'Failed due to an unknown error')
            db.session.add(request)
        db.session.commit()

    expected_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    flask_migrate.downgrade(revision=empty_index_revision)
    flask_migrate.upgrade()

    actual_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    assert expected_rv_json == actual_rv_json
