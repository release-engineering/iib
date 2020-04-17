# SPDX-License-Identifier: GPL-3.0-or-later
import random

import flask_migrate

from iib.web.models import RequestAdd, RequestRm


def test_migrate_to_polymorphic_requests(app, auth_env, client, db):
    total_requests = 20
    # flask_login.current_user is used in RequestAdd.from_json and RequestRm.from_json,
    # which requires a request context
    with app.test_request_context(environ_base=auth_env):
        # Generate some data to verify migration
        for i in range(total_requests):
            if random.choice((True, False)):
                data = {
                    'binary_image': 'quay.io/namespace/binary_image:latest',
                    'bundles': [f'quay.io/namespace/bundle:{i}'],
                    'from_index': f'quay.io/namespace/repo:{i}',
                }
                request = RequestAdd.from_json(data)
            else:
                data = {
                    'binary_image': 'quay.io/namespace/binary_image:latest',
                    'operators': [f'operator-{i}'],
                    'from_index': f'quay.io/namespace/repo:{i}',
                }
                request = RequestRm.from_json(data)
            if i % 5 == 0:
                # Simulate failed request
                request.add_state('failed', 'Failed due to an unknown error')
            db.session.add(request)
        db.session.commit()

    expected_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json

    flask_migrate.downgrade(revision='274ba38408e8')
    flask_migrate.upgrade()

    actual_rv_json = client.get(f'/api/v1/builds?per_page={total_requests}&verbose=true').json
    assert expected_rv_json == actual_rv_json
