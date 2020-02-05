# SPDX-License-Identifier: GPL-3.0-or-later
import flask

api_v1 = flask.Blueprint('api_v1', __name__)


@api_v1.route('/test', methods=['GET'])
def get_request():
    return 'Test request success!'
