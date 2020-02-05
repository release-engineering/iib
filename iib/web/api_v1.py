# SPDX-License-Identifier: GPL-3.0-or-later
import flask

from iib.workers.tasks.placeholder import ping

api_v1 = flask.Blueprint('api_v1', __name__)


@api_v1.route('/test', methods=['GET'])
def get_request():
    ping.delay()
    return 'Test request success!'
