# SPDX-License-Identifier: GPL-3.0-or-later
from flask import Blueprint, send_from_directory
from typing import IO


docs = Blueprint('docs', __name__)


@docs.route('/', methods=['GET'])
def index() -> IO:
    """Return the OpenAPI documentation presented by redoc."""
    return send_from_directory('static', 'docs.html')
