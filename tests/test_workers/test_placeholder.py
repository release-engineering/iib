# SPDX-License-Identifier: GPL-3.0-or-later

from iib.workers.tasks import placeholder


def test_ping():
    placeholder.ping()
