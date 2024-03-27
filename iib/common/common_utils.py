# SPDX-License-Identifier: GPL-3.0-or-later
from typing import Dict


def get_binary_versions() -> Dict:
    """
    Return string containing version of binary files used by IIB.

    :return: Dictionary with all binary used and their version
    :rtype: dict
    """
    from iib.workers.tasks.utils import run_cmd

    opm_version_cmd = ['opm', 'version']
    podman_version_cmd = ['podman', '-v']
    buildah_version_cmd = ['buildah', '-v']

    try:
        return {
            'opm': run_cmd(opm_version_cmd, exc_msg='Failed to get opm version.').strip(),
            'podman': run_cmd(podman_version_cmd, exc_msg='Failed to get podman version.').strip(),
            'buildah': run_cmd(
                buildah_version_cmd, exc_msg='Failed to get buildah version.'
            ).strip(),
        }
    except FileNotFoundError:
        return {'opm': '', 'podman': '', 'buildah': ''}
