# SPDX-License-Identifier: GPL-3.0-or-later
from typing import Dict
from iib.workers.config import get_worker_config


def get_binary_versions() -> Dict:
    """
    Return string containing version of binary files used by IIB.

    :return: Dictionary with all binary used and their version
    :rtype: dict
    """
    from iib.workers.tasks.utils import run_cmd

    podman_version_cmd = ['podman', '-v']
    buildah_version_cmd = ['buildah', '-v']

    worker_config = get_worker_config()
    iib_ocp_opm_mapping = worker_config.get("iib_ocp_opm_mapping")
    opm_versions_available = set()
    opm_versions_available.add(worker_config.get('iib_default_opm'))
    if iib_ocp_opm_mapping is not None:
        opm_versions_available.update(set(iib_ocp_opm_mapping.values()))

    try:
        return {
            'opm': [
                run_cmd([opm_path, 'version'], exc_msg='Failed to get opm version.').strip()
                for opm_path in opm_versions_available
            ],
            'podman': run_cmd(podman_version_cmd, exc_msg='Failed to get podman version.').strip(),
            'buildah': run_cmd(
                buildah_version_cmd, exc_msg='Failed to get buildah version.'
            ).strip(),
        }
    except FileNotFoundError:
        return {'opm': '', 'podman': '', 'buildah': ''}
