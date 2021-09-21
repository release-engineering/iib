# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os

from iib.workers.tasks.utils import skopeo_inspect, run_cmd

log = logging.getLogger(__name__)


def is_image_dc(image):
    """
    Detect declarative config image.

    We can have two types of image - SQLite and Declarative Config
    Those can be distinguished by LABELS.
    Declarative config will have defined this LABEL: operators.operatorframework.io.index.configs.v1

    :param str image: the pull specification of the container image (usually from_image)
    :return: True if image is declarative config type, False otherwise
    :rtype: bool
    """
    skopeo_output = skopeo_inspect(f'docker://{image}')
    dc_image_label = 'operators.operatorframework.io.index.configs.v1'
    return dc_image_label in skopeo_output['Labels']


def omp_generate_dockerfile(dc_root_dir):
    """
    Generate a Dockerfile for a declarative config index.
    :param str dc_root_dir: root directory for declarative config index.
    """
    cmd = ['opm', 'alpha', 'generate', 'dockerfile', dc_root_dir]

    log.info('Generating Dockerfile for DC image.')
    run_cmd(cmd, exc_msg='Failed to generate Dockerfile.')


def dcm_migrate(index_image, temp_dir):
    """
    Migrate image from SQLite base to declarative config (file base catalog).
    Create and return directory with migrate indices and Dockerfile ready to be build.

    :param str index_image: image to be migrated
    :param str temp_dir: path to temp directory
    :return: Local path to directory with indices and Dockerfile
    :rtype: str
    """

    cmd = ['dcm', 'migrate', index_image]
    dc_image = os.path.join(temp_dir, 'dc_image')
    if not os.path.exists(dc_image):
        os.mkdir(dc_image)
    params = {'cwd': temp_dir}

    log.info('Migrating image %s to file based catalog (declarative config)', index_image)
    run_cmd(cmd, params=params, exc_msg='Failed to migrate image to file based catalog.')

    index_dir = os.path.join(temp_dir, 'index')
    omp_generate_dockerfile(index_dir)
    return temp_dir
