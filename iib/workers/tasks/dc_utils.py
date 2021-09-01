# SPDX-License-Identifier: GPL-3.0-or-later

from iib.workers.tasks.utils import skopeo_inspect


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
