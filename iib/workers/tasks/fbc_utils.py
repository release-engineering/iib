# SPDX-License-Identifier: GPL-3.0-or-later
# This file contains functions that are common for File-Based Catalog image type

from iib.workers.tasks.utils import skopeo_inspect


def is_image_fbc(image):
    """
    Detect File-Based catalog image.

    We can have two types of image - SQLite and FBC
    Those can be distinguished by LABELS.
    Image with File-Based catalog will have defined this LABEL:
    "operators.operatorframework.io.index.configs.v1"

    :param str image: the pull specification of the container image (usually from_image)
    :return: True if image is FBC type, False otherwise (SQLite)
    :rtype: bool
    """
    skopeo_output = skopeo_inspect(f'docker://{image}')
    dc_image_label = 'operators.operatorframework.io.index.configs.v1'
    return dc_image_label in skopeo_output['Labels']
