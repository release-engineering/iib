# SPDX-License-Identifier: GPL-3.0-or-later
# This file contains functions that are common for File-Based Catalog image type
import os
import logging

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config

# path to index.db in our temp directories used in IIB code
TEMP_INDEX_DB_PATH = 'database/index.db'

log = logging.getLogger(__name__)


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
    from iib.workers.tasks.utils import skopeo_inspect

    skopeo_output = skopeo_inspect(f'docker://{image}')
    fbc_image_label = 'operators.operatorframework.io.index.configs.v1'
    return fbc_image_label in skopeo_output['Labels']


def get_index_fbc_dir(from_index, base_dir):
    """
    Get file-based catalog directory from the specified index image and save it locally.

    :param str from_index: index image to get file-based catalog directory from.
    :param str base_dir: base directory to which the database file should be saved.
    :return: path to the copied file-based catalog directory.
    :rtype: str
    :raises IIBError: if any podman command fails.
    """
    from iib.workers.tasks.build import _copy_files_from_image
    from iib.workers.tasks.utils import get_image_label

    fbc_dir_path = get_image_label(from_index, 'operators.operatorframework.io.index.configs.v1')
    if not fbc_dir_path:
        error_msg = f'Index image {from_index} does not contain file-based catalog.'
        log.error(error_msg)
        raise IIBError(error_msg)

    _copy_files_from_image(from_index, fbc_dir_path, base_dir)
    return os.path.join(base_dir, os.path.basename(fbc_dir_path))


def get_hidden_index_database(from_index, base_dir):
    """
    Get hidden database file from the specified index image and save it locally.

    :param str from_index: index image to get database file from.
    :param str base_dir: base directory to which the database file should be saved.
    :return: path to the copied database file.
    :rtype: str
    """
    from iib.workers.tasks.build import _copy_files_from_image

    db_path = get_worker_config()['hidden_index_db_path']
    base_db_file = os.path.join(base_dir, TEMP_INDEX_DB_PATH)
    os.makedirs(os.path.dirname(base_db_file), exist_ok=True)
    _copy_files_from_image(from_index, db_path, base_db_file)
    return base_db_file
