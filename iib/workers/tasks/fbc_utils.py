# SPDX-License-Identifier: GPL-3.0-or-later
# This file contains functions that are common for File-Based Catalog image type
import contextlib
import os
import logging
import shutil
import json
from pathlib import Path
from typing import Tuple, List

import ruamel.yaml

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config
from iib.common.tracing import instrument_tracing

log = logging.getLogger(__name__)
yaml = ruamel.yaml.YAML()


def is_image_fbc(image: str) -> bool:
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
    from iib.workers.tasks.utils import get_image_label

    return bool(get_image_label(image, 'operators.operatorframework.io.index.configs.v1'))


@instrument_tracing(span_name='iib.workers.tasks.fbc_utils.get_catalog_dir')
def get_catalog_dir(from_index: str, base_dir: str) -> str:
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

    log.info("Store file-based catalog directory from %s", from_index)
    fbc_dir_path = get_image_label(from_index, 'operators.operatorframework.io.index.configs.v1')
    if not fbc_dir_path:
        error_msg = f'Index image {from_index} does not contain file-based catalog.'
        log.error(error_msg)
        raise IIBError(error_msg)

    _copy_files_from_image(from_index, fbc_dir_path, base_dir)
    return os.path.join(base_dir, os.path.basename(fbc_dir_path))


def get_hidden_index_database(from_index: str, base_dir: str) -> str:
    """
    Get hidden database file from the specified index image and save it locally.

    :param str from_index: index image to get database file from.
    :param str base_dir: base directory to which the database file should be saved.
    :return: path to the copied database file.
    :rtype: str
    """
    from iib.workers.tasks.build import _copy_files_from_image

    log.info("Store hidden index.db from %s", from_index)
    conf = get_worker_config()
    base_db_file = os.path.join(base_dir, conf['temp_index_db_path'])
    os.makedirs(os.path.dirname(base_db_file), exist_ok=True)
    _copy_files_from_image(from_index, conf['hidden_index_db_path'], base_db_file)
    return base_db_file


def merge_catalogs_dirs(src_config: str, dest_config: str):
    """
    Merge two catalog directories by replacing everything from src_config over dest_config.

    :param str src_config: source config directory
    :param str dest_config: destination config directory
    """
    from iib.workers.tasks.opm_operations import opm_validate

    for conf_dir in (src_config, dest_config):
        if not os.path.isdir(conf_dir):
            msg = f"config directory does not exist: {conf_dir}"
            log.error(msg)
            raise IIBError(msg)

    log.info("Merging config folders: %s to %s", src_config, dest_config)
    shutil.copytree(src_config, dest_config, dirs_exist_ok=True)
    enforce_json_config_dir(conf_dir)
    opm_validate(conf_dir)


def extract_fbc_fragment(temp_dir: str, fbc_fragment: str) -> Tuple[str, List[str]]:
    """
    Extract operator packages from the fbc_fragment image.

    :param str temp_dir: base temp directory for IIB request.
    :param str fbc_fragment: pull specification of fbc_fragment in the IIB request.
    :return: fbc_fragment path, fbc_operator_packages.
    :rtype: tuple
    """
    from iib.workers.tasks.build import _copy_files_from_image

    log.info("Extracting the fbc_fragment's catalog from  %s", fbc_fragment)
    # store the fbc_fragment at /tmp/iib-**/fbc-fragment
    conf = get_worker_config()
    fbc_fragment_path = os.path.join(temp_dir, conf['temp_fbc_fragment_path'])
    # Copy fbc_fragment's catalog to /tmp/iib-**/fbc-fragment
    _copy_files_from_image(fbc_fragment, conf['fbc_fragment_catalog_path'], fbc_fragment_path)

    log.info("fbc_fragment extracted at %s", fbc_fragment_path)
    operator_packages = os.listdir(fbc_fragment_path)
    log.info("fbc_fragment contains packages %s", operator_packages)
    if not operator_packages:
        raise IIBError("No operator packages in fbc_fragment %s", fbc_fragment)

    return fbc_fragment_path, operator_packages


def enforce_json_config_dir(config_dir: str) -> None:
    """
    Ensure the files from config dir are in JSON format.

    It will walk recursively and convert any YAML files to the JSON format.

    :param str config_dir: The config dir to walk recursively converting any YAML to JSON.
    """
    log.info("Enforcing JSON content on config_dir: %s", config_dir)
    for dirpath, _, filenames in os.walk(config_dir):
        for file in filenames:
            in_file = os.path.join(dirpath, file)
            if in_file.lower().endswith(".yaml"):
                out_file = os.path.join(dirpath, f"{Path(in_file).stem}.json")
                log.debug(f"Converting {in_file} to {out_file}.")
                # Make sure the output file doesn't exist before opening in append mode
                with contextlib.suppress(FileNotFoundError):
                    os.remove(out_file)
                # The input file may contain multiple chunks, we must append them accordingly
                with open(in_file, 'r') as yaml_in, open(out_file, 'a') as json_out:
                    data = yaml.load_all(yaml_in)
                    for chunk in data:
                        json.dump(chunk, json_out)
                os.remove(in_file)
