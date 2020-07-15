# SPDX-License-Identifier: GPL-3.0-or-later
# This file can be deleted once OMPS is retired
import json
import logging
import os
import shutil
import tempfile

import requests

from iib.exceptions import IIBError
from iib.workers.api_utils import set_request_state
from iib.workers.config import get_worker_config
from iib.workers.tasks.utils import get_image_labels, retry, run_cmd

log = logging.getLogger(__name__)


def export_legacy_packages(packages, request_id, rebuilt_index_image, cnr_token, organization):
    """
    Export packages to be backported and push them via OMPS.

    :param set packages: a set of strings representing the names of the packages to be exported.
    :param int request_id: the ID of the IIB build request.
    :param str rebuilt_index_image: the pull specification of the index image rebuilt by IIB.
    :param str cnr_token: the token required to push backported packages to the legacy
        app registry via OMPS.
    :param str organization: the organization name in the legacy app registry to which the
        backported packages should be pushed to.
    :raises IIBError: if the export of packages fails.
    """
    with tempfile.TemporaryDirectory(prefix='iib-') as temp_dir:
        for package in packages:
            _opm_index_export(rebuilt_index_image, package, temp_dir)
            package_dir = os.path.join(temp_dir, package)
            _verify_package_info(package_dir, rebuilt_index_image)
            _zip_package(package_dir)
            _push_package_manifest(package_dir, cnr_token, organization)

    set_request_state(request_id, 'in_progress', 'Back ported packages successfully pushed to OMPS')


def _get_base_dir_and_pkg_name(package_dir):
    """
    Get the base directory and the package name from package directory.

    :param str package_dir: path to the exported package directory
    :return: base_dir, package name
    :rtype: str, str
    """
    return os.path.dirname(package_dir), os.path.basename(package_dir)


def get_legacy_support_packages(bundles, request_id, ocp_version, force_backport=False):
    """
    Get the packages that must be pushed to the legacy application registry.

    :param list<str> bundles: a list of strings representing the pull specifications of the bundles
        to add to the index image being built.
    :param int request_id: the ID of the IIB build request.
    :param str ocp_version: the OCP version that the index is intended for.
    :param bool force_backport: if True, backport legacy support is forced for every package
    :return: a set of packages that require legacy support
    :rtype: set
    """
    packages = set()
    if ocp_version != 'v4.5':
        log.info('Backport legacy support is disabled for %s', ocp_version)
        return packages
    if force_backport:
        set_request_state(request_id, 'in_progress', 'Backport legacy support will be forced')
    for bundle in bundles:
        labels = get_image_labels(bundle)
        if force_backport or labels.get('com.redhat.delivery.backport', False):
            packages.add(labels['operators.operatorframework.io.bundle.package.v1'])

    return packages


@retry(attempts=2, wait_on=IIBError, logger=log)
def _opm_index_export(rebuilt_index_image, package, temp_dir):
    """
    Export the package that needs to be backported.

    :param str rebuilt_index_image: the pull specification of the index image rebuilt by IIB.
    :param set package: a string representing the name of the package to be exported.
    :param str temp_dir: path to the temporary directory where the package will be exported to.
    :raises IIBError: if the export of packages fails.
    """
    cmd = [
        'opm',
        'index',
        'export',
        '--index',
        rebuilt_index_image,
        '--package',
        package,
        '--download-folder',
        package,
    ]

    log.info('Generating the backported operator for package: %s', package)

    run_cmd(
        cmd,
        {'cwd': temp_dir},
        exc_msg=f'Failed to push {package} to the legacy application registry',
    )


def _push_package_manifest(package_dir, cnr_token, organization):
    """
    Push ``manifests.zip`` file created for an exported package to OMPS.

    :param str package_dir: path to the exported package directory.
    :param str cnr_token: the token required to push backported packages to the legacy
        app registry via OMPS.
    :param str organization: the organization name in the legacy app registry to which
         the backported packages should be pushed to.
    :raises IIBError: if the push is unsucessful
    """
    conf = get_worker_config()
    base_dir, _ = _get_base_dir_and_pkg_name(package_dir)
    with open(f'{base_dir}/manifests.zip', 'rb') as fobj:
        files = {'file': (fobj.name, fobj)}
        log.info('Files are %s', files)
        resp = requests.post(
            f'{conf["iib_omps_url"]}{organization}/zipfile',
            headers={'Authorization': cnr_token},
            files=files,
        )
        if not resp.ok:
            log.error('Request to OMPS failed: %s', resp.text)
            try:
                error_msg = resp.json().get('message', 'An unknown error occured')
            except json.JSONDecodeError:
                error_msg = resp.text
            raise IIBError(
                f'Push to {organization} in the legacy app registry was unsucessful: {error_msg}'
            )


def validate_legacy_params_and_config(packages, bundles, cnr_token, organization):
    """
    Valiate parameters and config variables required for legacy support.

    :param set packages: a set of strings representing the names of the packages to be exported.
    :param list bundles: a list of strings representing the bundles to be added to the index image.
    :param str cnr_token: the token required to push backported packages to the legacy
        app registry via OMPS.
    :param str organization: organization name in the legacy app registry to which the backported
        packages should be pushed to.
    :raises IIBError: if legacy support is required and necessary params are missing.
    """
    if packages and not all([cnr_token, organization]):
        packages_str = ', '.join(packages)
        raise IIBError(
            f'Legacy support is required for {packages_str};'
            ' Both cnr_token and organization should be non-empty strings'
        )

    conf = get_worker_config()
    if not conf.get('iib_omps_url'):
        log.error('iib_omps_url not set in the Celery config')
        raise IIBError('IIB is not configured to handle the legacy app registry')


def _verify_package_info(package_dir, from_index):
    """
    Verify if the exported package info is generated correctly.

    :param str package_dir: path to the exported package directory
    :param str from_index: the pull specification of the image image
    :raises IIBError: if the generated package info is missing
    """
    _, package_name = _get_base_dir_and_pkg_name(package_dir)
    log.info('Verifying package_name %s', package_name)
    # opm does not fail when the package is missing in the index image, hence we
    # check the number of generated files. If it's equal to 1, that means only an empty
    # `package.yaml` file is generated and the package is missing
    if len(os.listdir(package_dir)) == 1:
        raise IIBError(f'package {package_name} is missing in index image {from_index}')


def _zip_package(package_dir):
    """
    Zip content of exported package to a ``manifests.zip`` file.

    :param str package_dir: path to the exported package directory
    :raises IIBError: if unable to zip the exported package
    """
    base_dir, package_name = _get_base_dir_and_pkg_name(package_dir)
    try:
        shutil.make_archive(f'{base_dir}/manifests', 'zip', package_dir)
    except Exception:
        log.exception('Unable to zip exported package: %s', package_name)
        raise IIBError(f'Unable to zip exported package for {package_name}')
