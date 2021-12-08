# SPDX-License-Identifier: GPL-3.0-or-later
import logging

from botocore.exceptions import ClientError
import boto3

from iib.exceptions import IIBError
from iib.workers.config import get_worker_config

log = logging.getLogger(__name__)


def upload_file_to_s3_bucket(file_path, s3_key_prefix, s3_file_name):
    """
    Upload artifact file to AWS S3 bucket.

    :param str file_path: the path of the file locally where the artifact file is stored
    :param str s3_key_prefix: the logical location of the file in the S3 bucket
    :param str s3_file_name: the name of the file in S3 bucket
    :raises IIBError: when unable to upload file to the S3 bucket
    """
    conf = get_worker_config()
    log.info(
        'Uploading file %s/%s to S3 bucket: %s',
        s3_key_prefix,
        s3_file_name,
        conf['iib_aws_s3_bucket_name'],
    )
    s3 = boto3.resource(service_name='s3')
    try:
        s3.meta.client.upload_file(
            Filename=file_path,
            Bucket=conf['iib_aws_s3_bucket_name'],
            Key=f'{s3_key_prefix}/{s3_file_name}',
        )
    except ClientError as error:
        log.exception(error)
        raise IIBError(
            f'Unable to upload file {file_path} to bucket {conf["iib_aws_s3_bucket_name"]}: {error}'
        )
