# SPDX-License-Identifier: GPL-3.0-or-later
import logging
from typing import Optional

import boto3
from botocore.response import StreamingBody

log = logging.getLogger(__name__)


def get_object_from_s3_bucket(
    s3_key_prefix: str,
    s3_file_name: str,
    bucket_name: str,
) -> Optional[StreamingBody]:
    """
    Get object from AWS S3 bucket.

    :param str s3_key_prefix: the logical location of the file in the S3 bucket
    :param str s3_file_name: the name of the file in S3 bucket
    :param str bucket_name: the name of the S3 bucket to fetch the file from
    :return: the body of the S3 object or None
    :rtype: botocore.response.StreamingBody
    """
    file_name = f'{s3_key_prefix}/{s3_file_name}'
    log.info('getting file from s3 : %s', file_name)
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        return response['Body'].read()
    except Exception as error:
        log.exception('Unable to fetch object %s from bucket %s: %s', file_name, bucket_name, error)
        return None
    finally:
        s3_client.close()
