# SPDX-License-Identifier: GPL-3.0-or-later
import os
import re
from unittest import mock

from botocore.exceptions import ClientError
import pytest

from iib.exceptions import IIBError
from iib.workers import s3_utils


@mock.patch('iib.workers.s3_utils.boto3')
def test_upload_file_to_s3_bucket(mock_boto3):
    my_mock = mock.MagicMock()
    mock_boto3.resource.return_value = my_mock
    my_mock.meta.client.upload_file.return_value = None

    s3_utils.upload_file_to_s3_bucket('file', 'prefix', 'file')

    mock_boto3.resource.assert_called_once_with(service_name='s3')
    my_mock.meta.client.upload_file.assert_called_once_with(
        Bucket=None, Filename='file', Key='prefix/file'
    )


@mock.patch('iib.workers.s3_utils.boto3')
def test_upload_file_to_s3_bucket_failure(mock_boto3):
    my_mock = mock.MagicMock()
    mock_boto3.resource.return_value = my_mock
    err_msg = {'Error': {'Code': 400, 'Message': 'Something went horribly wrong'}}
    my_mock.meta.client.upload_file.side_effect = ClientError(err_msg, 'upload')

    error = re.escape(
        'Unable to upload file file to bucket None: An error occurred (400)'
        ' when calling the upload operation: Something went horribly wrong'
    )
    with pytest.raises(IIBError, match=error):
        s3_utils.upload_file_to_s3_bucket('file', 'prefix', 'file')
