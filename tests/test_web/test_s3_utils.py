# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import botocore
from botocore.response import StreamingBody

from iib.web import s3_utils


@mock.patch('iib.web.s3_utils.boto3')
def test_get_object_from_s3_bucket(mock_boto3):
    mock_client = mock.Mock()
    mock_boto3.client.return_value = mock_client
    mock_body = StreamingBody('lots of data', 0)
    mock_body.read = mock.Mock(return_value=b'lots of data')
    mock_client.get_object.return_value = {'Body': mock_body}

    response = s3_utils.get_object_from_s3_bucket('prefix', 'file', 's3-bucket')

    assert response == b'lots of data'
    mock_boto3.client.assert_called_once_with('s3')
    mock_client.get_object.assert_called_once_with(Bucket='s3-bucket', Key='prefix/file')


@mock.patch('iib.web.s3_utils.boto3')
def test_get_object_from_s3_bucket_failure(mock_boto3):
    mock_client = mock.Mock()
    mock_boto3.client.return_value = mock_client
    error_msg = {
        'Error': {'Code': 'SomeServiceException', 'Message': 'Something went horribly wrong'}
    }
    mock_client.get_object.side_effect = botocore.exceptions.ClientError(error_msg, 'get_object')

    response = s3_utils.get_object_from_s3_bucket('prefix', 'file', 's3-bucket')
    assert response is None
