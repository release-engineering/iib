# SPDX-License-Identifier: GPL-3.0-or-later
from unittest import mock

import botocore

from iib.web import s3_utils


@mock.patch('iib.web.s3_utils.boto3')
def test_get_object_from_s3_bucket(mock_boto3):
    my_mock = mock.MagicMock()
    mock_boto3.resource.return_value = my_mock
    my_mock.meta.client.get_object.return_value = {
        'ResponseMetadata': {
            'RequestId': 'CK2VG4V5ZQXAAM5B',
            'HostId': 'q4Wp/tsvjnl/eBeN0dHvHYi6xUl9U149BdN6IAXjaFJnnQX+mLjg8hM4xAfgijfDo3fugYSEpPA=',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'x-amz-id-2': 'q4Wp/tsvjnl/eBeN0dHvHYi6xUl9U149BdN6IAXjaFJnnQX+mLjg8hM4xAfgijfDo3fugYSEpPA=',
                'x-amz-request-id': 'CK2VG4V5ZQXAAM5B',
                'date': 'Sun, 05 Dec 2021 03:35:14 GMT',
                'last-modified': 'Sun, 05 Dec 2021 03:29:26 GMT',
                'etag': '"69fefda897b58ced9d5f88df1804564c"',
                'x-amz-server-side-encryption': 'AES256',
                'accept-ranges': 'bytes',
                'content-type': 'binary/octet-stream',
                'server': 'AmazonS3',
                'content-length': '21179',
            },
            'RetryAttempts': 0,
        },
        'AcceptRanges': 'bytes',
        'ContentLength': 21179,
        'ETag': '"69fefda897b58ced9d5f88df1804564c"',
        'ContentType': 'binary/octet-stream',
        'ServerSideEncryption': 'AES256',
        'Metadata': {},
        'Body': 'lots of data present here',
    }

    response = s3_utils.get_object_from_s3_bucket('prefix', 'file', 's3-bucket')

    assert response == 'lots of data present here'
    mock_boto3.resource.assert_called_once_with(service_name='s3')
    my_mock.meta.client.get_object.assert_called_once_with(Bucket='s3-bucket', Key='prefix/file')


@mock.patch('iib.web.s3_utils.boto3')
def test_get_object_from_s3_bucket_failure(mock_boto3):
    my_mock = mock.MagicMock()
    mock_boto3.resource.return_value = my_mock
    error_msg = {
        'Error': {'Code': 'SomeServiceException', 'Message': 'Something went horribly wrong'}
    }
    my_mock.meta.client.get_object.side_effect = botocore.exceptions.ClientError(
        error_msg, 'get_object'
    )

    response = s3_utils.get_object_from_s3_bucket('prefix', 'file', 's3-bucket')
    assert response is None
