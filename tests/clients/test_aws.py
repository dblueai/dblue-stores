import os

from unittest import TestCase

import boto3

from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from moto import mock_s3

from dblue_stores.clients.aws import AWSClient


class TestAwsClient(TestCase):

    @mock_s3
    def test_get_aws_session(self):
        session = AWSClient.get_session(
            access_key='a1',
            secret_key='a2',
            session_token='a3',
            region_name='a4'
        )

        assert isinstance(session, boto3.session.Session)
        assert session.region_name == 'a4'
        credentials = session.get_credentials()
        assert credentials.access_key == 'a1'
        assert credentials.secret_key == 'a2'
        assert credentials.token == 'a3'

        os.environ['AWS_ACCESS_KEY'] = 'b1'
        os.environ['AWS_SECRET_KEY'] = 'b2'
        os.environ['AWS_SECURITY_TOKEN'] = 'b3'
        os.environ['AWS_REGION'] = 'b4'
        session = AWSClient.get_session()
        assert isinstance(session, boto3.session.Session)
        assert session.region_name == 'b4'
        credentials = session.get_credentials()
        assert credentials.access_key == 'b1'
        assert credentials.secret_key == 'b2'
        assert credentials.token == 'b3'

    @mock_s3
    def test_get_client(self):
        s3_client = AWSClient.get_client('s3')
        assert isinstance(s3_client, BaseClient)

    @mock_s3
    def test_get_resource(self):
        s3_resource = AWSClient.get_resource('s3')
        assert isinstance(s3_resource, ServiceResource)
