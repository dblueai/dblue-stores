from unittest import TestCase

import os
from azure.storage.blob import BlockBlobService

from dblue_stores.clients.azure import AzureClient


class TestAzureClient(TestCase):
    def test_get_blob_service_connection(self):
        with self.assertRaises(ValueError):
            AzureClient.get_client()

        service = AzureClient.get_client(account_name='foo', account_key='bar')
        assert isinstance(service, BlockBlobService)

        os.environ['AZURE_ACCOUNT_NAME'] = 'foo'
        os.environ['AZURE_ACCOUNT_KEY'] = 'bar'
        service = AzureClient.get_client()
        assert isinstance(service, BlockBlobService)
