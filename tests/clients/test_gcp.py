from unittest import TestCase

import mock

from dblue_stores.clients.gcp import GCPClient
from dblue_stores.exceptions import DblueStoresException

GCS_MODULE = 'dblue_stores.clients.gcp.{}'


class TestGCClient(TestCase):
    @mock.patch(GCS_MODULE.format('google.auth.default'))
    def test_get_default_gc_credentials(self, default_auth):
        default_auth.return_value = None, None
        credentials = GCPClient.get_credentials()
        assert default_auth.call_count == 1
        assert credentials is None

    @mock.patch(GCS_MODULE.format('Credentials.from_service_account_file'))
    def test_get_key_path_gc_credentials(self, service_account):
        with self.assertRaises(DblueStoresException):
            GCPClient.get_credentials(key_path='key_path')

        service_account.return_value = None
        credentials = GCPClient.get_credentials(key_path='key_path.json')
        assert service_account.call_count == 1
        assert credentials is None

    @mock.patch(GCS_MODULE.format('Credentials.from_service_account_info'))
    def test_get_keyfile_dict_gc_credentials(self, service_account):
        with self.assertRaises(DblueStoresException):
            GCPClient.get_credentials(keyfile_dict='keyfile_dict')

        service_account.return_value = None

        credentials = GCPClient.get_credentials(keyfile_dict={'private_key': 'key'})
        assert service_account.call_count == 1
        assert credentials is None

        credentials = GCPClient.get_credentials(keyfile_dict='{"private_key": "private_key"}')
        assert service_account.call_count == 2
        assert credentials is None

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_get_client(self, client, gc_credentials):
        GCPClient.get_client()
        assert gc_credentials.call_count == 1
        assert client.call_count == 1
