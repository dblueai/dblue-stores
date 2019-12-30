from azure.storage.blob import BlockBlobService
from decouple import config

from .base import BaseClient


class AzureClient(BaseClient):

    @classmethod
    def get_client(cls, *args, **kwargs):
        return cls.get_blob_service_connection(*args, **kwargs)

    @staticmethod
    def get_blob_service_connection(account_name=None, account_key=None, connection_string=None):
        account_name = account_name or config("AZURE_ACCOUNT_NAME", default=None)
        account_key = account_key or config("AZURE_ACCOUNT_KEY", default=None)
        connection_string = connection_string or config("AZURE_CONNECTION_STRING", default=None)

        return BlockBlobService(
            account_name=account_name,
            account_key=account_key,
            connection_string=connection_string
        )
