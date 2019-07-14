# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

from azure.storage.blob import BlockBlobService
from decouple import config


def get_account_name(key='AZURE_ACCOUNT_NAME'):
    return config(key, default=None)


def get_account_key(key='AZURE_ACCOUNT_KEY'):
    return config(key, default=None)


def get_connection_string(key='AZURE_CONNECTION_STRING'):
    return config(key, default=None)


def get_blob_service_connection(account_name=None, account_key=None, connection_string=None):
    account_name = account_name or get_account_name()
    account_key = account_key or get_account_key()
    connection_string = connection_string or get_connection_string()
    return BlockBlobService(account_name=account_name,
                            account_key=account_key,
                            connection_string=connection_string)
