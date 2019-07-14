# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import json
import os

from collections import Mapping

import google.auth
import google.oauth2.service_account

from google.cloud.storage.client import Client
from google.oauth2.service_account import Credentials

from dblue_stores.exceptions import DblueStoresException
from dblue_stores.logger import logger

from decouple import config

DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


def get_project_id(key=None):
    return config(key or 'GCP_PROJECT_ID')


def get_key_path(key=None):
    return config(key or 'GCP_KEY_FILE_PATH')


def get_keyfile_dict(key=None):
    return config(key or 'GCP_KEY_FILE_DICT')


def get_scopes(key=None):
    return config(key or 'GCP_SCOPES')


def get_gc_credentials(key_path=None, keyfile_dict=None, scopes=None):
    """
    Returns the Credentials object for Google API
    """
    key_path = key_path or get_key_path()
    keyfile_dict = keyfile_dict or get_keyfile_dict()
    scopes = scopes or get_scopes()

    if scopes is not None:
        scopes = [s.strip() for s in scopes.split(',')]
    else:
        scopes = DEFAULT_SCOPES

    if not key_path and not keyfile_dict:
        logger.info('Getting connection using `google.auth.default()` '
                    'since no key file is defined for hook.')
        credentials, _ = google.auth.default(scopes=scopes)
    elif key_path:
        # Get credentials from a JSON file.
        if key_path.endswith('.json'):
            logger.info('Getting connection using a JSON key file.')
            credentials = Credentials.from_service_account_file(
                os.path.abspath(key_path), scopes=scopes)
        else:
            raise DblueStoresException('Unrecognised extension for key file.')
    else:
        # Get credentials from JSON data.
        try:
            if not isinstance(keyfile_dict, Mapping):
                keyfile_dict = json.loads(keyfile_dict)

            # Convert escaped newlines to actual newlines if any.
            keyfile_dict['private_key'] = keyfile_dict['private_key'].replace('\\n', '\n')

            credentials = Credentials.from_service_account_info(keyfile_dict, scopes=scopes)
        except ValueError:  # json.decoder.JSONDecodeError does not exist on py2
            raise DblueStoresException('Invalid key JSON.')

    return credentials


def get_gc_access_token(key_path=None, keyfile_dict=None, credentials=None, scopes=None):
    credentials = credentials or get_gc_credentials(key_path=key_path,
                                                    keyfile_dict=keyfile_dict,
                                                    scopes=scopes)
    return credentials.token


def get_gc_client(project_id=None, key_path=None, keyfile_dict=None, credentials=None, scopes=None):
    credentials = credentials or get_gc_credentials(key_path=key_path,
                                                    keyfile_dict=keyfile_dict,
                                                    scopes=scopes)
    project_id = project_id or get_project_id()
    return Client(
        project=project_id,
        credentials=credentials
    )
