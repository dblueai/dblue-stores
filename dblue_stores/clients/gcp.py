import json
import os

from collections import Mapping

import google.auth

from decouple import config
from google.cloud.storage.client import Client  # pylint: disable=ungrouped-imports
from google.oauth2.service_account import Credentials  # pylint: disable=ungrouped-imports

from ..exceptions import DblueStoresException
from ..logger import logger
from .base import BaseClient

DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)


class GCPClient(BaseClient):

    @classmethod
    def get_client(cls, *args, **kwargs):
        return cls._get_client(*args, **kwargs)

    @staticmethod
    def get_credentials(key_path=None, keyfile_dict=None, scopes=None):
        """
        Returns the Credentials object for Google API
        """
        key_path = key_path or config("GCP_KEY_FILE_PATH", default=None)
        keyfile_dict = keyfile_dict or config("GCP_KEY_FILE_DICT", default=None)
        scopes = scopes or config("GCP_SCOPES", default=None)

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

    @classmethod
    def get_access_token(cls, key_path=None, keyfile_dict=None, credentials=None, scopes=None):
        credentials = credentials or cls.get_credentials(key_path=key_path,
                                                         keyfile_dict=keyfile_dict,
                                                         scopes=scopes)
        return credentials.token

    @classmethod
    def _get_client(cls, project_id=None, key_path=None, keyfile_dict=None, credentials=None, scopes=None):
        credentials = credentials or cls.get_credentials(key_path=key_path,
                                                         keyfile_dict=keyfile_dict,
                                                         scopes=scopes)

        project_id = project_id or config("GCP_PROJECT_ID", default=None)

        return Client(
            project=project_id,
            credentials=credentials
        )
