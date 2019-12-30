import json
import os

from .base import BaseStore
from .. import settings
from ..exceptions import DblueStoresException


class StoreManager(object):
    """
    A convenient class to map experiment/job outputs/data paths to a given/configured store.
    """

    def __init__(self, store=None, path=None):
        self._path = path
        if not store:
            store = BaseStore.get_store()
        if isinstance(store, BaseStore):
            self._store = store
        else:
            raise DblueStoresException('Received an unrecognised store `{}`.'.format(store))

    @classmethod
    def get_for_type(cls, store_type, store_access):
        store = BaseStore.get_store_for_type(store_type=store_type, store_access=store_access)
        return cls(store=store)

    @classmethod
    def get_credential_for_dataset(cls, dataset_id):
        credential_file_path = "{}/{}.json".format(settings.CREDENTIALS_AUTH_MOUNT_PATH, dataset_id)
        with open(credential_file_path) as f:
            credential = json.load(f)
            return credential

    @classmethod
    def get_store_for_dataset(cls, dataset_id):
        try:
            credential = cls.get_credential_for_dataset(dataset_id)

            store_type = credential.get("store") or BaseStore.get_store_type_from_path(credential.get("bucket"))
            store_access = credential.get("secret")
            bucket = credential.get("bucket")

            store = BaseStore.get_store_for_type(store_type=store_type, store_access=store_access)
            return cls(store=store, path=bucket)
        except IOError:
            raise DblueStoresException("Unable to get credential for %s", dataset_id)
        except Exception as e:  # handle other exceptions such as attribute errors
            raise DblueStoresException("Unable to create store: %s", e)

    @classmethod
    def get_store_for_path(cls, path, store_access):
        store_type = BaseStore.get_store_type_from_path(path)
        store = BaseStore.get_store_for_type(store_type=store_type, store_access=store_access)
        return cls(store=store, path=path)

    def set_store(self, store):
        self._store = store

    def set_path(self, path):
        self._path = path

    def set_env_vars(self):
        """Set authentication and access of the current store to the env vars"""
        if self.store:
            self.store.set_env_vars()

    @property
    def store(self):
        return self._store

    @property
    def path(self):
        return self._path

    def ls(self, path, sort=True):
        if self._path:  # We assume rel paths
            path = os.path.join(self._path, path)
        results = self.store.ls(path)
        if sort:
            return {'files': sorted(results['files']), 'dirs': sorted(results['dirs'])}
        return results

    def list(self, path):
        if self._path:  # We assume rel paths
            path = os.path.join(self._path, path)
        return self.store.list(path)

    def delete(self, path):
        return self.store.delete(path)

    def upload_file(self, filename, path=None, **kwargs):
        path = path or self._path
        self.store.upload_file(filename, path, **kwargs)

    def upload_dir(self, dirname, path=None, **kwargs):
        path = path or self._path
        self.store.upload_dir(dirname, path, **kwargs)

    def download_file(self, filename, local_path=None, use_basename=False, **kwargs):
        if self._path:  # We assume rel paths
            file_path = os.path.join(self._path, filename)
            local_path = local_path or filename
        else:
            file_path = filename
        self.store.download_file(file_path, local_path, use_basename=use_basename, **kwargs)

    def download_dir(self, dirname, local_path=None, use_basename=False, **kwargs):
        if self._path:  # We assume rel paths
            dir_path = os.path.join(self._path, dirname)
            local_path = local_path or dirname
        else:
            dir_path = dirname
        self.store.download_dir(dir_path, local_path, use_basename=use_basename, **kwargs)
