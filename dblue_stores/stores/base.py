from ..exceptions import DblueStoresException


class BaseStore:
    """
    A base store interface.
    """
    LOCAL_STORE = 'local'
    AZURE_STORE = 'azure-storage'
    S3_STORE = 's3'
    GCS_STORE = 'gcs'
    SFTP_STORE = 'sftp'
    STORE_TYPES = {LOCAL_STORE, AZURE_STORE, S3_STORE, GCS_STORE, SFTP_STORE}

    STORE_TYPE = None

    BLOB_TYPE_DIR = "DIR"
    BLOB_TYPE_FILE = "FILE"

    @classmethod
    def get_store(cls, store_type=None, **kwargs):
        # We assume that `None` refers to local store as well
        store_type = cls.LOCAL_STORE if store_type is None else store_type
        if store_type not in cls.STORE_TYPES:
            raise DblueStoresException(
                'Received an unrecognised store type `{}`.'.format(store_type))

        if store_type == cls.LOCAL_STORE:
            from .local import LocalStore
            return LocalStore()
        if store_type == cls.AZURE_STORE:
            from .azure import AzureStore
            return AzureStore(**kwargs)
        if store_type == cls.S3_STORE:
            from .s3 import S3Store
            return S3Store(**kwargs)
        if store_type == cls.GCS_STORE:
            from .gcs import GCSStore
            return GCSStore(**kwargs)
        if store_type == cls.SFTP_STORE:
            from .sftp import SFTPStore
            return SFTPStore(**kwargs)

        raise DblueStoresException(
            'Received an unrecognised store type `{}`.'.format(store_type))

    @classmethod
    def get_store_type_from_path(cls, path):
        store_type = BaseStore.LOCAL_STORE

        if path.startswith("gs://"):
            store_type = BaseStore.GCS_STORE
        elif path.startswith("s3://"):
            store_type = BaseStore.S3_STORE
        elif path.startswith("wasbs://"):
            store_type = BaseStore.AZURE_STORE
        elif path.startswith("sftp://"):
            store_type = BaseStore.SFTP_STORE

        return store_type

    @classmethod
    def get_store_for_type(cls, store_type, store_access):
        if store_type == cls.GCS_STORE:
            return cls.get_store(store_type=store_type, keyfile_dict=store_access)
        return cls.get_store(store_type=store_type, **store_access)

    def set_env_vars(self):
        """Set authentication and access of the current store to the env vars"""
        pass

    @property
    def is_local_store(self):
        return self.STORE_TYPE == self.LOCAL_STORE

    @property
    def is_s3_store(self):
        return self.STORE_TYPE == self.S3_STORE

    @property
    def is_azure_store(self):
        return self.STORE_TYPE == self.AZURE_STORE

    @property
    def is_gcs_store(self):
        return self.STORE_TYPE == self.GCS_STORE

    @property
    def is_sftp_store(self):
        return self.STORE_TYPE == self.SFTP_STORE

    def ls(self, path):
        raise NotImplementedError

    def list(self, *args, **kwargs):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        raise NotImplementedError

    def download_file(self, *args, **kwargs):
        raise NotImplementedError

    def download_dir(self, *args, **kwargs):
        raise NotImplementedError

    def upload_file(self, *args, **kwargs):
        raise NotImplementedError

    def upload_dir(self, *args, **kwargs):
        raise NotImplementedError
