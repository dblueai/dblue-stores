import errno
import os

from stat import S_ISDIR

from ..clients.sftp import SFTPClient
from ..exceptions import DblueStoresException
from ..utils import walk
from .base import BaseStore

# pylint:disable=arguments-differ


class SFTPStore(BaseStore):
    """
    SFTP Store
    """
    STORE_TYPE = BaseStore.SFTP_STORE

    def __init__(self, client=None, **kwargs):
        self._client = client
        self._host = kwargs.get('host')
        self._port = kwargs.get('port')
        self._username = kwargs.get('username')
        self._password = kwargs.get('password')

    @property
    def client(self):
        if self._client is None:
            self.set_client(
                host=self._host,
                port=self._port,
                username=self._username,
                password=self._password
            )

        return self._client

    def set_client(self, host=None, port=None, username=None, password=None):
        """
        Sets a new paramiko sftp client.

        Args:
            host: `str`. SFTP host
            port: `int`. SFTP port
            username: `str`.  SFTP username
            password: `str`.  SFTP password


        Returns:
            SFTP paramiko client
        """
        self._client = SFTPClient.get_client(
            host=host,
            port=port,
            username=username,
            password=password
        )

    def close(self):
        """Close client connection"""
        if self._client:
            self._client.close()

    def ls(self, path="/"):
        return self.list(path=path)

    def list(self, path="/"):
        dirs = []
        files = []

        for info in self.client.listdir_attr(path):
            item = {
                "name": info.filename,
                "path": os.path.join(path, info.filename),
                "size": info.st_size,
                "updated_at": info.st_mtime,
                "type": self.BLOB_TYPE_DIR if S_ISDIR(info.st_mode) else self.BLOB_TYPE_FILE
            }

            if S_ISDIR(info.st_mode):
                dirs.append(item)
            else:
                files.append(item)

        return {
            'dirs': dirs,
            'files': files,
        }

    def delete(self, path):
        try:
            if S_ISDIR(self.client.lstat(path).st_mode):
                self.client.rmdir(path)
            else:
                self.client.remove(path)
        except OSError:
            raise DblueStoresException("Failed to delete {}" % path)

    def download_file(self, remote_path, local_path):
        self.client.get(remote_path, local_path)

    def upload_file(self, local_path, remote_path):
        self.client.put(local_path, remote_path)

    def upload_dir(self, local_dir, remote_dir):

        if not os.path.exists(local_dir):
            return

        if not self.exists(remote_dir):
            self.client.mkdir(remote_dir)

        with walk(local_dir) as files:
            for f in files:
                remote_path = os.path.join(remote_dir, f)

                if not self.exists(os.path.dirname(remote_path)):
                    self.client.mkdir(os.path.dirname(remote_path))

                self.upload_file(f, remote_path)

    def exists(self, path):

        try:
            self.client.stat(path)
        except IOError as e:
            if e.errno == errno.ENOENT:
                return False
            raise
        else:
            return True

    def download_dir(self, remote_dir, local_dir):
        if not self.exists(remote_dir):
            return

        if not os.path.exists(local_dir):
            os.mkdir(local_dir)

        for info in self.client.listdir_attr(remote_dir):
            remote_path = os.path.join(remote_dir, info.filename)
            local_path = os.path.join(local_dir, info.filename)

            if S_ISDIR(info.st_mode):
                self.download_dir(remote_path, local_path)
            else:
                if not os.path.isfile(os.path.join(local_dir, info.filename)):
                    self.client.get(remote_path, local_path)
