import paramiko

from decouple import config

from .base import BaseClient


class SFTPClient(BaseClient):
    @classmethod
    def get_client(cls, *args, **kwargs):
        return cls._get_client(*args, **kwargs)

    @staticmethod
    def _get_client(host=None, port=None, username='root', password=None):
        host = host or config("SFTP_HOST", default='127.0.0.1')
        port = port or config("SFTP_PORT", default=22, cast=int)
        username = username or config("SFTP_USER", default='root')
        password = password or config("SFTP_PASSWORD", default='')

        transport = paramiko.Transport(sock=(host, port))
        transport.connect(None, username, password)
        return paramiko.SFTPClient.from_transport(transport)
