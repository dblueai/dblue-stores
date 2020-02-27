import os

from six import BytesIO
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from ..clients.aws import AWSClient
from ..exceptions import DblueStoresException
from ..logger import logger
from ..utils import append_basename, check_dir_exists, force_bytes, walk
from .base import BaseStore

# pylint:disable=arguments-differ


class S3Store(BaseStore):
    """
    S3 store Service using Boto3.
    """
    STORE_TYPE = BaseStore.S3_STORE
    ENCRYPTION = "AES256"

    def __init__(self, client=None, resource=None, **kwargs):
        self._client = client
        self._resource = resource

        self._encoding = kwargs.get('encoding', 'utf-8')
        self._endpoint_url = kwargs.get('endpoint_url')
        self._access_key = kwargs.get('access_key')
        self._secret_key = kwargs.get('secret_key')
        self._session_token = kwargs.get('session_token')
        self._region_name = kwargs.get('region_name')
        self._verify_ssl = kwargs.get('verify_ssl', True)
        self._use_ssl = kwargs.get('use_ssl', True)
        self._legacy_api = kwargs.get('legacy_api', False)

    @property
    def client(self):
        if self._client is None:
            self.set_client(
                endpoint_url=self._endpoint_url,
                access_key=self._access_key,
                secret_key=self._secret_key,
                session_token=self._session_token,
                region_name=self._region_name,
                use_ssl=self._use_ssl,
                verify_ssl=self._verify_ssl
            )
        return self._client

    def set_env_vars(self):
        if self._endpoint_url:
            os.environ['AWS_ENDPOINT_URL'] = self._endpoint_url
        if self._access_key:
            os.environ['AWS_ACCESS_KEY'] = self._access_key
        if self._secret_key:
            os.environ['AWS_SECRET_KEY'] = self._secret_key
        if self._session_token:
            os.environ['AWS_SECURITY_TOKEN'] = self._session_token
        if self._region_name:
            os.environ['AWS_REGION'] = self._region_name
        if self._use_ssl is not None:
            os.environ['AWS_USE_SSL'] = self._use_ssl
        if self._verify_ssl is not None:
            os.environ['AWS_VERIFY_SSL'] = self._verify_ssl
        if self._legacy_api:
            os.environ['AWS_LEGACY_API'] = self._legacy_api

    @property
    def resource(self):
        if self._resource is None:
            self.set_resource(
                endpoint_url=self._endpoint_url,
                access_key=self._access_key,
                secret_key=self._secret_key,
                session_token=self._session_token,
                region_name=self._region_name
            )
        return self._resource

    def set_client(self,
                   endpoint_url=None,
                   access_key=None,
                   secret_key=None,
                   session_token=None,
                   region_name=None,
                   use_ssl=True,
                   verify_ssl=None):
        """
        Sets a new s3 boto3 client.

        Args:
            endpoint_url: `str`. The complete URL to use for the constructed client.
            access_key: `str`. The access key to use when creating the client.
            secret_key: `str`. The secret key to use when creating the client.
            session_token: `str`. The session token to use when creating the client.
            region_name: `str`. The name of the region associated with the client.
                A client is associated with a single region.

        Returns:
            Service client instance
        """
        self._client = AWSClient.get_client(
            's3',
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            region_name=region_name,
            use_ssl=use_ssl,
            verify_ssl=verify_ssl
        )

    def set_resource(self,
                     endpoint_url=None,
                     access_key=None,
                     secret_key=None,
                     session_token=None,
                     region_name=None):
        """
        Sets a new s3 boto3 resource.

        Args:
            endpoint_url: `str`. The complete URL to use for the constructed client.
            access_key: `str`. The access key to use when creating the client.
            secret_key: `str`. The secret key to use when creating the client.
            session_token: `str`. The session token to use when creating the client.
            region_name: `str`. The name of the region associated with the client.
                A client is associated with a single region.

        Returns:
             Service resource instance
        """
        self._resource = AWSClient.get_resource(
            's3',
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            region_name=region_name)

    @staticmethod
    def parse_s3_url(s3_url):
        """
        Parses and validates an S3 url.

        Returns:
             tuple(bucket_name, key).
        """
        parsed_url = urlparse(s3_url)
        if not parsed_url.netloc:
            raise DblueStoresException('Received an invalid S3 url `{}`'.format(s3_url))
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip('/')
            return bucket_name, key

    @staticmethod
    def check_prefix_format(prefix, delimiter):
        if not delimiter or not prefix:
            return prefix
        return prefix + delimiter if prefix[-1] != delimiter else prefix

    def check_bucket(self, bucket_name):
        """
        Checks if a buckete exists.

        Args:
            bucket_name: `str`. Name of the bucket
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            logger.info(e.response["Error"]["Message"])
            return False

    def get_bucket(self, bucket_name):
        """
        Gets a bucket by name.

        Args:
            bucket_name: `str`. Name of the bucket
        """
        return self.resource.Bucket(bucket_name)

    def ls(self, path):
        (bucket_name, key) = self.parse_s3_url(path)
        results = self.list(bucket_name=bucket_name, prefix=key)
        return {'files': results['keys'], 'dirs': results['prefixes']}

    def list(self,
             bucket_name,
             prefix='',
             delimiter='/',
             page_size=None,
             max_items=None,
             keys=True,
             prefixes=True):
        """
        Lists prefixes and contents in a bucket under prefix.

        Args:
            bucket_name: `str`. the name of the bucket
            prefix: `str`. a key prefix
            delimiter: `str`. the delimiter marks key hierarchy.
            page_size: `str`. pagination size
            max_items: `int`. maximum items to return
            keys: `bool`. if it should include keys
            prefixes: `boll`. if it should include prefixes
        """
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        legacy_api = AWSClient.get_legacy_api(legacy_api=self._legacy_api)

        if legacy_api:
            paginator = self.client.get_paginator('list_objects')
        else:
            paginator = self.client.get_paginator('list_objects_v2')

        prefix = self.check_prefix_format(prefix=prefix, delimiter=delimiter)
        response = paginator.paginate(Bucket=bucket_name,
                                      Prefix=prefix,
                                      Delimiter=delimiter,
                                      PaginationConfig=config)

        def get_keys(contents):
            list_keys = []
            for cont in contents:
                # To solve empty blob issue
                if prefix == cont['Key']:
                    continue

                list_keys.append((cont['Key'][len(prefix):], cont.get('Size')))

            return list_keys

        def get_prefixes(page_prefixes):
            list_prefixes = []
            for pref in page_prefixes:
                list_prefixes.append(pref['Prefix'][len(prefix): -1])
            return list_prefixes

        results = {
            'keys': [],
            'prefixes': []
        }
        for page in response:
            if prefixes:
                results['prefixes'] += get_prefixes(page.get('CommonPrefixes', []))
            if keys:
                results['keys'] += get_keys(page.get('Contents', []))

        return results

    def list_prefixes(self, bucket_name, prefix='', delimiter='', page_size=None, max_items=None):
        """
        Lists prefixes in a bucket under prefix

        Args:
            bucket_name: `str`. the name of the bucket
            prefix: `str`. a key prefix
            delimiter: `str`. the delimiter marks key hierarchy.
            page_size: `int`. pagination size
            max_items: `int`. maximum items to return
        """
        results = self.list(bucket_name=bucket_name,
                            prefix=prefix,
                            delimiter=delimiter,
                            page_size=page_size,
                            max_items=max_items,
                            keys=False,
                            prefixes=True)
        return results['prefixes']

    def list_keys(self, bucket_name, prefix='', delimiter='', page_size=None, max_items=None):
        """
        Lists keys in a bucket under prefix and not containing delimiter

        Args:
            bucket_name: `str`. the name of the bucket
            prefix: `str`. a key prefix
            delimiter: `str`. the delimiter marks key hierarchy.
            page_size: `int`. pagination size
            max_items: `int`. maximum items to return
        """
        results = self.list(bucket_name=bucket_name,
                            prefix=prefix,
                            delimiter=delimiter,
                            page_size=page_size,
                            max_items=max_items,
                            keys=True,
                            prefixes=False)
        return results['keys']

    def check_key(self, key, bucket_name=None):
        """
        Checks if a key exists in a bucket

        Args:
            key: `str`. S3 key that will point to the file
            bucket_name: `str`. Name of the bucket in which the file is stored
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            logger.info(e.response["Error"]["Message"])
            return False

    def get_key(self, key, bucket_name=None):
        """
        Returns a boto3.s3.Object

        Args:
            key: `str`. the path to the key.
            bucket_name: `str`. the name of the bucket.
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        try:
            obj = self.resource.Object(bucket_name, key)
            obj.load()
            return obj
        except Exception as e:
            raise DblueStoresException(e)

    def read_key(self, key, bucket_name=None):
        """
        Reads a key from S3

        Args:
            key: `str`. S3 key that will point to the file.
            bucket_name: `str`. Name of the bucket in which the file is stored.
        """

        obj = self.get_key(key, bucket_name)
        return obj.get()['Body'].read().decode('utf-8')

    def upload_bytes(self,
                     bytes_data,
                     key,
                     bucket_name=None,
                     overwrite=False,
                     encrypt=False,
                     acl=None):
        """
        Uploads bytes to S3

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        Args:
            bytes_data: `bytes`. bytes to set as content for the key.
            key: `str`. S3 key that will point to the file.
            bucket_name: `str`. Name of the bucket in which to store the file.
            overwrite: `bool`. A flag to decide whether or not to overwrite the key
                if it already exists.
            encrypt: `bool`. If True, the file will be encrypted on the server-side
                by S3 and will be stored in an encrypted form while at rest in S3.
            acl: `str`. ACL to use for uploading, e.g. "public-read".
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        if not overwrite and self.check_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = self.ENCRYPTION
        if acl:
            extra_args['ACL'] = acl

        filelike_buffer = BytesIO(bytes_data)

        self.client.upload_fileobj(filelike_buffer, bucket_name, key, ExtraArgs=extra_args)

    def upload_string(self,
                      string_data,
                      key,
                      bucket_name=None,
                      overwrite=False,
                      encrypt=False,
                      acl=None,
                      encoding='utf-8'):
        """
        Uploads a string to S3.

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        Args:
            string_data: `str`. string to set as content for the key.
            key: `str`. S3 key that will point to the file.
            bucket_name: `str`. Name of the bucket in which to store the file.
            overwrite: `bool`. A flag to decide whether or not to overwrite the key
                if it already exists.
            encrypt: `bool`. If True, the file will be encrypted on the server-side
                by S3 and will be stored in an encrypted form while at rest in S3.
            acl: `str`. ACL to use for uploading, e.g. "public-read".
            encoding: `str`. Encoding to use.
        """
        self.upload_bytes(force_bytes(string_data, encoding=encoding),
                          key=key,
                          bucket_name=bucket_name,
                          overwrite=overwrite,
                          encrypt=encrypt,
                          acl=acl)

    def upload_file(self,
                    filename,
                    key,
                    bucket_name=None,
                    overwrite=False,
                    encrypt=False,
                    acl=None,
                    use_basename=True):
        """
        Uploads a local file to S3.

        Args:
            filename: `str`. name of the file to upload.
            key: `str`. S3 key that will point to the file.
            bucket_name: `str`. Name of the bucket in which to store the file.
            overwrite: `bool`. A flag to decide whether or not to overwrite the key
                if it already exists. If replace is False and the key exists, an
                error will be raised.
            encrypt: `bool`. If True, the file will be encrypted on the server-side
                by S3 and will be stored in an encrypted form while at rest in S3.
            acl: `str`. ACL to use for uploading, e.g. "public-read".
            use_basename: `bool`. whether or not to use the basename of the filename.
        """
        if not bucket_name:
            bucket_name, key = self.parse_s3_url(key)

        if use_basename:
            key = append_basename(key, filename)

        if not overwrite and self.check_key(key, bucket_name):
            raise DblueStoresException("The key {} already exists.".format(key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = self.ENCRYPTION
        if acl:
            extra_args['ACL'] = acl

        self.client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args)

    def download_file(self, key, local_path, bucket_name=None, use_basename=True):
        """
        Download a file from S3.

        Args:
            key: `str`. S3 key that will point to the file.
            local_path: `str`. the path to download to.
            bucket_name: `str`. Name of the bucket in which to store the file.
            use_basename: `bool`. whether or not to use the basename of the key.
        """
        if not bucket_name:
            bucket_name, key = self.parse_s3_url(key)

        local_path = os.path.abspath(local_path)

        if use_basename:
            local_path = append_basename(local_path, key)

        check_dir_exists(local_path)

        try:
            self.client.download_file(bucket_name, key, local_path)
        except ClientError as e:
            raise DblueStoresException(e)

    def upload_dir(self,
                   dirname,
                   key,
                   bucket_name=None,
                   overwrite=False,
                   encrypt=False,
                   acl=None,
                   use_basename=True):
        """
        Uploads a local directory to S3.

        Args:
            dirname: `str`. name of the directory to upload.
            key: `str`. S3 key that will point to the file.
            bucket_name: `str`. Name of the bucket in which to store the file.
            overwrite: `bool`. A flag to decide whether or not to overwrite the key
                if it already exists. If replace is False and the key exists, an
                error will be raised.
            encrypt: `bool`. If True, the file will be encrypted on the server-side
                by S3 and will be stored in an encrypted form while at rest in S3.
            acl: `str`. ACL to use for uploading, e.g. "public-read".
            use_basename: `bool`. whether or not to use the basename of the directory.
        """
        if not bucket_name:
            bucket_name, key = self.parse_s3_url(key)

        if use_basename:
            key = append_basename(key, dirname)

        # Turn the path to absolute paths
        dirname = os.path.abspath(dirname)
        with walk(dirname) as files:
            for f in files:
                file_key = os.path.join(key, os.path.relpath(f, dirname))
                self.upload_file(filename=f,
                                 key=file_key,
                                 bucket_name=bucket_name,
                                 overwrite=overwrite,
                                 encrypt=encrypt,
                                 acl=acl,
                                 use_basename=False)

    def download_dir(self, key, local_path, bucket_name=None, use_basename=True):
        """
        Download a directory from S3.

        Args:
            key: `str`. S3 key that will point to a directory.
            local_path: `str`. the path to download to.
            bucket_name: `str`. Name of the bucket in which to store the file.
            use_basename: `bool`. whether or not to use the basename of the key.
        """
        if not bucket_name:
            bucket_name, key = self.parse_s3_url(key)

        local_path = os.path.abspath(local_path)

        if use_basename:
            local_path = append_basename(local_path, key)

        try:
            check_dir_exists(local_path, is_dir=True)
        except DblueStoresException:
            os.makedirs(local_path)

        results = self.list(bucket_name=bucket_name, prefix=key, delimiter='/')

        # Create directories
        for prefix in sorted(results['prefixes']):
            direname = os.path.join(local_path, prefix)
            prefix = os.path.join(key, prefix)
            # Download files under
            self.download_dir(key=prefix,
                              local_path=direname,
                              bucket_name=bucket_name,
                              use_basename=False)

        # Download files
        for file_key in results['keys']:
            file_key = file_key[0]
            filename = os.path.join(local_path, file_key)
            file_key = os.path.join(key, file_key)
            self.download_file(key=file_key,
                               local_path=filename,
                               bucket_name=bucket_name,
                               use_basename=False)

    def delete(self, key, bucket_name=None):
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        results = self.list(bucket_name=bucket_name, prefix=key, delimiter='/')

        if not any([results['prefixes'], results['keys']]):
            self.delete_file(key=key, bucket_name=bucket_name)

        # Delete directories
        for prefix in sorted(results['prefixes']):
            prefix = os.path.join(key, prefix)
            # Download files under
            self.delete(key=prefix, bucket_name=bucket_name)

        # Delete files
        for file_key in results['keys']:
            file_key = file_key[0]
            file_key = os.path.join(key, file_key)
            self.delete_file(key=file_key, bucket_name=bucket_name)

    def delete_file(self, key, bucket_name=None):
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)
        try:
            obj = self.resource.Object(bucket_name, key)
            obj.delete()
        except ClientError as e:
            raise DblueStoresException(e)
