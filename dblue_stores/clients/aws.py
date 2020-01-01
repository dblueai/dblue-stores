import boto3

from decouple import config

from .base import BaseClient


class AWSClient(BaseClient):
    @classmethod
    def get_client(cls, *args, **kwargs):
        return cls._get_client(*args, **kwargs)

    @staticmethod
    def get_legacy_api(legacy_api=False):
        legacy_api = legacy_api or config("AWS_LEGACY_API", default=False, cast=bool)
        return legacy_api

    @staticmethod
    def get_session(access_key=None, secret_key=None, session_token=None, region_name=None):

        access_key = access_key or config("AWS_ACCESS_KEY", default=None)
        secret_key = secret_key or config("AWS_SECRET_KEY", default=None)
        session_token = session_token or config("AWS_SECURITY_TOKEN", default=None)
        region_name = region_name or config("AWS_REGION", default=None)

        return boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region_name
        )

    @classmethod
    def _get_client(cls,
                    client_type,
                    endpoint_url=None,
                    access_key=None,
                    secret_key=None,
                    session_token=None,
                    region_name=None,
                    use_ssl=True,
                    verify_ssl=None):

        session = cls.get_session(
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            region_name=region_name
        )

        endpoint_url = endpoint_url or config("AWS_ENDPOINT_URL", default=None)
        use_ssl = use_ssl or config("AWS_USE_SSL", default=True, cast=bool)

        if verify_ssl is None:
            verify_ssl = config("AWS_VERIFY_SSL", default=True, cast=bool)
        else:
            verify_ssl = verify_ssl

        return session.client(
            client_type,
            endpoint_url=endpoint_url,
            use_ssl=use_ssl,
            verify=verify_ssl
        )

    @classmethod
    def get_resource(cls,
                     resource_type,
                     endpoint_url=None,
                     access_key=None,
                     secret_key=None,
                     session_token=None,
                     region_name=None,
                     use_ssl=True,
                     verify_ssl=None):
        session = cls.get_session(
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
            region_name=region_name
        )

        endpoint_url = endpoint_url or config("AWS_ENDPOINT_URL", default=None)
        use_ssl = use_ssl or config("AWS_USE_SSL", default=True, cast=bool)

        if verify_ssl is None:
            verify_ssl = config("AWS_VERIFY_SSL", default=True, cast=bool)
        else:
            verify_ssl = verify_ssl

        return session.resource(
            resource_type,
            endpoint_url=endpoint_url,
            use_ssl=use_ssl,
            verify=verify_ssl
        )
