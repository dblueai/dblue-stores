# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import boto3
from decouple import config


def get_aws_access_key_id(key=None):
    return config(key or 'AWS_ACCESS_KEY_ID')


def get_aws_secret_access_key(key=None):
    return config(key or 'AWS_SECRET_ACCESS_KEY')


def get_aws_security_token(key=None):
    return config(key or 'AWS_SECURITY_TOKEN')


def get_region(key=None):
    return config(key or 'AWS_REGION')


def get_endpoint_url(key=None):
    return config(key or 'AWS_ENDPOINT_URL')


def get_aws_use_ssl(key=None):
    return config(key or 'AWS_USE_SSL', cast=bool)


def get_aws_verify_ssl(key=None):
    return config(key or 'AWS_VERIFY_SSL', cast=bool)


def get_aws_legacy_api(key=None):
    return config(key or 'AWS_LEGACY_API', cast=bool)


def get_legacy_api(legacy_api=False):
    legacy_api = legacy_api or get_aws_legacy_api()
    return legacy_api


def get_aws_session(aws_access_key_id=None,
                    aws_secret_access_key=None,
                    aws_session_token=None,
                    region_name=None):
    aws_access_key_id = aws_access_key_id or get_aws_access_key_id()
    aws_secret_access_key = aws_secret_access_key or get_aws_secret_access_key()
    aws_session_token = aws_session_token or get_aws_security_token()
    region_name = region_name or get_region()
    return boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name)


def get_aws_client(client_type,
                   endpoint_url=None,
                   aws_access_key_id=None,
                   aws_secret_access_key=None,
                   aws_session_token=None,
                   region_name=None,
                   aws_use_ssl=True,
                   aws_verify_ssl=None):
    session = get_aws_session(aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              aws_session_token=aws_session_token,
                              region_name=region_name)
    endpoint_url = endpoint_url or get_endpoint_url()
    aws_use_ssl = aws_use_ssl or get_aws_use_ssl()
    if aws_verify_ssl is None:
        aws_verify_ssl = get_aws_verify_ssl()
    else:
        aws_verify_ssl = aws_verify_ssl
    return session.client(
        client_type,
        endpoint_url=endpoint_url,
        use_ssl=aws_use_ssl,
        verify=aws_verify_ssl)


def get_aws_resource(resource_type,
                     endpoint_url=None,
                     aws_access_key_id=None,
                     aws_secret_access_key=None,
                     aws_session_token=None,
                     region_name=None,
                     aws_use_ssl=True,
                     aws_verify_ssl=None):
    session = get_aws_session(aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              aws_session_token=aws_session_token,
                              region_name=region_name)
    endpoint_url = endpoint_url or get_endpoint_url()
    aws_use_ssl = aws_use_ssl or get_aws_use_ssl()
    if aws_verify_ssl is None:
        aws_verify_ssl = get_aws_verify_ssl()
    else:
        aws_verify_ssl = aws_verify_ssl
    return session.resource(
        resource_type,
        endpoint_url=endpoint_url,
        use_ssl=aws_use_ssl,
        verify=aws_verify_ssl)
