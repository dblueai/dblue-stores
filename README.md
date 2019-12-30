# Dblue Stores

Dblue Store is an abstraction and a collection of clients to interact with cloud storages.

## Install

```bash
$ pip install -U dblue-stores
```

N.B. this module does not include by default the cloud storage's client requirements
to keep the library lightweight, the user needs to install the appropriate module to use with `dblue-stores`.

### Install S3

```bash
pip install -U dblue-stores[s3]
```

### Install GCS

```bash
pip install -U dblue-stores[gcs]
```

### Install Azure Storage

```bash
pip install -U dblue-stores[azure]
```

### Install SFTP

```bash
pip install -U dblue-stores[sftp]
```

## Stores

This module includes clients and stores abstraction that can be used to interact with AWS S3, Azure Storage, Google Cloud Storage and SFTP.

## S3

### Normal instantiation

```python
from dblue_stores.stores.s3 import S3Store

s3_store = S3Store(
    endpoint_url=...,
    access_key=...,
    secret_key=...,
    session_token=...,
    region=...
)
```

### Using env vars

```bash
export AWS_ENDPOINT_URL=...
export AWS_ACCESS_KEY=...
export AWS_SECRET_KEY=...
export AWS_SECURITY_TOKEN=...
exprot AWS_REGION=...
```

And then you can instantiate the store

```python
from dblue_stores.stores.s3 import S3Store

s3_store = S3Store()
```

### Using a client

```python
from dblue_stores.stores.s3 import S3Store

s3_store = S3Store(client=client)
```

### Important methods

```python
s3_store.list(bucket_name, prefix='', delimiter='', page_size=None, max_items=None, keys=True, prefixes=True)
s3_store.list_prefixes(bucket_name, prefix='', delimiter='', page_size=None, max_items=None)
s3_store.list_keys(bucket_name, prefix='', delimiter='', page_size=None, max_items=None)
s3_store.check_key(key, bucket_name=None)
s3_store.get_key(key, bucket_name=None)
s3_store.read_key(key, bucket_name=None)
s3_store.upload_bytes(bytes_data, key, bucket_name=None, overwrite=False, encrypt=False, acl=None)
s3_store.upload_string(string_data, key, bucket_name=None, overwrite=False, encrypt=False, acl=None, encoding='utf-8')
s3_store.upload_file(filename, key, bucket_name=None, overwrite=False, encrypt=False, acl=None, use_basename=True)
s3_store.download_file(key, local_path, bucket_name=None, use_basename=True)
s3_store.upload_dir(dirname, key, bucket_name=None, overwrite=False, encrypt=False, acl=None, use_basename=True)
s3_store.download_dir(key, local_path, bucket_name=None, use_basename=True)
```

## GCS

### Normal instantiation

```python
from dblue_stores.stores.gcs import GCSStore

gcs_store = GCSStore(
    project_id=...,
    credentials=...,
    key_path=...,
    keyfile_dict=...,
    scopes=...
)
```

### Using a client

```python
from dblue_stores.stores.gcs import GCSStore

gcs_store = GCSStore(client=client)
```

### Important methods

```python
gcs_store.list(key, bucket_name=None, path=None, delimiter='/', blobs=True, prefixes=True)
gcs_store.upload_file(filename, blob, bucket_name=None, use_basename=True)
gcs_store.download_file(blob, local_path, bucket_name=None, use_basename=True)
gcs_store.upload_dir(dirname, blob, bucket_name=None, use_basename=True)
gcs_store.download_dir(blob, local_path, bucket_name=None, use_basename=True)
```

## Azure Storage

### Normal instantiation

```python
from dblue_stores.stores.azure import AzureStore

az_store = AzureStore(
    account_name=...,
    account_key=...,
    connection_string=...
)

```

### Using env vars

```bash
export AZURE_ACCOUNT_NAME=...
export AZURE_ACCOUNT_KEY=...
export AZURE_CONNECTION_STRING=...
```

And then you can instantiate the store

```python
from dblue_stores.stores.azure import AzureStore

az_store = AzureStore()
```

### Using a client

```python
from dblue_stores.stores.azure import AzureStore

az_store = AzureStore(client=client)
```

### Important methods

```python
az_store.list(key, container_name=None, path=None, delimiter='/', blobs=True, prefixes=True)
az_store.upload_file(filename, blob, container_name=None, use_basename=True)
az_store.download_file(blob, local_path, container_name=None, use_basename=True)
az_store.upload_dir(dirname, blob, container_name=None, use_basename=True)
az_store.download_dir(blob, local_path, container_name=None, use_basename=True)
```

## Running tests

```
pytest
```

## Publish

### Install twine

```bash
pip install twine
```

### Build distribution

```bash
python setup.py sdist
```

### Publish to pypi

```bash
twine upload dist/*
```

## Credits

Most of the code are borrowed from https://github.com/polyaxon/polystores
