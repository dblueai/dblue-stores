
import os
import tempfile

from unittest import TestCase

import mock

from dblue_stores.exceptions import DblueStoresException
from dblue_stores.stores.gcs import GCSStore

GCS_MODULE = 'dblue_stores.clients.gcp.{}'


class TestGCSStore(TestCase):
    def test_parse_gcs_url(self):
        # Correct url
        gcs_url = 'gs://bucket/path/to/blob'
        parsed_url = GCSStore.parse_gcs_url(gcs_url)
        assert parsed_url == ('bucket', 'path/to/blob')

        # Wrong url
        gcs_url = 'gs:/bucket/path/to/blob'
        with self.assertRaises(DblueStoresException):
            GCSStore.parse_gcs_url(gcs_url)

        # Trailing slash
        gcs_url = 'gs://bucket/path/to/blob/'
        assert GCSStore.parse_gcs_url(gcs_url) == ('bucket', 'path/to/blob/')

        # Bucket only
        gcs_url = 'gs://bucket/'
        assert GCSStore.parse_gcs_url(gcs_url) == ('bucket', '')

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_store_client(self, client, gc_credentials):
        client.return_value = 'client'
        store = GCSStore(client='foo')
        assert store.client == 'foo'

        store = GCSStore()
        assert store.client == 'client'
        assert gc_credentials.call_count == 1
        assert client.call_count == 1

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_existing_object(self, client, _):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        client.return_value.objects.return_value.get.return_value.execute.return_value = {
            "kind": "storage#object",
            # the bucket name, object name, and generation number.
            "id": "{}/{}/1521132662504504".format(test_bucket, test_object),
            "name": test_object,
            "bucket": test_bucket,
            "generation": "1521132662504504",
            "contentType": "text/csv",
            "timeCreated": "2018-03-15T16:51:02.502Z",
            "updated": "2018-03-15T16:51:02.502Z",
            "storageClass": "MULTI_REGIONAL",
            "timeStorageClassUpdated": "2018-03-15T16:51:02.502Z",
            "size": "89",
            "md5Hash": "leYUJBUWrRtks1UeUFONJQ==",
            "metadata": {
                "md5-hash": "95e614241516ad1b64b3551e50538d25"
            },
            "crc32c": "xgdNfQ==",
            "etag": "CLf4hODk7tkCEAE="
        }

        response = GCSStore().check_blob(blob=test_object, bucket_name=test_bucket)
        assert response is True

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_non_existing_object(self, client, _):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        client.return_value.get_bucket.return_value.get_blob.side_effect = Exception

        response = GCSStore().check_blob(blob=test_object, bucket_name=test_bucket)
        assert response is False

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_get_bucket(self, client, _):
        test_bucket = 'test_bucket'

        client.return_value.get_bucket.return_value = {}

        response = GCSStore().get_bucket(bucket_name=test_bucket)

        assert response == {}

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_get_blob(self, client, _):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        bucket = mock.MagicMock()
        client.return_value.get_bucket.return_value = bucket
        bucket.get_blob.return_value = {}

        response = GCSStore().get_blob(blob=test_object, bucket_name=test_bucket)

        assert response == {}

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_delete(self, client, _):
        test_bucket = 'test_bucket'
        test_object = 'test_object'

        # Correct file
        bucket = mock.MagicMock()
        client.return_value.get_bucket.return_value = bucket
        bucket.delete_blob.return_value = True

        GCSStore().delete(key=test_object, bucket_name=test_bucket)

        # Wrong file
        bucket = mock.MagicMock()
        client.return_value.get_bucket.return_value = bucket
        bucket.delete_blob.return_value = True

        GCSStore().delete(key=test_object, bucket_name=test_bucket)

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_list_empty(self, client, _):
        gcs_url = 'gs://bucket/path/to/blob'
        store = GCSStore()
        client.return_value.get_bucket.return_value.list_blobs.return_value = mock.MagicMock()
        assert store.list(gcs_url) == {'blobs': [], 'prefixes': []}

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_list_dirs_and_blobs(self, client, _):
        blob_root_path = 'project_path/experiment_id/'
        gcs_url = 'gs://bucket/' + blob_root_path

        obj_mock = mock.Mock()
        file_path = 'fileA'
        obj_mock.configure_mock(name=blob_root_path + file_path, size=1)

        dir_mock = mock.Mock()
        dirname = 'model1'
        dir_mock.configure_mock(prefixes=(blob_root_path + dirname + '/',))

        mock_results = mock.MagicMock()
        mock_results.configure_mock(pages=[dir_mock])
        mock_results.__iter__.return_value = [obj_mock]

        client.return_value.get_bucket.return_value.list_blobs.return_value = mock_results

        store = GCSStore()
        results = store.list(gcs_url)

        blobs = results['blobs']
        prefixes = results['prefixes']
        assert len(blobs) == 1
        assert len(prefixes) == 1
        assert blobs[0][0] == file_path
        assert blobs[0][1] == obj_mock.size
        assert prefixes[0] == dirname

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_list_with_subdir(self, client, _):
        blob_root_path = 'project_path/experiment_id/'
        gcs_url = 'gs://bucket/' + blob_root_path
        dirname = 'model'
        obj_mock = mock.Mock()
        file_path = dirname + '/' + 'tf.pb'
        obj_mock.configure_mock(name=blob_root_path + file_path, size=1)

        subdir_mock = mock.Mock()
        subdirname = dirname + '/' + 'files'
        subdir_mock.configure_mock(prefixes=(blob_root_path + subdirname + '/',))

        mock_results = mock.MagicMock()
        mock_results.configure_mock(pages=[subdir_mock])
        mock_results.__iter__.return_value = [obj_mock]

        client.return_value.get_bucket.return_value.list_blobs.return_value = mock_results

        store = GCSStore()
        results = store.list(gcs_url, path=dirname)

        blobs = results['blobs']
        prefixes = results['prefixes']
        assert len(blobs) == 1
        assert len(prefixes) == 1
        assert blobs[0][0] == file_path
        assert blobs[0][1] == obj_mock.size
        assert prefixes[0] == subdirname

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_upload(self, client, _):
        dirname = tempfile.mkdtemp()
        fpath = dirname + '/test.txt'
        open(fpath, 'w')

        (client.return_value
         .get_bucket.return_value
         .blob.return_value
         .upload_from_filename.side_effect) = os.path.isfile

        store = GCSStore()

        # Test without basename
        gcs_url = 'gs://bucket/path/to/blob.txt'
        store.upload_file(filename=fpath, blob=gcs_url, use_basename=False)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket.return_value.blob.assert_called_with('path/to/blob.txt')
        (client.return_value
         .get_bucket.return_value
         .blob.return_value
         .upload_from_filename.assert_called_with(fpath))

        # Test with basename
        gcs_url = 'gs://bucket/path/to/'
        store.upload_file(filename=fpath, blob=gcs_url, use_basename=True)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket.return_value.blob.assert_called_with('path/to/test.txt')
        (client.return_value
         .get_bucket.return_value
         .blob.return_value
         .upload_from_filename.assert_called_with(fpath))

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_download(self, client, _):
        dirname = tempfile.mkdtemp()
        fpath = dirname + '/test.txt'

        def mkfile(fname):
            return open(fname, 'w')

        (client.return_value
         .get_bucket.return_value
         .get_blob.return_value
         .download_to_filename.side_effect) = mkfile

        store = GCSStore()

        # Test without basename
        gcs_url = 'gs://bucket/path/to/blob.txt'
        store.download_file(gcs_url, fpath, use_basename=False)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket().get_blob.assert_called_with('path/to/blob.txt')
        client.return_value.get_bucket().get_blob().download_to_filename.assert_called_with(fpath)

        # Test with basename
        gcs_url = 'gs://bucket/path/to/blob.txt'
        store.download_file(gcs_url, dirname, use_basename=True)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket().get_blob.assert_called_with('path/to/blob.txt')
        client.return_value.get_bucket().get_blob().download_to_filename.assert_called_with(
            dirname + '/blob.txt'
        )

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_upload_dir(self, client, _):
        dirname1 = tempfile.mkdtemp()
        fpath1 = dirname1 + '/test1.txt'
        with open(fpath1, 'w') as f:
            f.write('data1')

        fpath2 = dirname1 + '/test2.txt'
        with open(fpath2, 'w') as f:
            f.write('data2')

        dirname2 = tempfile.mkdtemp(prefix=dirname1 + '/')
        fpath3 = dirname2 + '/test3.txt'
        with open(fpath3, 'w') as f:
            f.write('data3')

        (client.return_value
         .get_bucket.return_value
         .blob.return_value
         .upload_from_filename.side_effect) = os.path.isfile

        store = GCSStore()

        blob_path = 'path/to/'
        gcs_url = 'gs://bucket/' + blob_path
        rel_path1 = dirname1.split('/')[-1]
        rel_path2 = dirname2.split('/')[-1]

        # Test without basename
        store.upload_dir(dirname=dirname1, blob=gcs_url, use_basename=False)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket.return_value.blob.assert_has_calls(
            [
                mock.call('{}test1.txt'.format(blob_path)),
                mock.call('{}test2.txt'.format(blob_path)),
                mock.call('{}{}/test3.txt'.format(blob_path, rel_path2)),
            ], any_order=True)
        (client.return_value
         .get_bucket.return_value
         .blob.return_value
         .upload_from_filename.assert_has_calls([mock.call(fpath1),
                                                 mock.call(fpath2),
                                                 mock.call(fpath3),
                                                 ], any_order=True))

        # Test with basename
        store.upload_dir(dirname=dirname1, blob=gcs_url, use_basename=True)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket.return_value.blob.assert_has_calls(
            [
                mock.call('{}{}/test1.txt'.format(blob_path, rel_path1)),
                mock.call('{}{}/test2.txt'.format(blob_path, rel_path1)),
                mock.call('{}{}/{}/test3.txt'.format(blob_path, rel_path1, rel_path2)),
            ], any_order=True)
        (client.return_value
         .get_bucket.return_value
         .blob.return_value
         .upload_from_filename.assert_has_calls([mock.call(fpath1),
                                                 mock.call(fpath2),
                                                 mock.call(fpath3),
                                                 ], any_order=True))

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_download_dir(self, client, _):
        dirname1 = tempfile.mkdtemp()
        dirname2 = tempfile.mkdtemp(prefix=dirname1 + '/')

        def mkfile(fname):
            return open(fname, 'w')

        (client.return_value
         .get_bucket.return_value
         .get_blob.return_value
         .download_to_filename.side_effect) = mkfile

        store = GCSStore()

        blob_path = 'path/to/'
        gcs_url = 'gs://bucket/' + blob_path
        rel_path2 = dirname2.split('/')[-1]

        # Mock return list
        obj_mock1 = mock.Mock()
        obj_mock1.configure_mock(name=blob_path + 'test1.txt', size=1)

        obj_mock2 = mock.Mock()
        obj_mock2.configure_mock(name=blob_path + 'test2.txt', size=1)

        subdir_mock = mock.Mock()
        subdirname = rel_path2 + '/'
        subdir_mock.configure_mock(prefixes=(blob_path + subdirname,))

        obj_mock3 = mock.Mock()
        obj_mock3.configure_mock(name=blob_path + subdirname + 'test3.txt', size=1)

        mock_results1 = mock.MagicMock()
        mock_results1.configure_mock(pages=[subdir_mock])
        mock_results1.__iter__.return_value = [obj_mock1, obj_mock2]

        mock_results2 = mock.MagicMock()
        mock_results2.configure_mock(pages=[])
        mock_results2.__iter__.return_value = [obj_mock3]

        def list_side_effect(prefix, delimiter='/'):
            if prefix == blob_path:
                return mock_results1
            return mock_results2

        client.return_value.get_bucket.return_value.list_blobs.side_effect = list_side_effect

        dirname3 = tempfile.mkdtemp()

        # Test without basename
        store.download_dir(blob=gcs_url, local_path=dirname3, use_basename=False)
        client.return_value.get_bucket().get_blob.assert_has_calls(
            [
                mock.call('{}test1.txt'.format(blob_path)),
                mock.call('{}test2.txt'.format(blob_path)),
                mock.call('{}{}/test3.txt'.format(blob_path, rel_path2)),
            ], any_order=True)

        (client.return_value
         .get_bucket.return_value
         .get_blob.return_value
         .download_to_filename
         .assert_has_calls([mock.call('{}/test1.txt'.format(dirname3)),
                            mock.call('{}/test2.txt'.format(dirname3)),
                            mock.call('{}/{}/test3.txt'.format(dirname3, rel_path2))],
                           any_order=True))

    @mock.patch(GCS_MODULE.format('GCPClient.get_credentials'))
    @mock.patch(GCS_MODULE.format('Client'))
    def test_download_dir_with_basename(self, client, _):
        dirname1 = tempfile.mkdtemp()
        dirname2 = tempfile.mkdtemp(prefix=dirname1 + '/')

        def mkfile(fname):
            return open(fname, 'w')

        (client.return_value
         .get_bucket.return_value
         .get_blob.return_value
         .download_to_filename.side_effect) = mkfile

        store = GCSStore()

        blob_path = 'path/to/'
        gcs_url = 'gs://bucket/' + blob_path
        rel_path2 = dirname2.split('/')[-1]

        # Mock return list
        obj_mock1 = mock.Mock()
        obj_mock1.configure_mock(name=blob_path + 'foo/test1.txt', size=1)

        obj_mock2 = mock.Mock()
        obj_mock2.configure_mock(name=blob_path + 'foo/test2.txt', size=1)

        subdir_mock = mock.Mock()
        subdirname = rel_path2 + '/'
        subdir_mock.configure_mock(prefixes=(blob_path + 'foo/' + subdirname,))

        obj_mock3 = mock.Mock()
        obj_mock3.configure_mock(name=blob_path + 'foo/' + subdirname + 'test3.txt', size=1)

        mock_results1 = mock.MagicMock()
        mock_results1.configure_mock(pages=[subdir_mock])
        mock_results1.__iter__.return_value = [obj_mock1, obj_mock2]

        mock_results2 = mock.MagicMock()
        mock_results2.configure_mock(pages=[])
        mock_results2.__iter__.return_value = [obj_mock3]

        def list_side_effect(prefix, delimiter='/'):
            if prefix == blob_path + 'foo/':
                return mock_results1
            return mock_results2

        client.return_value.get_bucket.return_value.list_blobs.side_effect = list_side_effect

        dirname3 = tempfile.mkdtemp()

        # Test with basename
        store.download_dir(blob=gcs_url + 'foo', local_path=dirname3, use_basename=True)
        client.return_value.get_bucket.assert_called_with('bucket')
        client.return_value.get_bucket().get_blob.assert_has_calls(
            [mock.call('{}foo/test1.txt'.format(blob_path)),
             mock.call('{}foo/test2.txt'.format(blob_path)),
             mock.call('{}foo/{}/test3.txt'.format(blob_path, rel_path2)), ], any_order=True)

        (client.return_value
         .get_bucket.return_value
         .get_blob.return_value
         .download_to_filename
         .assert_has_calls([mock.call('{}/foo/test1.txt'.format(dirname3)),
                            mock.call('{}/foo/test2.txt'.format(dirname3)),
                            mock.call('{}/foo/{}/test3.txt'.format(dirname3, rel_path2))],
                           any_order=True))
