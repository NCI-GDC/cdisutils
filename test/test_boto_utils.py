from unittest import TestCase
from cdisutils.net import BotoManager, cancel_stale_multiparts, md5sum_with_size
from cdisutils.net import url_for_boto_key


from boto.s3.connection import S3Connection
import boto
from moto import mock_s3
from mock import patch, MagicMock, call


def connect_s3_mock(*args, **kwargs):
    kwargs["spec"] = S3Connection
    return MagicMock(*args, **kwargs)


class BotoUtilsTest(TestCase):

    @patch("cdisutils.storage.S3ConnectionProxyFix", connect_s3_mock)
    def test_basic_connect(self):
        """These tests are admitedly rudimentary. I wanted to use moto but
        moto works by intercepting calls to s3.amazonaws.com, so it
        won't work for testing mutiple hosts like this -___-
        """
        config = {
            "s3.amazonaws.com": {
                "aws_secret_access_key": "aws_key",
                "aws_access_key_id": "secret_key",
                "is_secure": True,
            },
            "s3.myinstallation.org": {
                "aws_secret_access_key": "my_key",
                "aws_access_key_id": "my_secret_key",
                "is_secure": False,
            },
        }
        manager = BotoManager(config)
        aws_conn_mock = manager["s3.amazonaws.com"]
        self.assertEqual(aws_conn_mock.host, "s3.amazonaws.com")
        self.assertEqual(aws_conn_mock.is_secure, True)
        site_conn_mock = manager["s3.myinstallation.org"]
        self.assertEqual(site_conn_mock.host, "s3.myinstallation.org")
        self.assertEqual(site_conn_mock.is_secure, False)

    @patch("cdisutils.storage.S3ConnectionProxyFix", connect_s3_mock)
    def test_get_url(self):
        config = {
            "s3.amazonaws.com": {
                "aws_secret_access_key": "aws_key",
                "aws_access_key_id": "secret_key",
                "is_secure": True,
            },
            "s3.myinstallation.org": {
                "aws_secret_access_key": "my_key",
                "aws_access_key_id": "my_secret_key",
                "is_secure": False,
            },
        }
        manager = BotoManager(config)
        manager.get_url("s3://s3.amazonaws.com/bucket/dir/key")
        mock = manager["s3.amazonaws.com"]
        self.assertIn(call.get_bucket("bucket", headers=None, validate=True), mock.mock_calls)
        self.assertIn(call.get_bucket().get_key("dir/key",headers=None, validate=True), mock.mock_calls)
        with self.assertRaises(KeyError):
            manager.get_url("s3://fake_host/bucket/dir/key")

    def test_url_for_boto_key(self):
        with mock_s3():
            conn = boto.connect_s3()
            buck = conn.create_bucket("foo")
            key = buck.new_key("bar")
            self.assertEqual(url_for_boto_key(key), "s3://s3.amazonaws.com/foo/bar")

    def test_cancel_stale_multiparts(self):
        with mock_s3():
            conn = boto.connect_s3()
            bucket = conn.create_bucket("test")
            upload = bucket.initiate_multipart_upload("test_key")
            cancel_stale_multiparts(bucket)
            # moto hard coded multipart initiate date to 2010-11-10T20:48:33.000Z,
            # so newly created uploads actually are created > 7 days.
            assert len(bucket.get_all_multipart_uploads()) == 0


    def test_cancel_stale_multiparts_does_not_cancel_active_uploads(self):
        with mock_s3():
            conn = boto.connect_s3()
            bucket = conn.create_bucket("test")
            upload = bucket.initiate_multipart_upload("test_key")
            # moto hard coded multipart initiate date to 2010-11-10T20:48:33.000Z
            cancel_stale_multiparts(bucket, stale_days=100000000)
            assert len(bucket.get_all_multipart_uploads()) == 1

    def test_md5sum_with_size(self):
        with mock_s3():
            conn = boto.connect_s3()
            bucket = conn.create_bucket("test")
            key = bucket.new_key("test_key")
            key.set_contents_from_string("test")
            (md5, size) = md5sum_with_size(key)
            assert size == key.size
            assert md5.encode('ascii') == key.md5
