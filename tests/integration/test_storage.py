from unittest import TestCase
from cdisutils.net import BotoManager, cancel_stale_multiparts, md5sum_with_size
from cdisutils.net import url_for_boto_key


from boto.s3.connection import S3Connection
import boto
from moto import mock_s3
from unittest.mock import patch, MagicMock, call


def connect_s3_mock(*args, **kwargs):
    kwargs["spec"] = S3Connection
    return MagicMock(*args, **kwargs)


class BotoUtilsTest(TestCase):

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
