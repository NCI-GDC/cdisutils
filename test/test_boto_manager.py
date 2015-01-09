from unittest import TestCase
from cdisutils.net import BotoManager


from boto.s3.connection import S3Connection
from mock import patch, MagicMock, call


def connect_s3_mock(*args, **kwargs):
    kwargs["spec"] = S3Connection
    return MagicMock(*args, **kwargs)


class BotoManagerTest(TestCase):

    @patch("cdisutils.net.connect_s3", connect_s3_mock)
    def test_basic_connect(self):
        """These tests are admitedly rudimentary. I wanted to use moto but
        moto works by intercepting calls to s3.amazonaws.com, so it
        won't work for testing mutiple hosts like this -___-
        """
        config = {
            "s3.amazonaws.com": {
                "secret_access_key": "aws_key",
                "access_key_id": "secret_key",
                "is_secure": True,
            },
            "s3.myinstallation.org": {
                "secret_access_key": "my_key",
                "access_key_id": "my_secret_key",
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

    @patch("cdisutils.net.connect_s3", connect_s3_mock)
    def test_get_url(self):
        config = {
            "s3.amazonaws.com": {
                "secret_access_key": "aws_key",
                "access_key_id": "secret_key",
                "is_secure": True,
            },
            "s3.myinstallation.org": {
                "secret_access_key": "my_key",
                "access_key_id": "my_secret_key",
                "is_secure": False,
            },
        }
        manager = BotoManager(config)
        manager.get_url("s3://s3.amazonaws.com/bucket/dir/key")
        mock = manager["s3.amazonaws.com"]
        self.assertIn(call.get_bucket("bucket"), mock.mock_calls)
        self.assertIn(call.get_bucket().get_key("dir/key"), mock.mock_calls)
        with self.assertRaises(KeyError):
            manager.get_url("s3://fake_host/bucket/dir/key")
