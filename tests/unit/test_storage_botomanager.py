from unittest.mock import MagicMock

import pytest
from boto.s3.connection import S3Connection

import cdisutils
from cdisutils.storage import BotoManager


def get_config():
    return {
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


def test_basic_connect():
    """These tests are admitedly rudimentary. I wanted to use moto but
    moto works by intercepting calls to s3.amazonaws.com, so it
    won't work for testing mutiple hosts like this -___-
    """
    config = get_config()
    manager = BotoManager(config)
    aws_conn_mock = manager["s3.amazonaws.com"]
    assert aws_conn_mock.host == 's3.amazonaws.com'
    assert aws_conn_mock.is_secure is True
    site_conn_mock = manager["s3.myinstallation.org"]
    assert site_conn_mock.host == 's3.myinstallation.org'
    assert site_conn_mock.is_secure is False


def connect_s3_mock(*args, **kwargs):
    kwargs["spec"] = S3Connection
    return MagicMock(*args, **kwargs)


def test_get_url(monkeypatch):
    monkeypatch.setattr(cdisutils.storage, 'S3ConnectionProxyFix', MagicMock())
    config = get_config()
    manager = BotoManager(config)
    manager.get_url("s3://s3.amazonaws.com/bucket/dir/key")
    mock = manager['s3.amazonaws.com']
    mock.get_bucket.assert_called_once_with("bucket", headers=None, validate=True)
    mock.get_bucket().get_key.assert_called_once_with("dir/key",headers=None, validate=True)
    with pytest.raises(KeyError):
        manager.get_url("s3://fake_host/bucket/dir/key")