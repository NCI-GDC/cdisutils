import pytest

from cdisutils.storage import (
    cancel_stale_multiparts,
    md5sum_with_size,
    url_for_boto_key,
)

import boto


@pytest.fixture
def get_bucket(moto_server_no_ssl):
    conn = boto.connect_s3(
        host="localhost",
        port=7000,
        calling_format="boto.s3.connection.OrdinaryCallingFormat",
        is_secure=False,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    buck = conn.create_bucket("foo")
    return buck


def test_url_for_boto_key(get_bucket):
    key = get_bucket.new_key("bar")
    assert url_for_boto_key(key) == "s3://localhost/foo/bar"


def test_cancel_stale_multiparts(get_bucket):
    upload = get_bucket.initiate_multipart_upload("test_key")
    cancel_stale_multiparts(get_bucket)
    # moto hard coded multipart initiate date to 2010-11-10T20:48:33.000Z,
    # so newly created uploads actually are created > 7 days.
    assert len(get_bucket.get_all_multipart_uploads()) == 0


def test_cancel_stale_multiparts_does_not_cancel_active_uploads(get_bucket):
    upload = get_bucket.initiate_multipart_upload("test_key")
    # moto hard coded multipart initiate date to 2010-11-10T20:48:33.000Z
    cancel_stale_multiparts(get_bucket, stale_days=100000000)
    assert len(get_bucket.get_all_multipart_uploads()) == 1


def test_md5sum_with_size(get_bucket):
    key = get_bucket.new_key("test_key")
    key.set_contents_from_string("test")
    (md5, size) = md5sum_with_size(key)
    assert size == key.size
    assert md5.encode("ascii") == key.md5
