from cdisutils.storage import cancel_stale_multiparts, md5sum_with_size, url_for_boto_key

import boto


def test_url_for_boto_key(moto_server_no_ssl):
    conn = boto.connect_s3(host='localhost', port=7000, calling_format='boto.s3.connection.OrdinaryCallingFormat', is_secure=False)
    buck = conn.create_bucket("foo")
    key = buck.new_key("bar")
    assert url_for_boto_key(key) == "s3://localhost/foo/bar"


def test_cancel_stale_multiparts(moto_server_no_ssl):
    conn = boto.connect_s3(host='localhost', port=7000, calling_format='boto.s3.connection.OrdinaryCallingFormat', is_secure=False)
    bucket = conn.create_bucket("test")
    upload = bucket.initiate_multipart_upload("test_key")
    cancel_stale_multiparts(bucket)
    # moto hard coded multipart initiate date to 2010-11-10T20:48:33.000Z,
    # so newly created uploads actually are created > 7 days.
    assert len(bucket.get_all_multipart_uploads()) == 0


def test_cancel_stale_multiparts_does_not_cancel_active_uploads(moto_server_no_ssl):
    conn = boto.connect_s3(host='localhost', port=7000, calling_format='boto.s3.connection.OrdinaryCallingFormat', is_secure=False)
    bucket = conn.create_bucket("test")
    upload = bucket.initiate_multipart_upload("test_key")
    # moto hard coded multipart initiate date to 2010-11-10T20:48:33.000Z
    cancel_stale_multiparts(bucket, stale_days=100000000)
    assert len(bucket.get_all_multipart_uploads()) == 1


def test_md5sum_with_size(moto_server_no_ssl):
    conn = boto.connect_s3(host='localhost', port=7000, calling_format='boto.s3.connection.OrdinaryCallingFormat', is_secure=False)
    bucket = conn.create_bucket("test")
    key = bucket.new_key("test_key")
    key.set_contents_from_string("test")
    (md5, size) = md5sum_with_size(key)
    assert size == key.size
    assert md5.encode('ascii') == key.md5
