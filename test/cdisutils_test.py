import pytest

import cdisutils.net as net
import cdisutils.storage as storage

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

import os
import json
import tempfile
from contextlib import contextmanager
from shutil import rmtree


def test_no_proxy_cm():
    os.environ["http_proxy"] = "foobar"
    with net.no_proxy():
        assert not os.environ.get("http_proxy")
    assert os.environ["http_proxy"] == "foobar"


def test_no_proxy_dec():
    os.environ["http_proxy"] = "foobar"

    @net.no_proxy()
    def inner():
        assert not os.environ.get("http_proxy")
    inner()
    assert os.environ["http_proxy"] == "foobar"


SWIFT_JSON = json.dumps([{"path": "/test_segs/seg1",
                          "etag": "foobar", "size_bytes": 4},
                         {"path": "/test_segs/seg2",
                          "etag": "bazquux", "size_bytes": 4}])

@contextmanager
def local_s3():
    tmpdir = tempfile.mkdtemp()
    Local = get_driver(Provider.LOCAL)
    s3 = Local(tmpdir)
    test = s3.create_container("test")
    test.upload_object_via_stream(iterator=SWIFT_JSON,
                                  object_name="testobj")
    test.upload_object_via_stream(iterator="not json!",
                                  object_name="other")
    segs = s3.create_container("test_segs")
    segs.upload_object_via_stream(iterator="asdf",
                                  object_name="seg1")
    segs.upload_object_via_stream(iterator="ghjk",
                                  object_name="seg2")
    try:
        yield s3
    finally:
        rmtree(tmpdir)


def test_is_probably_swift_segments_returns_true_on_swift_json():
    with local_s3() as s3:
        assert storage.is_probably_swift_segments(s3.get_object("test",
                                                                "testobj"))


def test_is_probably_swift_segments_returns_false_on_other_data():
    with local_s3() as s3:
        assert not storage.is_probably_swift_segments(s3.get_object("test",
                                                                    "other"))


def test_swift_stream_works():
    with local_s3() as s3:
        result = "".join(storage.swift_stream(s3.get_object("test","testobj")))
        assert result == "asdfghjk"
