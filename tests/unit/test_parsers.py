import pytest

from cdisutils import parsers


@pytest.mark.parametrize(
    "url, scheme, netloc, bucket, key",
    (
        (
            "s3://ceph.service.consul/bucket/key/name",
            "s3",
            "ceph.service.consul",
            "bucket",
            "key/name",
        ),
        (
            "s3://s3.amazonaws.com/bucket/key/name",
            "s3",
            "s3.amazonaws.com",
            "bucket",
            "key/name",
        ),
        (
            "s3://s3.something-here.amazonaws.com/bucket/key/name",
            "s3",
            "s3.something-here.amazonaws.com",
            "bucket",
            "key/name",
        ),
        (
            "s3://bucket.s3.amazonaws.com/key/name",
            "s3",
            "s3.amazonaws.com",
            "bucket",
            "key/name",
        ),
        (
            "s3://bucket.s3.something-here.amazonaws.com/key/name",
            "s3",
            "s3.something-here.amazonaws.com",
            "bucket",
            "key/name",
        ),
        (
            "http://bucket.s3.amazonaws.com/key/name",
            "http",
            "s3.amazonaws.com",
            "bucket",
            "key/name",
        ),
        (
            "https://bucket.s3.amazonaws.com/key/name",
            "https",
            "s3.amazonaws.com",
            "bucket",
            "key/name",
        ),
    ),
)
def test_s3_url_parser__correctly_parse(url, scheme, netloc, bucket, key):
    parsed_url = parsers.S3URLParser(url)
    assert parsed_url.scheme == scheme
    assert parsed_url.netloc == netloc
    assert parsed_url.bucket == bucket
    assert parsed_url.key == key


def test_s3_old_url_parser():
    url = "s3://ceph.service.consul/bucket/key/name/goes/here"
    parse_object = parsers.S3URLParser(url)
    assert parse_object.get_url() == url

    parse_object.netloc = "cleversafe.service.consul"
    assert (
        parse_object.get_url()
        == "s3://cleversafe.service.consul/bucket/key/name/goes/here"
    )

    parse_object.bucket = "new-bucket"
    assert (
        parse_object.get_url()
        == "s3://cleversafe.service.consul/new-bucket/key/name/goes/here"
    )


def test_s3_new_url_parser():
    url = "https://bucket.s3.amazonaws.com/key/name/goes/here"
    parse_object = parsers.S3URLParser(url)
    assert parse_object.get_url(new_style=True) == url

    parse_object.netloc = "fake.aws.com"
    assert (
        parse_object.get_url(new_style=True)
        == "https://bucket.fake.aws.com/key/name/goes/here"
    )

    parse_object.bucket = "new-bucket"
    assert (
        parse_object.get_url(new_style=True)
        == "https://new-bucket.fake.aws.com/key/name/goes/here"
    )
