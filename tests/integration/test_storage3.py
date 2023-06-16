import os
import time
import typing
import boto3
import pytest

from cdisutils.storage3 import Boto3Manager
from tests.integration.conftest import MotoServer


LARGE_NUMBER_TO_WRITE = 10000000
ORIGINAL_FILE_NAME = "original_file"
COPIED_FILE_NAME = "copied_file"
S3_URL = "s3://{}".format(os.getenv("CLEVERSAFE_ENDPOINT", "localhost:7000"))
TEST_BUCKET = "test_bucket"


def get_config() -> typing.Dict:
    return {
        "localhost:7000": {
            "aws_secret_access_key": "testing",
            "aws_access_key_id": "testing",
            "verify": False,
        },
    }


@pytest.fixture(scope="module")
def create_large_file(tmp_path_factory):
    large_file = tmp_path_factory.mktemp("data") / ORIGINAL_FILE_NAME
    with open(str(large_file), "w") as fp:
        for _ in range(LARGE_NUMBER_TO_WRITE):
            fp.write("test")

    yield large_file


@pytest.fixture
def create_large_object(create_large_file, moto_server):
    s3 = boto3.client(
        "s3",
        endpoint_url="https://localhost:7000",
        verify=False,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    s3.create_bucket(Bucket=TEST_BUCKET)
    s3.put_object(
        Body=open(str(create_large_file), "rb"),
        Bucket=TEST_BUCKET,
        Key=ORIGINAL_FILE_NAME,
    )
    original_object = s3.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert original_object["ContentLength"] == create_large_file.stat().st_size

    yield original_object

    s3.delete_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)


@pytest.mark.usefixtures("create_large_object")
def test_get_connection():
    config = get_config()
    manager = Boto3Manager(config)
    conn = manager.get_connection("localhost:7000")
    head = conn.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.usefixtures("create_large_object")
def test_parse_url():
    config = get_config()
    manager = Boto3Manager(config)
    s3_info = manager.parse_url(
        f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    )
    assert s3_info == {
        "url": "s3://localhost:7000/test_bucket/original_file",
        "s3_loc": "localhost:7000",
        "bucket_name": "test_bucket",
        "key_name": "original_file",
    }


@pytest.mark.usefixtures("create_large_object")
def test_get_url():
    config = get_config()
    manager = Boto3Manager(config)
    key = manager.get_url(f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}")
    assert key["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.usefixtures("create_large_object")
def test_list_buckets():
    config = get_config()
    manager = Boto3Manager(config)
    bucket_list = manager.list_buckets("localhost:7000")
    assert len(bucket_list) == 1
    assert bucket_list[0]["Name"] == TEST_BUCKET


def test_simulate_cleversafe_to_aws_multipart_copy(
    create_large_file, moto_server_factory
):
    """
    The multipart upload is used to support transferring large files from one S3 provider to another.
    """
    # Start the servers
    host_a = "localhost"
    port_a = 7001
    url_a = f"{host_a}:{port_a}"
    moto_server_a = moto_server_factory(hostname=host_a, port=port_a)
    assert url_a == moto_server_a.url

    host_b = "localhost"
    port_b = 7002
    url_b = f"{host_b}:{port_b}"
    moto_server_b = moto_server_factory(hostname=host_b, port=port_b)
    assert url_b == moto_server_b.url

    # Setup the BotoManager
    s3_configs = {
        f"{moto_server_a.url}": {
            "aws_secret_access_key": "testing",
            "aws_access_key_id": "testing",
            "verify": False,
        },
        f"{moto_server_b.url}": {
            "aws_secret_access_key": "testing",
            "aws_access_key_id": "testing",
            "verify": False,
        },
    }
    manager = Boto3Manager(s3_configs)
    conn_a = manager.get_connection(f"{moto_server_a.url}")
    conn_b = manager.get_connection(f"{moto_server_b.url}")

    # load in the initial data
    conn_a.create_bucket(Bucket=TEST_BUCKET)
    conn_a.put_object(
        Body=open(str(create_large_file), "rb"),
        Bucket=TEST_BUCKET,
        Key=ORIGINAL_FILE_NAME,
    )
    head = conn_a.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert head["ContentLength"] == LARGE_NUMBER_TO_WRITE * len("test")

    # create the destination bucket
    conn_b.create_bucket(Bucket=TEST_BUCKET)

    # Copy files from one host to the other using multipart
    src_url = f"s3://{host_a}:{port_a}/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    dst_url = f"s3://{host_b}:{port_b}/{TEST_BUCKET}/{COPIED_FILE_NAME}"

    src_info = manager.parse_url(src_url)
    dst_info = manager.parse_url(dst_url)

    # copy_multipart_file invokes the other multipart operation: create_multipart_upload, complete_multipart_upload
    res = manager.copy_multipart_file(src_info=src_info, dst_info=dst_info)
    assert res == {
        "md5_sum": "bc0354f0646794a755a4276435ec5a6c",
        "sha256_sum": "c97d1f1ab2ae91dbe05ad8e20bc58fc6f3af28e98d98ca8dbeee31a9d32e1e5b",
        "bytes_transferred": 40000000,
    }

    head = conn_b.head_object(Bucket=TEST_BUCKET, Key=COPIED_FILE_NAME)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert head["ContentLength"] == LARGE_NUMBER_TO_WRITE * len("test")

    conn_b.delete_object(Bucket=TEST_BUCKET, Key=COPIED_FILE_NAME)


@pytest.mark.usefixtures("create_large_object")
def test_load_file():
    config = get_config()
    manager = Boto3Manager(config)
    url = f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    file_content = manager.load_file(url=url)
    assert file_content == "test" * LARGE_NUMBER_TO_WRITE


@pytest.mark.usefixtures("create_large_object")
def test_checksum_s3_key():
    config = get_config()
    manager = Boto3Manager(config)
    url = f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    res = manager.checksum_s3_key(url=url)

    assert res["md5_sum"] == "bc0354f0646794a755a4276435ec5a6c"
    assert (
        res["sha256_sum"]
        == "c97d1f1ab2ae91dbe05ad8e20bc58fc6f3af28e98d98ca8dbeee31a9d32e1e5b"
    )
