import os

import boto3
import pytest

from cdisutils.storage3 import Boto3Manager


LARGE_NUMBER_TO_WRITE = 10000000
ORIGINAL_FILE_NAME = "original_file"
COPIED_FILE_NAME = "copied_file"
S3_URL = "s3://{}".format(os.getenv("CLEVERSAFE_ENDPOINT", "localhost:7000"))
TEST_BUCKET = "test_bucket"


def get_config():
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


@pytest.mark.usefixtures("create_large_object")
def test_create_multipart_upload():
    config = get_config()
    manager = Boto3Manager(config)
    src_url = f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    dst_url = f"s3://localhost:7000/{TEST_BUCKET}/{COPIED_FILE_NAME}"
    mp_info = manager.create_multipart_upload(src_url=src_url, dst_url=dst_url)

    assert "md5_sum" in mp_info
    mp_info.pop("md5_sum")
    assert "sha256_sum" in mp_info
    mp_info.pop("sha256_sum")
    assert "mp_id" in mp_info
    mp_info.pop("mp_id")
    assert "stream_buffer" in mp_info
    mp_info.pop("stream_buffer")
    assert "start_time" in mp_info
    mp_info.pop("start_time")

    assert mp_info == {
        "dst_info": {
            "url": "s3://localhost:7000/test_bucket/copied_file",
            "s3_loc": "localhost:7000",
            "bucket_name": "test_bucket",
            "key_name": "copied_file",
        },
        "src_info": {
            "url": "s3://localhost:7000/test_bucket/original_file",
            "s3_loc": "localhost:7000",
            "bucket_name": "test_bucket",
            "key_name": "original_file",
        },
        "mp_chunk_size": 1073741824,
        "download_chunk_size": 16777216,
        "cur_size": 0,
        "chunk_index": 1,
        "total_size": 0,
        "manifest": {"Parts": []},
    }


@pytest.mark.usefixtures("create_large_object")
def test_complete_multipart_upload():
    config = get_config()
    manager = Boto3Manager(config)
    src_url = f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    dst_url = f"s3://localhost:7000/{TEST_BUCKET}/{COPIED_FILE_NAME}"
    mp_info = manager.create_multipart_upload(src_url=src_url, dst_url=dst_url)

    manager.complete_multipart_upload(mp_info=mp_info)

    conn = manager.get_connection("localhost:7000")
    head = conn.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert head["ContentLength"] == LARGE_NUMBER_TO_WRITE * len("test")

    conn.delete_object(Bucket=TEST_BUCKET, Key=COPIED_FILE_NAME)


@pytest.mark.usefixtures("create_large_object")
def test_copy_multipart_file():
    config = get_config()
    manager = Boto3Manager(config)
    src_url = f"s3://localhost:7000/{TEST_BUCKET}/{ORIGINAL_FILE_NAME}"
    dst_url = f"s3://localhost:7000/{TEST_BUCKET}/{COPIED_FILE_NAME}"
    mp_info = manager.create_multipart_upload(src_url=src_url, dst_url=dst_url)

    res = manager.copy_multipart_file(
        src_info=mp_info["src_info"], dst_info=mp_info["dst_info"]
    )
    assert res == {
        "md5_sum": "bc0354f0646794a755a4276435ec5a6c",
        "sha256_sum": "c97d1f1ab2ae91dbe05ad8e20bc58fc6f3af28e98d98ca8dbeee31a9d32e1e5b",
        "bytes_transferred": 40000000,
    }

    conn = manager.get_connection("localhost:7000")
    head = conn.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert head["ContentLength"] == LARGE_NUMBER_TO_WRITE * len("test")

    conn.delete_object(Bucket=TEST_BUCKET, Key=COPIED_FILE_NAME)


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
