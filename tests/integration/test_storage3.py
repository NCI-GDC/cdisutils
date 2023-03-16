import os

import boto3
import pytest

from cdisutils.storage3 import Boto3Manager


LARGE_NUMBER_TO_WRITE = 10
ORIGINAL_FILE_NAME = "original_file"
COPIED_FILE_NAME = "copied_file"
S3_URL = "s3://{}".format(os.getenv("CLEVERSAFE_ENDPOINT", "localhost:7000"))
TEST_BUCKET = 'test_bucket'


def get_config():
    return {
        "localhost:7000": {
            "aws_secret_access_key": "testing",
            "aws_access_key_id": "testing",
            "verify": False
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
    # s3_client = make_s3_client()
    # s3_client.upload_file(str(create_large_file), TEST_BUCKET, ORIGINAL_FILE_NAME)
    s3 = boto3.client("s3", endpoint_url='https://localhost:7000', verify=False)
    s3.create_bucket(Bucket=TEST_BUCKET)
    s3.put_object(Body=open(str(create_large_file), 'rb'), Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    # s3.upload_fileobj(TEST_BUCKET, ORIGINAL_FILE_NAME).put(Body=open(str(create_large_file), 'rb'))
    original_object = s3.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert original_object["ContentLength"] == create_large_file.stat().st_size

    yield original_object

    s3.delete_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)


@pytest.mark.usefixtures('create_large_object')
def test_get_connection():
    config = get_config()
    manager = Boto3Manager(config)
    conn = manager.get_connection('localhost:7000')
    head = conn.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert head['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.usefixtures('create_large_object')
def test_parse_url():
    config = get_config()
    manager = Boto3Manager(config)
    s3_info = manager.parse_url("s3://localhost:7000/{}/{}".format(TEST_BUCKET, ORIGINAL_FILE_NAME))
    assert s3_info == {'url': 's3://localhost:7000/test_bucket/original_file', 's3_loc': 'localhost:7000', 'bucket_name': 'test_bucket', 'key_name': 'original_file'}


@pytest.mark.usefixtures('create_large_object')
def test_get_url():
    config = get_config()
    manager = Boto3Manager(config)
    key = manager.get_url("s3://localhost:7000/{}/{}".format(TEST_BUCKET, ORIGINAL_FILE_NAME))
    assert key['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.usefixtures('create_large_object')
def test_list_buckets():
    config = get_config()
    manager = Boto3Manager(config)
    bucket_list = manager.list_buckets('localhost:7000')
    assert len(bucket_list) == 1
    assert bucket_list[0]['Name'] == TEST_BUCKET

