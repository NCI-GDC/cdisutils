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
def create_file(tmp_path_factory):
    large_file = tmp_path_factory.mktemp("data") / ORIGINAL_FILE_NAME
    with open(str(large_file), "w") as fp:
        for _ in range(LARGE_NUMBER_TO_WRITE):
            fp.write("test")

    yield large_file


@pytest.fixture
def create_object(create_file, moto_server):
    # s3_client = make_s3_client()
    # s3_client.upload_file(str(create_large_file), TEST_BUCKET, ORIGINAL_FILE_NAME)
    s3 = boto3.client("s3", endpoint_url='https://localhost:7000', verify=False)
    s3.create_bucket(Bucket=TEST_BUCKET)
    s3.put_object(Body=open(str(create_file), 'rb'), Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    # s3.upload_fileobj(TEST_BUCKET, ORIGINAL_FILE_NAME).put(Body=open(str(create_large_file), 'rb'))
    original_object = s3.head_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)
    assert original_object["ContentLength"] == create_file.stat().st_size

    yield original_object

    s3.delete_object(Bucket=TEST_BUCKET, Key=ORIGINAL_FILE_NAME)


def test_get_url(create_object, moto_server):
    config = get_config()
    manager = Boto3Manager(config)
    key = manager.get_url("s3://localhost:7000/{}/{}".format(TEST_BUCKET, ORIGINAL_FILE_NAME))