from cdisutils.storage3 import Boto3Manager


def test_basic_connect():
    config = {
        "s3.amazonaws.com": {
            "aws_secret_access_key": "aws_key",
            "aws_access_key_id": "secret_key",
        },
        "s3.myinstallation.org": {
            "aws_secret_access_key": "my_key",
            "aws_access_key_id": "my_secret_key",
        },
    }
    manager = Boto3Manager(config=config)
    aws_conn_mock = manager['s3.amazonaws.com']
    assert aws_conn_mock._endpoint.host == 'https://s3.amazonaws.com'
    site_conn_mock = manager['s3.myinstallation.org']
    assert site_conn_mock._endpoint.host == 'https://s3.myinstallation.org'