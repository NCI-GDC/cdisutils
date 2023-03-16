from cdisutils.storage3 import Boto3Manager
from tests.utils import get_config


def test_basic_connect():
    config = get_config()
    manager = Boto3Manager(config=config)
    aws_conn_mock = manager['s3.amazonaws.com']
    assert aws_conn_mock._endpoint.host == 'https://s3.amazonaws.com'
    site_conn_mock = manager['s3.myinstallation.org']
    assert site_conn_mock._endpoint.host == 'https://s3.myinstallation.org'
