"""
Test suite for utils that interact with the storage system
"""

from cdisutils.storage import StorageClient

import pytest

INDEXD_CONFIG = dict(host='localhost', port=8000, auth=())


@pytest.fixture
def storage_client():
    return StorageClient.from_configs({
        'boto_manager': {
            'config': {
                "aws_access_key_id": "foo",
                "aws_secret_access_key": "bar",
                "is_secure": False},
            'lazy': True
        }
    })


@pytest.mark.parametrize('config', [
    {
        'boto_manager': {
            'config': {
                'localhost:5555': {
                    "aws_access_key_id": "foo",
                    "aws_secret_access_key": "bar",
                    "is_secure": False
                }
            },
            'lazy': True
        }
    }, {
        'boto_manager': {
            'config': {
                'localhost:5555': {
                    "aws_access_key_id": "foo",
                    "aws_secret_access_key": "bar",
                    "is_secure": False
                }
            },
            'host_aliases': {
                '.*service.dns.resolve': 'localhost'
            },
            'lazy': True
        },
    },
])
def test_from_configs(config):
    StorageClient.from_configs(**config)


@pytest.mark.parametrize('aliases,host,expected', [
    ({r'aws\..*\.com': 'a', r'aws\..*\.org': 'b'}, 'aws.custom.com', 'a'),
    ({r'aws\..*\.com': 'b'}, 'c', 'c'),
])
def test_aliased_connections(aliases, host, expected):
    kwargs = {"aws_access_key_id": "", "aws_secret_access_key": ""}
    StorageClient.from_configs(**{
        'boto_manager': {
            'config': {
                'a': kwargs,
                'b': kwargs,
                'c': kwargs,
            },
            'host_aliases': aliases,
        }
    }).boto_manager.get_connection(host).host == expected
