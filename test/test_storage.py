# -*- coding: utf-8 -*-
"""
Test suite for utils that interact with the storage system
"""

from cdisutils.storage import StorageClient
from cdisutils.indexd_utils import IndexdTestHelper

import pytest

INDEXD_CONFIG = dict(host='localhost', port=8000, auth=())


@pytest.yield_fixture(scope='module', autouse=True)
def indexd():
    indexd = IndexdTestHelper.run_indexd()
    yield indexd
    if indexd.is_alive:
        indexd.terminate()


@pytest.fixture
def storage_client():
    return StorageClient.from_configs({
        'indexd_client': {
            'baseurl': 'http://localhost:8000'
        },
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
        'indexd_client': {'baseurl': 'http://localhost:8000'},
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
        'indexd_client': {
            'baseurl': 'http://localhost:8000',
            'version': 'v0'
        },
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
    ({'aws\..*\.com': 'a', 'aws\..*\.org': 'b'}, 'aws.custom.com', 'a'),
    ({'aws\..*\.com': 'b'}, 'c', 'c'),
])
def test_aliased_connections(aliases, host, expected):
    kwargs = {"aws_access_key_id": "", "aws_secret_access_key": ""}
    StorageClient.from_configs(**{
        'indexd_client': {'baseurl': 'http://localhost:8000'},
        'boto_manager': {
            'config': {
                'a': kwargs,
                'b': kwargs,
                'c': kwargs,
            },
            'host_aliases': aliases,
        }
    }).boto_manager.get_connection(host).host == expected
