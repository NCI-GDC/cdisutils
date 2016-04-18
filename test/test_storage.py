# -*- coding: utf-8 -*-
"""
Test suite for utils that interact with the storage system
"""

from cdisutils.storage import StorageClient
from signpost import Signpost
from multiprocessing import Process

import pytest
import requests

SIGNPOST_CONFIG = dict(host='localhost', port=8000)


def run_signpost():
    Signpost({
        "driver": "inmemory",
        "layers": ["validator"]
    }).run(**SIGNPOST_CONFIG)


def wait_for_signpost_alive():
    url = 'http://{host}:{port}'.format(**SIGNPOST_CONFIG)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return wait_for_signpost_alive()
    else:
        return


@pytest.yield_fixture(scope='module', autouse=True)
def signpost():
    signpost = Process(target=run_signpost)
    signpost.start()
    wait_for_signpost_alive()
    yield signpost
    if signpost.is_alive:
        signpost.terminate()


@pytest.fixture
def storage_client():
    return StorageClient.from_configs({
        'signpost_client': {
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
        'signpost_client': {'baseurl': 'http://localhost:8000'},
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
        'signpost_client': {
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
        'signpost_client': {'baseurl': 'http://localhost:8000'},
        'boto_manager': {
            'config': {
                'a': kwargs,
                'b': kwargs,
                'c': kwargs,
            },
            'host_aliases': aliases,
        }
    }).boto_manager.get_connection(host).host == expected
