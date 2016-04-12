# -*- coding: utf-8 -*-

import pytest
import os

from cdisutils.env import env_load_yaml


def test_missing_env_load_yaml():
    assert env_load_yaml('KEY1') == {}


@pytest.mark.parametrize('config,expected', [
    ("""top:
          key1: foo
          key2: 1
          keyB: false
    """,
     {'top': {'key1': 'foo', 'key2': 1, 'key3': False}}),
    ("top: {key1: foo, key2: 1, keyB: false}",
     {'top': {'key1': 'foo', 'key2': 1, 'key3': False}}),
])
def test_valid_env_load_yaml(config, expected, monkeypatch):
    monkeypatch.setitem(os.environ, 'KEY2', config)
    assert env_load_yaml('KEY2') == expected


@pytest.mark.parametrize('config', [
    'test1: test2: test3',
    '"'
])
def test_valid_env_load_yaml(config, monkeypatch):
    monkeypatch.setitem(os.environ, 'KEY3', config)
    assert env_load_yaml('KEY3') == {}
