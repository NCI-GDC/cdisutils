# -*- coding: utf-8 -*-

from cdisutils.settings import global_settings

import pytest


def test_missing_setting():
    with pytest.raises(KeyError):
        global_settings['key']


def test_missing_setting_default():
    assert global_settings.get('key', '1234') == '1234'


def test_from_file(temp_settings):
    global_settings.load(temp_settings)
    assert global_settings.get('namespace1') == {'var1': 3, 'var2': 'test'}


def test_clear(temp_settings):
    global_settings.settings['key1'] = True
    global_settings.clear()
    assert 'key1' not in global_settings.settings


def test_delitem(temp_settings):
    global_settings.settings['key1'] = True
    del global_settings['key1']
    assert 'key1' not in global_settings.settings


def test_setitem(temp_settings):
    global_settings['key1'] = True
    assert global_settings.settings['key1'] is True
