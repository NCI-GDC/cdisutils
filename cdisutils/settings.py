# -*- coding: utf-8 -*-
"""
cdisutils.settings
----------------------------------

Define global configuration class
"""

import os
import yaml

from .log import get_logger
from .misc import iterable


class GlobalSettings(object):
    """
    This class is used to contain the global settings.  The
    settings will be imported on the load of the module from the
    default path.

    """

    settings = {}

    default_paths = [
        os.path.expanduser('~/.config/gdc/settings.yaml'),
        '/etc/gdc/settings.yaml'
    ]

    log = get_logger(__name__)

    @classmethod
    def load(cls, paths=default_paths, overwrite=True):
        """Loads config files in reverse order. This means that values from
        the first path in the list override any collisions in the
        second path, etc.

        :param path: A string or list of strings to use instead of defaults
        :param bool overwrite: Delete any previously existing settings

        """

        new_settings = {
            # Load settings, overwriting entries from the bottom of
            # list with entries from the top
            key: value
            for path in reversed(iterable(paths))
            for key, value in cls.load_path(path).items()
        }

        if overwrite:
            cls.settings = new_settings
        else:
            cls.settings.update(new_settings)

    @classmethod
    def load_path(cls, path):
        cls.log.debug("Loading settings file: '{}'".format(path))
        config_yaml = cls.read_config(path)

        if config_yaml is None:
            cls.log.warning('Proceeding without settings from {}'.format(path))
            return {}

        try:
            return yaml.safe_load(config_yaml)
        except yaml.error.YAMLError as e:
            msg = "Unable to load settings from {}: {}".format(path, e)
            cls.log.error(msg)
            return {}

    @classmethod
    def read_config(cls, path):
        cls.log.debug("Loading settings file: '{}'".format(path))

        try:
            with open(path, 'r') as yaml_file:
                settings_yaml = yaml_file.read()
        except Exception as e:
            cls.log.warning("Unable to read settings '{}': {}".format(path, e))
        else:
            cls.log.info("Read settings from '{}'.".format(path))
            return settings_yaml

    @classmethod
    def get(cls, key, default=None):
        """Get a value from settings. If absent, log and return None"""

        try:
            return cls.settings[key]
        except KeyError as e:
            msg = 'Missing setting {}, using default. {}'.format(key, e)
            cls.log.info(msg)
            return default

    @classmethod
    def set(cls, key, value):
        """Set a value"""
        if key in cls.settings:
            cls.log.info("Overwriting setting {}".format(key))
        cls.settings[key] = value
        return value

    @classmethod
    def update(cls, settings):
        for key, value in settings.iteritems():
            cls.set(key, value)

    @classmethod
    def __getitem__(cls, key):
        return cls.settings[key]

    @classmethod
    def __setitem__(cls, key, value):
        cls.set(key, value)

    @classmethod
    def __delitem__(cls, key):
        del cls.settings[key]

    @classmethod
    def __repr__(cls):
        return "<GlobalSettings({})>".format(', '.join(cls.settings.keys()))

    @classmethod
    def clear(cls):
        if cls.settings != {}:
            cls.log.warning("Deleting global settings!")
        cls.settings = {}


global_settings = GlobalSettings()
