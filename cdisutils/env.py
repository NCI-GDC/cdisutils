# -*- coding: utf-8 -*-
"""
cdisutils.env
----------------------------------

Utility functions for interacting with the environment.

"""

import os
import yaml


from .log import get_logger
logger = get_logger(__name__)


def env_load_yaml(variable):
    try:
        config = os.environ[variable]
    except KeyError as e:
        logger.warning("{} not set".format(variable))
        return {}

    try:
        return yaml.safe_load(config)
    except yaml.error.YAMLError as e:
        logger.warning("Failed to parse {}: {}".format(variable, e))
        return {}
