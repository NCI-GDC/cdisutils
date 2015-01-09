"""Opinionated basic logging setup."""

import logging
import sys

LOGGERS = {}


def get_logger(name):
    """Return an opinionated basic logger named `name` that logs to
    stdout."""
    if LOGGERS.get(name):
        return LOGGERS.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s][%(name)10s][%(levelname)7s] %(message)s')
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger
