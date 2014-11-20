"""Opinionated basic logging setup."""

import logging
import sys


def get_logger(name):
    """Return an opinionated basic logger named `name` that logs to
    stdout."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)-4s: %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
