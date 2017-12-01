"""Opinionated basic logging setup."""

import logging
import sys

LOGGERS = {}

LOG_FORMAT=logging.Formatter('[%(asctime)s][%(name)10s][%(levelname)7s] %(message)s')

def get_handler():
    """Return a stdout stream handler"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LOG_FORMAT)
    return handler

def get_file_handler(path):
    """Return a file handler"""
    handler = logging.FileHandler(path)
    handler.setFormatter(LOG_FORMAT)
    return handler

def get_logger(name):
    """Return an opinionated basic logger named `name` that logs to
    stdout."""
    if LOGGERS.get(name):
        return LOGGERS.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not (len(logger.handlers) > 0
                and type(logger.handlers[0]) == logging.StreamHandler):
            logger.addHandler(get_handler())
            logger.propagate = False
    return logger

def get_file_logger(name, path):
    """Return an opinionated basic logger named `name` that logs to
    a file at the specified path"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler(path))
    logger.propagate = False
