"""Opinionated basic logging setup."""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return an opinionated basic logger named `name` that logs to
    stdout."""
    return logging.getLogger(name)
