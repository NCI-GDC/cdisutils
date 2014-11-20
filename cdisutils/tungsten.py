"""Utilities for working in tungsten provisioned environments."""

import os


def is_tungsten():
    """Are we running on a tungsten provisioned server"""
    os.path.isdir(os.path.join("etc", "tungsten"))


def pillar():
    """What tungsten pillar are we in?"""
    return open(os.path.join("etc", "tungsten", "pillar")).read().strip()


def route():
    """What tungsten route are we in?"""
    return open(os.path.join("etc", "tungsten", "route")).read().strip()


def service():
    """What tungsten service does this machine provide?"""
    return open(os.path.join("etc", "tungsten", "service")).read().strip()


def uuid():
    """What is this machine's uuid?"""
    return open(os.path.join("etc", "tungsten", "uuid")).read().strip()


# TODO some helpers for standard authentiation patterns (e.g. give me s3 keys)
