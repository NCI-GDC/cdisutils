# -*- coding: utf-8 -*-
"""
cdisutils.misc
----------------------------------

General application utilities
"""

from collections import Sequence
try:
    # python2
    from types import StringTypes
    string_types = StringTypes
except ImportError:
    # python3
    string_types = (str,)


def iterable(value):
    """Wraps scalars in a list.  Strings are treated as scalars"""

    if isinstance(value, Sequence) and not isinstance(value, string_types):
        return value
    else:
        return [value]
