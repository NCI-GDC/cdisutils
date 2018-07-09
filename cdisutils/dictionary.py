# -*- coding: utf-8 -*-
"""
Helper functions to work with dictionary data
"""


def sort_dict(tree, remove_keys=None):
    """
    Recursively sorts dictionary tree and removes some keys
    """
    if remove_keys is None:
        remove_keys = []

    if isinstance(tree, dict):
        return {
            key: sort_dict(tree[key], remove_keys=remove_keys)
            for key in tree.keys() if key not in remove_keys
        }
    elif isinstance(tree, list):
        return sorted(
            [sort_dict(element, remove_keys=remove_keys) for element in tree]
        )
    else:
        return tree
