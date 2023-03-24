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
            for key in tree if key not in remove_keys
        }
    elif isinstance(tree, list):
        return sorted(
            sort_dict(element, remove_keys=remove_keys) for element in tree
        )
    else:
        return tree


def remove_keys_from_dict(tree, remove_keys):
    """
    Recursively remove keys from dictionary tree
    """
    if remove_keys is None or remove_keys == []:
        return tree

    if isinstance(tree, dict):
        return {
            key: remove_keys_from_dict(tree[key], remove_keys)
            for key in tree if key not in remove_keys
        }
    elif isinstance(tree, list):
        return [
            remove_keys_from_dict(element, remove_keys)
            for element in tree
        ]
    else:
        return tree
