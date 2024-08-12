import hashlib
import sys


def md5sum(iterable):
    md5 = (
        hashlib.md5() if sys.version_info < (3, 9) else hashlib.md5(usedforsecurity=False)
    )  # nosec
    for chunk in iterable:
        md5.update(chunk)
    return md5.hexdigest()
