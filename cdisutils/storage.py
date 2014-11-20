"""Utilities for working with object stores using libcloud."""

import json

# TODO these are yolo'ed, need tests


def is_probably_swift_segments(obj):
    """The way OpenStack swift works is that you can't upload anything
    larger than 5GB. When you try to do so, swift instead uploads each
    5GB chunk of that object to it's own 'segment' (which is in a
    special segments bucket) and instead of the thing you wanted to
    upload, stores a json doc with information about how to get the
    pieces of the thing you actually wanted. This function is a
    heuristic predicate that returns True if the libcloud object
    passed in is probably one of these swift segment indicators (note
    that I say probably because there is no way to be sure, its
    possible, although unlikely, that the key really does consist of a
    jsob blob that just happens to look like a segment indicatior).
    """
    if obj.size > 10**6:
        return False
    try:
        blob = json.loads("".join(obj.as_stream()))
    except ValueError:
        return False
    if (type(blob) == list
            and len(blob) > 0
            and "path" in blob[0]
            and "etag" in blob[0]
            and "size_bytes" in blob[0]):
        return True
    else:
        return False


def swift_stream(obj):
    """Given a libcloud object containing one of the aforementioned swift
    segment indicators, return a generator that yields byte chunks of
    the chunked object it represents.
    """
    doc = json.loads("".join(obj.as_stream()))
    driver = obj.container.driver
    for segment in doc:
        container_name, obj_name = segment["path"].split("/", 2)[1:3]
        bucket = driver.get_container(container_name)
        for bytes in bucket.get_object(obj_name).as_stream():
            yield bytes
