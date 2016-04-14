# -*- coding: utf-8 -*-
"""
cdisutils.storage
----------------------------------

Utilities for working with object stores using boto/libcloud.

TODO: add tests

"""

from .log import get_logger
from boto import connect_s3
from boto.s3 import connection
from dateutil import parser
from datetime import timedelta, datetime
from signpostclient import SignpostClient
from urlparse import urlparse

import hashlib
import json
import re

# TODO: add tests


def url_for_boto_key(key):
    template = "s3://{host}/{bucket}/{name}"
    return template.format(
        host=key.bucket.connection.host,
        bucket=key.bucket.name,
        name=key.name
    )


def md5sum_with_size(iterable):
    '''
    Get md5sum and size given an iterable (eg: a boto key)
    '''
    md5 = hashlib.md5()
    size = 0
    for chunk in iterable:
        md5.update(chunk)
        size += len(chunk)
    return md5.hexdigest(), size


def cancel_stale_multiparts(bucket, stale_days=7):
    '''
    Cancel uploads that are stale for more than [stale_days]
    File state shouldn't be effected as there might be ongoing upload for this key
    '''
    uploads = bucket.get_all_multipart_uploads()
    for upload in uploads:
        initiated = parser.parse(upload.initiated)
        if (datetime.now(initiated.tzinfo) - initiated) > timedelta(days=stale_days):
            upload.cancel_upload()


def filter_s3_urls(urls):
    return [url for url in urls if urlparse(url).scheme == 's3']


class StorageError(Exception):
    pass


class KeyLookupError(LookupError, StorageError):
    pass


class StorageClient(object):
    """Class that abstracts away storage interfaces, UUID resolution"""

    log = get_logger('storage_client')

    def __init__(self, signpost_client, boto_manager):
        """Constructs a StorageClient

        :param signpost_client:
            DID resolver SignpostClient object or similar interface
        :param s3_config:
            BotoManager or config for BotoManager

        """
        self.signpost_client = signpost_client
        self.boto_manager = boto_manager

    @classmethod
    def from_configs(cls, signpost_client, boto_manager, **kwargs):
        return cls(
            signpost_client=SignpostClient(**signpost_client),
            boto_manager=BotoManager(**boto_manager),
            **kwargs
        )

    def get_signpost_record(self, uuid):
        """Fetch Signpost doc from uuid"""

        doc = self.signpost_client.get(uuid)

        if not doc:
            raise KeyLookupError('Signpost record not found: {}'.format(uuid))

        if not doc.urls:
            raise KeyLookupError("No urls found for '{}'".format(uuid))

        return doc

    def get_key_from_uuid(self, uuid):
        """Returns a boto key given a uuid"""

        doc = self.get_signpost_record(uuid)
        return self.get_key_from_urls(doc.urls)

    def get_key_from_urls(self, urls):
        """Loop through list of urls to fetch boto key"""

        remaining_urls = filter_s3_urls(urls)
        if not urls:
            raise KeyLookupError('No s3 urls found in {}'.format(urls))
        self.log.debug('using {} s3 of {}'.format(remaining_urls, urls))

        errors = {}

        while remaining_urls:
            url = remaining_urls.pop(0)
            self.log.debug("fetching key: '{}'".format(url))

            try:
                return self.boto_manager.get_url(url)
            except Exception as e:
                self.log.warning("failed to fetch '{}': {}".format(url, e))
                errors[url] = e

        raise KeyLookupError("failed to fetch key from any: {}".format(errors))


class BotoManager(object):
    """
    A class that abstracts away boto calls to multiple underlying
    object stores. Given a map from hostname -> arguments to
    connect_s3, it will maintain connections to all of those hosts
    which can be used transparently through this object.
    """

    log = get_logger('boto_manager')

    def __init__(self, config, lazy=False, host_aliases={}):
        """
        Config map should be a map from hostname to args, e.g.:
        {
            "s3.amazonaws.com": {
                "aws_access_key_id": "foo",
                "aws_secret_access_key": "bar",
                "is_secure": False,
                . . .
            },
        }

        :param host_aliases:
            A *REGEX* map from names that match the regex to hostnames
            provided in config
            e.g. ``{'aws\.accesssor1\.mirror': 's3.amazonaws.com'}``
        """

        self.config = config
        for host, kwargs in self.config.iteritems():
            # we need to pass the host argument in when we connect, so
            # set it here
            kwargs["host"] = host
            if 'calling_format' not in kwargs:
                kwargs["calling_format"] = connection.OrdinaryCallingFormat()

        self.host_aliases = host_aliases

        self.conns = {}
        if not lazy:
            self.connect()

    @property
    def hosts(self):
        return self.conns.keys()

    def connect(self):
        for host, kwargs in self.config.iteritems():
            self.conns[host] = connect_s3(**kwargs)

    def new_connection_to(self, host):
        return connect_s3(**self.config[host])

    def __getitem__(self, host):
        return self.get_connection(host)

    def get_connection(self, host):
        matched = {
            alias: aliased_host
            for alias, aliased_host in self.host_aliases.iteritems()
            if re.match(alias, host)
        }

        if len(matched) > 1:
            self.log.warning('matched multiple aliases: {}'.format(matched))

        if matched:
            self.log.info('using matched aliases: {}'.format(matched.keys()))
            return self.conns[next(matched.itervalues())]
        else:
            return self.conns[host]

    def get_url(self, url):
        """
        Parse an s3://host/bucket/key formatted url and return the
        corresponding boto Key object.
        """
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        if scheme != "s3":
            raise RuntimeError("{} is not an s3 url".format(url))
        host = parsed_url.netloc
        bucket, key = parsed_url.path.split("/", 2)[1:]
        bucket = self.get_connection(host).get_bucket(bucket)
        return bucket.get_key(key)


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
