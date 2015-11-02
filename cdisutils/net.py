"""General utility belt."""

import os
import functools
import hashlib

from boto import connect_s3, s3
from dateutil import parser
from datetime import timedelta, datetime
from urlparse import urlparse


class ContextDecorator(object):
    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwds):
            with self:
                return f(*args, **kwds)
        return decorated


class no_proxy(ContextDecorator):
    def __enter__(self):
        PROXY_ENV_VARS = ["http_proxy", "https_proxy"]
        self.temp_env = {}
        for key in PROXY_ENV_VARS:
            if os.environ.get(key):
                self.temp_env[key] = os.environ.pop(key)

    def __exit__(self, exc_type, exc_value, traceback):
        for key, val in self.temp_env.iteritems():
            os.environ[key] = val


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


class BotoManager(object):
    """
    A class that abstracts away boto calls to multiple underlying
    object stores. Given a map from hostname -> arguments to
    connect_s3, it will maintain connections to all of those hosts
    which can be used transparently through this object.
    """

    def __init__(self, config, lazy=False):
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
        """
        self.config = config
        for host, kwargs in self.config.iteritems():
            # we need to pass the host argument in when we connect, so
            # set it here
            kwargs["host"] = host
            if 'calling_format' not in kwargs:
                kwargs["calling_format"] = s3.connection.OrdinaryCallingFormat()

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
        bucket = self.conns[host].get_bucket(bucket)
        return bucket.get_key(key)
