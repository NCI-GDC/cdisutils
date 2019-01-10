# -*- coding: utf-8 -*-
"""
cdisutils.storage
----------------------------------

Utilities for working with object stores using boto/libcloud.

TODO: add tests

"""

import os
import sys
import hashlib
import json
import re
import time
from cStringIO import StringIO as BIO
from urlparse import urlparse
from datetime import timedelta, datetime

from boto import connect_s3
from boto.s3 import connection
from boto.s3.key import Key
from dateutil import parser

from indexclient.client import IndexClient

from .log import get_logger

# TODO: add tests

DEFAULT_MP_CHUNK_SIZE = 1073741824 # 1GiB
DEFAULT_DOWNLOAD_CHUNK_SIZE = 16777216 # 16MiB

def url_for_boto_key(key):
    ''' Create standardized URL '''
    template = "s3://{host}/{bucket}/{name}"
    return template.format(
        host=key.bucket.connection.host,
        bucket=key.bucket.name,
        name=key.name
    )

def md5sum_with_size(iterable):
    ''' Get md5sum and size given an iterable (eg: a boto key) '''
    md5sum = hashlib.md5()
    size = 0
    for chunk in iterable:
        md5sum.update(chunk)
        size += len(chunk)
    return md5sum.hexdigest(), size

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
    ''' Filter out non-s3 URLs '''
    return [url for url in urls if urlparse(url).scheme == 's3']

class StorageError(Exception):
    ''' StorageError Class - bypass '''
    pass

class KeyLookupError(LookupError, StorageError):
    ''' KeyLookupError Class - bypass '''
    pass

class StorageClient(object):
    ''' Class that abstracts away storage interfaces, UUID resolution '''

    log = get_logger('storage_client')

    def __init__(self, indexd_client, boto_manager):
        '''
        Constructs a StorageClient

        :param indexd_client:
            DID resolver IndexdClient object or similar interface
        :param s3_config:
            BotoManager or config for BotoManager

        '''
        self.indexd_client = indexd_client
        self.boto_manager = boto_manager

    @classmethod
    def from_configs(cls, indexd_client, boto_manager, **kwargs):
        ''' ??? '''
        return cls(
            indexd_client=IndexClient(**indexd_client),
            boto_manager=BotoManager(**boto_manager),
            **kwargs
        )

    def get_indexd_record(self, uuid):
        ''' fetch indexd doc from uuid '''
        doc = self.indexd_client.get(uuid)

        if not doc:
            raise KeyLookupError('Indexd record not found: {}'.format(uuid))

        if not doc.urls:
            raise KeyLookupError("No urls found for '{}'".format(uuid))

        return doc

    def get_key_from_uuid(self, uuid):
        ''' Returns a boto key given a uuid '''

        doc = self.get_indexd_record(uuid)
        return self.get_key_from_urls(doc.urls)

    def get_key_from_urls(self, urls):
        '''
        Loop through list of urls to fetch boto key

        :raises: :class:.KeyLookupError if no urls succeeded

        '''

        remaining_urls = filter_s3_urls(urls)
        if not urls:
            raise KeyLookupError('No s3 urls found in %s' % urls)
        self.log.debug('using %s s3 of %s', remaining_urls, urls)

        errors = {}

        while remaining_urls:
            url = remaining_urls.pop(0)
            self.log.debug("fetching key: '%s'", url)

            try:
                return self.boto_manager.get_url(url)
            except Exception as exception:
                self.log.warning("failed to fetch '%s': %s", url, exception)
                errors[url] = exception

        raise KeyLookupError("failed to fetch key from any: %s", errors)

class BotoManager(object):
    '''
    A class that abstracts away boto calls to multiple underlying
    object stores. Given a map from hostname -> arguments to
    connect_s3, it will maintain connections to all of those hosts
    which can be used transparently through this object.
    '''

    log = get_logger('boto_manager')

    def __init__(self,
                 config={},
                 lazy=False,
                 host_aliases={},
                 stream_status=False):
        '''
        Config map should be a map from hostname to args, e.g.:
        {
            "cleversafe.service.consul: {
                "aws_access_key_id": "foo",
                "aws_secret_access_key": "bar",
                "is_secure": False,
                . . .
            },
        }

        :param host_aliases:
            A *REGEX* map from names that match the regex to hostnames
            provided in config
            e.g. ``{'aws\.accessor1\.mirror': 'cleversafe.service.consul'}``
        '''

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

        self.s3_inst_info = {
            'ceph': {
                'secure': True,
                'url': 'ceph.service.consul',
                'access_key': "",
                'secret_key': ""
            },
            'ceph2': {
                'secure': True,
                'url': 'gdc-cephb-objstore.osdc.io',
                'access_key': "",
                'secret_key': ""
            },
            'cleversafe': {
                'secure': True,
                'url': 'gdc-accessors.osdc.io',
                'access_key': "",
                'secret_key': ""
            }
        }
        self.stream_status = stream_status

        # magic number here for multipart chunk size, change with care
        self.mp_chunk_size = DEFAULT_MP_CHUNK_SIZE

        # 16 MiB is used because it was tested for performance, if
        # speed issues are seen, this is a good value to try and
        # tweak. Probably good to keep it powers of 2, and an
        # even interval of the mp_chunk_size above
        self.chunk_size = DEFAULT_DOWNLOAD_CHUNK_SIZE

    @property
    def hosts(self):
        ''' Get a list of all the hosts '''
        return self.conns.keys()

    def connect(self):
        ''' Connect to all the s3 instances given from config '''
        for host, kwargs in self.config.iteritems():
            self.conns[host] = connect_s3(**kwargs)

    def new_connection_to(self, host):
        ''' Make a new connection to an s3 instance '''
        return connect_s3(**self.config[host])

    def __getitem__(self, host):
        ''' Internal call for getting a connection '''
        return self.get_connection(host)

    def harmonize_host(self, host):
        ''' Harmonize a host name to get one in the list of hosts '''
        matches = {
            alias: aliased_host
            for alias, aliased_host in self.host_aliases.iteritems()
            if re.match(alias, host)
        }

        if len(matches) > 1:
            self.log.warning('matched multiple aliases: %s', matches)

        if matches:
            self.log.info('using matched aliases: %s', matches.keys)
            return next(matches.itervalues())
        else:
            return host

    def get_connection(self, host):
        ''' Get an s3 connection handle '''
        return self.conns[self.harmonize_host(host)]

    def get_url(self, url):
        '''
        Parse an s3://host/bucket/key formatted url and return the
        corresponding boto Key object.
        '''
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        if scheme != "s3":
            raise RuntimeError("{} is not an s3 url".format(url))
        host = parsed_url.netloc
        bucket, key = parsed_url.path.split("/", 2)[1:]
        bucket = self.get_connection(host).get_bucket(bucket)
        return bucket.get_key(key)

    def list_buckets(self, host=None):
        ''' List all buckets for a given host '''
        if host:
            if host in self.conns:
                bucket_list = self.conns[host].get_all_buckets()
                for bucket in bucket_list:
                    self.log.info(bucket.name)

                self.log.info("%d buckets", len(bucket_list))
            else:
                self.log.error("No connection to host %s found", host)
        else:
            self.log.error("No host given")

    def get_s3_bucket(self, host=None, bucket_name=None):
        ''' Get a handle to an s3 bucket '''
        bucket = None
        if host:
            if host in self.conns:
                try:
                    self.log.info("Getting bucket %s from %s", bucket_name, host)
                    bucket = self.conns[host].get_bucket(bucket_name)
                except Exception as exception:
                    if exception.error_code == 'NoSuchBucket':
                        print 'Bucket not found'
                    else:
                        self.log.error(exception)
            else:
                self.log.error("No connection to host %s found", host)
        else:
            self.log.error("No host given")

        self.log.info("Returning %s", bucket)
        return bucket

    def upload_file(self, host=None,
                    bucket_name=None,
                    file_name=None,
                    key_name=None,
                    calc_md5=False):
        ''' Upload a file to the object store '''
        if calc_md5:
            md5_sum = hashlib.md5()
        else:
            md5_sum = None

        bucket = self.get_s3_bucket(host=host, bucket_name=bucket_name)
        if not bucket:
            bucket = self.conns[host].create_bucket(bucket_name)

        new_key = Key(bucket)
        self.log.info("Creating key: %s", key_name)
        new_key.key = key_name
        new_key.set_contents_from_filename(file_name)

        return md5_sum

    def get_all_bucket_names(self, host=None):
        ''' Get all the buckets for a host '''
        bucket_names = []
        try:
            bucket_list = self.conns[host].get_all_buckets()
        except Exception as exception:
            self.log.error("Unable to list buckets: %s", exception)
        else:
            for instance in bucket_list:
                bucket_names.append(instance.name)
        return bucket_names

    def get_file_key(self, host=None,
                     bucket_name=None,
                     key_name=None):
        ''' Get a key from a given bucket '''
        key = None
        bucket = self.get_s3_bucket(host=host, bucket_name=bucket_name)
        if bucket:
            self.log.info("Getting key %s", key_name)
            key = bucket.get_key(key_name)

        return key

    def load_file(self,
                  uri=None,
                  host=None,
                  bucket_name=None,
                  key_name=None,
                  stream_status=False):

        ''' Load an object into memory '''
        downloading = True
        file_data = bytearray()
        total_transfer = 0
        chunk = []
        if uri:
            uri_data = urlparse(uri)
            host = uri_data.netloc
            os_name = uri_data.netloc.split('.')[0]
            bucket_name = uri_data.path.split('/')[1]
            key_name = uri_data.path.split('/')[-1]

        if not self.conns[host]:
            self.log.info("Making OS connections")
            self.conns[host] = self.connect()

        # get the key from the bucket
        self.log.info("Getting %s from %s", key_name, bucket_name)
        try:
            file_key = self.get_file_key(host=host,
                                         bucket_name=bucket_name,
                                         key_name=key_name)
        except Exception as exception:
            self.log.error("Unable to get %s from %s", key_name, bucket_name)
            self.log.error(exception)
        else:
            if file_key:
                while downloading:
                    try:
                        chunk = file_key.read(size=self.chunk_size)
                    except Exception as exception:
                        downloading = False
                        self.log.error("Error %s reading bytes, got %d bytes",
                                       str(exception), len(chunk))
                        total_transfer = total_transfer + len(chunk)
                    else:
                        if len(chunk) < self.chunk_size:
                            downloading = False
                        total_transfer += len(chunk)
                        file_data.extend(chunk)
                        if stream_status:
                            sys.stdout.write("%6.02f%%\r",
                                             (float(total_transfer) /
                                              float(file_key.size) * 100.0))
                            sys.stdout.flush()
            else:
                self.log.warn('Unable to find key %s/%s/%s',
                              os_name, bucket_name, key_name)

        self.log.info('%d lines received', len(str(file_data)))
        return str(file_data)

    def parse_data_file(self,
                        uri=None,
                        data_type='tsv',
                        custom_delimiter=None):
        '''
        Processes loaded data as a tsv, csv, or
        json, returning it as a list of dicts
        '''
        key_data = []
        header = None
        skipped_lines = 0
        delimiters = {'tsv': '\t',
                      'csv': ',',
                      'json': '',
                      'other': ''}
        other_delimiters = [' ', ',', ';']

        file_data = self.load_file(uri=uri)

        if data_type not in delimiters.keys():
            self.log.warning("Unable to process data type %s", data_type)
            self.log.warning("Valid data types:")
            self.log.warning("%s", delimiters.keys())
        else:
            if data_type == 'other':
                if custom_delimiter:
                    delimiter = custom_delimiter
                else:
                    raise Exception("With data_type 'other', a delimiter is needed")
            else:
                delimiter = delimiters[data_type]

            if data_type == 'json':
                for line in file_data.split('\n'):
                    line_data = json.loads(line)
                    key_data.append(line_data)
            # load as tsv/csv, assuming the first row is the header
            # that provides keys for the dict
            else:
                for line in file_data.split('\n'):
                    if delimiter in line:
                        if line.strip('\n').strip():
                            if not header:
                                header = line.strip('\n').split(delimiter)
                            else:
                                line_data = dict(zip(header, line.strip('\n')\
                                                            .split(delimiter)))
                                key_data.append(line_data)
                    else:
                        # ok, let's see if we can be smart here
                        #if not header:
                        #    remaining_chars = set([c for c in line if not c.isalnum()])
                        skipped_lines += 1

        self.log.info('%d lines in file, %d processed',
                      len(file_data.split('\n')), len(key_data))
        return key_data

    def md5_s3_key(self, conn,
                   which_bucket,
                   key_name):
        ''' Get the checksum of an s3 object '''
        result = {
            'transfer_time': 0,
            'bytes_transferred': 0
        }
        md5sum = hashlib.md5()
        sha = hashlib.sha256()
        total_transfer = 0
        retries = 0
        result['start_time'] = time.time()
        running = False
        file_key = self.get_file_key(conn, which_bucket, key_name)
        if file_key:
            running = True
            file_key.BufferSize = self.chunk_size
        else:
            self.log.warning("Unable to find key %s %s", which_bucket, key_name)

        while running:
            try:
                #chunk = file_key.read()
                chunk = file_key.read(size=self.chunk_size)
            except Exception as exception:
                if chunk:
                    if retries > 10:
                        self.log.error("Error reading bytes: %s", exception)
                        break
                    else:
                        retries += 1
                        self.log.error("Error reading bytes %s, retry %d",
                                       exception, retries)
                        time.sleep(2)
                else:
                    self.log.error("Error reading bytes %s, got %d bytes",
                                   exception, len(chunk))
                    total_transfer = total_transfer + len(chunk)
                    md5sum.update(chunk)
                    sha.update(chunk)
                    retries = 0
            else:
                if chunk == 0:
                    running = False
                result['bytes_transferred'] += len(chunk)
                if file_key.size > 0:
                    sys.stdout.write("{:6.02f}%%\r".format(
                        (float(result['bytes_transferred']) /
                         float(file_key.size) * 100.0)))
                else:
                    sys.stdout.write("0.00%%\r")

                sys.stdout.flush()
                md5sum.update(chunk)
                sha.update(chunk)
                retries = 0
        result['transfer_time'] = time.time() - result['start_time']
        result['md5_sum'] = md5sum.hexdigest()
        result['sha256_sum'] = sha.hexdigest()
        return result

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
    if (isinstance(blob, list)
            and blob
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
        for get_bytes in bucket.get_object(obj_name).as_stream():
            yield get_bytes
