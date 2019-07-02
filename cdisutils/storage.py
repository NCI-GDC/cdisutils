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
from indexclient.client import IndexClient
from future.utils import iteritems
from future.standard_library import install_aliases
install_aliases()

from urllib.parse import urlparse

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

    def __init__(self, indexd_client, boto_manager):
        """Constructs a StorageClient

        :param indexd_client:
            DID resolver IndexdClient object or similar interface
        :param s3_config:
            BotoManager or config for BotoManager

        """
        self.indexd_client = indexd_client
        self.boto_manager = boto_manager

    @classmethod
    def from_configs(cls, indexd_client, boto_manager, **kwargs):
        return cls(
            indexd_client=IndexClient(**indexd_client),
            boto_manager=BotoManager(**boto_manager),
            **kwargs
        )

    def get_indexd_record(self, uuid):
        """Fetch Indexd doc from uuid"""

        doc = self.indexd_client.get(uuid)

        if not doc:
            raise KeyLookupError('Indexd record not found: {}'.format(uuid))

        if not doc.urls:
            raise KeyLookupError("No urls found for '{}'".format(uuid))

        return doc

    def get_key_from_uuid(self, uuid):
        """Returns a boto key given a uuid"""

        doc = self.get_indexd_record(uuid)
        return self.get_key_from_urls(doc.urls)

    def get_key_from_urls(self, urls):
        """Loop through list of urls to fetch boto key

        :raises: :class:.KeyLookupError if no urls succeeded

        """

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

    def __init__(self,
                 config={},
                 lazy=False,
                 host_aliases={},
                 stream_status=False):
        """
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
        """

        self.config = config
        for host, kwargs in iteritems(self.config):
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
        self.mp_chunk_size = 1073741824 # 1GiB

        # semi-magic number here, worth playing with if speed issues seen
        self.chunk_size = 16777216

    @property
    def hosts(self):
        return self.conns.keys()

    def connect(self):
        for host, kwargs in iteritems(self.config):
            self.conns[host] = connect_s3(**kwargs)

    def new_connection_to(self, host):
        return connect_s3(**self.config[host])

    def __getitem__(self, host):
        return self.get_connection(host)

    def harmonize_host(self, host):
        matches = {
            alias: aliased_host
            for alias, aliased_host in iteritems(self.host_aliases)
            if re.match(alias, host)
        }

        if len(matches) > 1:
            self.log.warning('matched multiple aliases: {}'.format(matches))

        if matches:
            self.log.info('using matched aliases: {}'.format(matches.keys(
            )))
            return next(iter(matches.values()))
        else:
            return host

    def get_connection(self, host):
        return self.conns[self.harmonize_host(host)]

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

    def list_buckets(self, host=None):
        total_files = 0
        if host:
            if host in self.conns:
                bucket_list = self.conns[host].get_all_buckets()
                for bucket in bucket_list:
                    file_count, bucket_size = self.get_count_of_keys_in_s3_bucket(conn, bucket.name)
                    self.log.info(bucket.name, file_count)
                    total_files = total_files + file_count

                self.log.info("{} files in {} buckets".format(total_files, len(bucket_list)))
            else:
                self.log.error("No connection to host {} found".format(host))
        else:
            self.log.error("No host given")

    def get_s3_bucket(self, host=None, bucket_name=None):
        bucket = None
        if host:
            if host in self.conns:
                try:
                    self.log.info("Getting bucket {} from {}".format(bucket_name, host))
                    bucket = self.conns[host].get_bucket(bucket_name)
                except Exception as e:
                    if e.error_code == 'NoSuchBucket':
                        print('Bucket not found')
                    else:
                        self.log.error(e)
            else:
                self.log.error("No connection to host {} found".format(host))
        else:
            self.log.error("No host given")

        self.log.info("Returning {}".format(bucket))
        return bucket

    def upload_file(self, host=None,
                    bucket_name=None,
                    file_name=None,
                    key_name=None,
                    calc_md5=False):
        if calc_md5:
            md5_sum = md5.new()

        bucket = self.get_s3_bucket(host=host, bucket_name=bucket_name)
        if not bucket:
            bucket = self.conns[host].create_bucket(bucket_name)

        new_key = Key(bucket)
        self.log.info("Creating key: {}".format(key_name))
        new_key.key = key_name
        new_key.set_contents_from_filename(file_name)

    def get_all_s3_files(self):
        all_s3_files = {}
        for s3_inst in self.s3_inst_info.keys():
            try:
                cs_conn = self.connect_to_s3(s3_inst)
            except:
                self.log.warn("Unable to connect to %s, skipping" % s3_inst)
            else:
                self.log.info("Connected to S3, cs_conn = ", cs_conn)
                if cs_conn != None:
                    all_s3_files[s3_inst] = self.get_s3_file_counts(cs_conn, s3_inst)
        return all_s3_files

    def get_all_bucket_names(self, host=None):
        bucket_names = []
        try:
            bucket_list = conn.get_all_buckets()
        except Exception as e:
            self.log.error("Unable to list buckets: %s" % e)
        else:
            for instance in bucket_list:
                bucket_names.append(instance.name)
        return bucket_names

    def print_running_status(self, transferred_bytes, start_time):
        size_info = self.get_nearest_file_size(transferred_bytes)
        cur_time = time.clock()
        base_transfer_rate = float(transferred_bytes) / float(cur_time - start_time)
        transfer_info = self.get_nearest_file_size(base_transfer_rate)
        cur_conv_size = float(transferred_bytes) / float(size_info[0])
        cur_conv_rate = base_transfer_rate / float(transfer_info[0])
        sys.stdout.write("%7.02f %s : %6.02f %s per sec\r" % (
            cur_conv_size, size_info[1],
            cur_conv_rate, transfer_info[1]))
        sys.stdout.flush()

    def get_file_key(self, host=None,
                     bucket_name=None,
                     key_name=None):
        key = None
        bucket = self.get_s3_bucket(host=host, bucket_name=bucket_name)
        if bucket:
            self.log.info("Getting key {}".format(key_name))
            key = bucket.get_key(key_name)

        return key

    def load_file(self,
                  uri=None,
                  host=None,
                  bucket_name=None,
                  key_name=None,
                  stream_status=False,
                  chunk_size=16777216):

        downloading = True
        error = False
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
            conn = self.connect()
        else:
            conn = self.conns[host]

        # get the key from the bucket
        self.log.info("Getting {} from {}".format(key_name, bucket_name))
        try:
            file_key = self.get_file_key(host=host,
                                         bucket_name=bucket_name,
                                         key_name=key_name)
        except Exception as e:
            self.log.error("Unable to get {} from {}".format(key_name, bucket_name))
            self.log.error(e)
        else:
            if file_key:
                while downloading == True:
                    try:
                        chunk = file_key.read(size=chunk_size)
                    except Exception as e:
                        error = True
                        downloading = False
                        self.log.error("Error {} reading bytes, got {} bytes".format(
                            str(e), len(chunk)))
                        total_transfer = total_transfer + len(chunk)
                    else:
                        if len(chunk) < chunk_size:
                            downloading = False
                        total_transfer += len(chunk)
                        file_data.extend(chunk)
                        if stream_status:
                            sys.stdout.write("{:6.02}%%\r".format(
                                float(total_transfer) / float(file_key.size) * 100.0))
                            sys.stdout.flush()
            else:
                self.log.warn('Unable to find key {}/{}/{}'.format(os_name, bucket_name, key_name))

        self.log.info('{} lines received'.format(len(str(file_data))))
        return str(file_data)

    
    def parse_data_file(self,
                        uri=None,
                        data_type='tsv',
                        custom_delimiter=None):
        """Processes loaded data as a tsv, csv, or
        json, returning it as a list of dicts"""
        key_data = []
        header = None
        skipped_lines = 0
        delimiters = { 'tsv': '\t',
                       'csv': ',',
                       'json': '',
                       'other': ''}
        other_delimiters = [' ', ',', ';']

        file_data = self.load_file(uri=uri)

        if data_type not in delimiters.keys():
            print("Unable to process data type %s" % data_type)
            print("Valid data types:")
            print(list(delimiters.keys()))
        else:
            if data_type == 'other':
                if custom_delimiter:
                    delimiter = custom_delimiter
                else:
                    print("With data_type 'other', a delimiter is needed")
                    raise
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
                        if len(line.strip('\n').strip()):
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

        print('%d lines in file, %d processed' % (len(file_data.split('\n')), len(key_data)))
        return key_data

    def md5_s3_key(self, conn, which_bucket, key_name, chunk_size=16777216):
        result = {
            'transfer_time': 0,
            'bytes_transferred': 0
        }
        m = md5.new()
        sha = hashlib.sha256()
        size = 0
        retries = 0
        result['start_time'] = time.time()
        error = False
        running = False
        file_key = self.get_file_key(conn, which_bucket, key_name)
        if file_key:
            running = True
            file_key.BufferSize = self.chunk_size
        else:
            self.log.warning("Unable to find key %s %s" % (which_bucket, key_name))

        while running == True:
            try:
                #chunk = file_key.read()
                chunk = file_key.read(size=chunk_size)
            except:
                if len(chunk) == 0:
                    if retries > 10:
                        print("Error reading bytes")
                        error = True
                        break
                    else:
                        retries += 1
                        print("%d: Error reading bytes, retry %d" % (id, retries))
                        time.sleep(2)
                else:
                    print("%d: Error %s reading bytes, got %d bytes" % (
                        id, str((sys.exc_info())[1]), len(chunk)))
                    total_transfer = total_transfer + len(chunk)
                    m.update(chunk)
                    sha.update(chunk)
                    retries = 0
            else:
                if len(chunk) == 0:
                    running = False
                result['bytes_transferred'] += len(chunk)
                if file_key.size > 0:
                   sys.stdout.write("%6.02f%%\r" % (float(result['bytes_transferred']) / float(file_key.size) * 100.0))
                else:
                   sys.stdout.write("0.00%%\r")

                sys.stdout.flush()
                m.update(chunk)
                sha.update(chunk)
                retries = 0
        result['transfer_time'] = time.time() - result['start_time']
        result['md5_sum'] = m.hexdigest()
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
