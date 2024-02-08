"""
cdisutils.storage3
----------------------------------

Utilities for working with object stores using boto3

"""
import hashlib
import io
import json
import os
import re
import sys
import time
from urllib.parse import urlparse

import boto3
import urllib3
from botocore.exceptions import ClientError

from .log import get_logger

# NOTE: These are to disable the cert mismatch for our object stores
# should we ever fix that, we should remove these
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)

# magic number here for multipart chunk size, change with care
DEFAULT_MP_CHUNK_SIZE = 1073741824  # 1GiB

# 16 MiB is used because it was tested for performance, if
# speed issues are seen, this is a good value to try and
# tweak. Probably good to keep it powers of 2, and an
# even interval of the mp_chunk_size above
DEFAULT_DOWNLOAD_CHUNK_SIZE = 16777216  # 16MiB


def get_nearest_file_size(size):
    """
    Given a size, in bytes, get the nearest canonical
    size, and return both the divisor and string size
    in a tuple

    """
    sizes = [
        (1000000000000000, "PB"),
        (1000000000000, "TB"),
        (1000000000, "GB"),
        (1000000, "MB"),
        (1000, "KB"),
    ]

    value = sizes[len(sizes) - 1]
    for entry in sizes:
        if size < entry[0]:
            continue
        value = entry
        break

    return value


def print_running_status(
    transferred_bytes=None, start_time=None, total_size=None, msg_id=0
):
    """Print the status of a transfer, given time and size"""
    size_info = get_nearest_file_size(transferred_bytes)
    cur_time = time.perf_counter()
    base_transfer_rate = float(transferred_bytes) / float(cur_time - start_time)
    transfer_info = get_nearest_file_size(base_transfer_rate)
    cur_conv_size = float(transferred_bytes) / float(size_info[0])
    cur_conv_rate = base_transfer_rate / float(transfer_info[0])
    if total_size:
        percent_complete = float(transferred_bytes) / float(total_size) * 100.0
        sys.stdout.write(
            "{:3d}: {:7.02f} {} ({:6.02f}%) : {:6.02f} {} / sec\r".format(
                msg_id,
                cur_conv_size,
                size_info[1],
                percent_complete,
                cur_conv_rate,
                transfer_info[1],
            )
        )
    else:
        sys.stdout.write(
            "{:3d}: {:7.02f} {} : {:6.02f} {} / sec\r".format(
                msg_id, cur_conv_size, size_info[1], cur_conv_rate, transfer_info[1]
            )
        )
    sys.stdout.flush()


def load_creds():
    """Load s3 creds from environment vars"""
    s3_creds = {}
    s3_key_mapping = {
        "ACCESS_KEY": "aws_access_key_id",
        "SECRET_KEY": "aws_secret_access_key",
        # 'ENDPOINT': 'url',
        "SECURE": "use_ssl",
        "VALIDATE_CERTS": "verify",
    }
    s3_endpoint_defaults = {
        "ceph": "ceph.service.consul",
        "cephb": "gdc-cephb-objstore.osdc.io",
        "cleversafe": "cleversafe.service.consul",
        "aws": "s3-external-1.amazonaws.com",
        "jamboree": "gdc-accessors-jamboree.osdc.io",
        "pdc": "bionimbus-objstore-cs.opensciencedatacloud.org",
    }
    s3_inst_default = {
        "use_ssl": True,
        "verify": True,
        "aws_access_key_id": "",
        "aws_secret_access_key": "",
        # 'url': ''
    }

    for env in os.environ:
        for key in s3_key_mapping:
            if key in env:
                os_name = env[: env.find(key)].rstrip("_").lower()
                s3_key = s3_endpoint_defaults.get(os_name)
                if s3_key:
                    if s3_key not in s3_creds:
                        s3_creds[s3_key] = dict(s3_inst_default)
                    if isinstance(s3_inst_default[s3_key_mapping[key]], bool):
                        if str(os.environ[env]).lower() == "false":
                            s3_creds[s3_key][s3_key_mapping[key]] = False
                        else:
                            s3_creds[s3_key][s3_key_mapping[key]] = True
                    elif isinstance(s3_inst_default[s3_key_mapping[key]], list):
                        if not s3_creds[s3_key][s3_key_mapping[key]]:
                            s3_creds[s3_key][s3_key_mapping[key]] = []
                        s3_creds[s3_key][s3_key_mapping[key]].append(
                            str(os.environ[env])
                        )
                    else:
                        s3_creds[s3_key][s3_key_mapping[key]] = str(os.environ[env])

    for key, value in s3_creds[s3_key].items():
        if not str(value):
            print(f"Incomplete cred data for {s3_key}: {key}")

    return s3_creds


class Boto3Manager:
    """
    A class that abstracts away boto3 calls to multiple underlying
    object stores. Given a map from hostname -> arguments to
    connect_s3, it will maintain connections to all of those hosts
    which can be used transparently through this object.
    """

    log = get_logger("boto3_manager")

    def __init__(self, config=None, lazy=False, host_aliases=None, stream_status=False):
        """
        Config map should be a map from hostname to args, e.g.:
        {
            "cleversafe.service.consul: {
                "aws_access_key_id": "foo",
                "aws_secret_access_key": "bar",
                "verify": False,
                . . .
            },
        }

        :param host_aliases:
            A *REGEX* map from names that match the regex to hostnames
            provided in config
            e.g. ``{'aws.accessor1.mirror': 'cleversafe.service.consul'}``
        """

        if config:
            self.config = config
        else:
            self.config = {}
        for host, kwargs in self.config.items():
            # we need to pass the host argument in when we connect, so
            # set it here
            kwargs["host"] = host
            # if 'calling_format' not in kwargs:
            #    kwargs["calling_format"] = connection.OrdinaryCallingFormat()

        if host_aliases:
            self.host_aliases = host_aliases
        else:
            self.host_aliases = {}

        self.conns = {}
        if not lazy:
            self.connect()

        self.s3_inst_info = {
            "ceph": {
                "secure": True,
                "url": "ceph.service.consul",
                "access_key": "",
                "secret_key": "",
            },
            "ceph2": {
                "secure": True,
                "url": "gdc-cephb-objstore.osdc.io",
                "access_key": "",
                "secret_key": "",
            },
            "cleversafe": {
                "secure": True,
                "url": "gdc-accessors.osdc.io",
                "access_key": "",
                "secret_key": "",
            },
        }
        self.stream_status = stream_status

        self.mp_chunk_size = DEFAULT_MP_CHUNK_SIZE
        self.chunk_size = DEFAULT_DOWNLOAD_CHUNK_SIZE

    def __getitem__(self, host):
        """Internal call for getting a connection"""
        return self.get_connection(host)

    def harmonize_host(self, host):
        """Harmonize a host name to get one in the list of hosts"""
        matches = {
            alias: aliased_host
            for alias, aliased_host in self.host_aliases.items()
            if re.match(alias, host)
        }

        if len(matches) > 1:
            self.log.warning("matched multiple aliases: %s", matches)

        if matches:
            self.log.info("using matched aliases: %s", matches.keys)
            return next(iter(matches.values()))
        else:
            return host

    def get_connection(self, host):
        """Get an s3 connection handle"""
        return self.conns[self.harmonize_host(host)]

    def connect(self):
        """Connect to all hosts in config"""
        for host in self.config:
            self.conns[host] = self.new_connection_to(host)

    def new_connection_to(self, host):
        """Connect to a given host"""
        if "https" not in host:
            s3_url = f"https://{host}"
        else:
            s3_url = host
        # TODO: Allow the location to be passed in via config
        cur_dict = dict(self.config[host])
        del cur_dict["host"]
        if "ceph" in s3_url:
            cur_dict["config"] = boto3.session.Config(signature_version="s3")
        if cur_dict.get("verify") == "false":
            print("Skipping verify")
            cur_dict.pop("verify")
            conn = boto3.client(
                "s3", "us-east-1", endpoint_url=s3_url, verify=False, **cur_dict
            )
        else:
            print("No verify found, using dict")
            conn = boto3.client("s3", "us-east-1", endpoint_url=s3_url, **cur_dict)

        return conn

    def parse_url(self, url=None):
        """Parse a URL into a dictionary with component parts"""
        s3_info = {"url": url, "s3_loc": None, "bucket_name": None, "key_name": None}
        parts = urlparse(url)
        for key in self.config:
            if key in parts.netloc:
                s3_info["s3_loc"] = key
                break
        s3_info["bucket_name"] = parts.path.split("/")[1]
        s3_info["key_name"] = "/".join(parts.path.split("/")[2:])

        return s3_info

    def get_url(self, url):
        """
        Parse an s3://host/bucket/key formatted url and return the
        corresponding boto Key object.
        """
        parsed_url = self.parse_url(url=url)
        if not url.lower().startswith("s3"):
            raise RuntimeError("%s is not an s3 url" % url)
        key = self.get_connection(parsed_url["s3_loc"]).get_object(
            Bucket=parsed_url["bucket_name"], Key=parsed_url["key_name"]
        )
        return key

    def head_url(self, url):
        """
        Parse an s3://host/bucket/key formatted url and return the
        corresponding boto Key metadata without the object.
        """
        parsed_url = self.parse_url(url=url)
        if not url.lower().startswith("s3"):
            raise RuntimeError("%s is not an s3 url" % url)
        try:
            key = self.get_connection(parsed_url["s3_loc"]).head_object(
                Bucket=parsed_url["bucket_name"], Key=parsed_url["key_name"]
            )
        except ClientError as exception:
            self.log.warning("Unable to find %s: %s", url, exception)
            key = None

        return key

    def list_buckets(self, host=None):
        """List all buckets available for a given host"""
        bucket_list = []
        if host:
            if host in self.conns:
                bucket_list = self.conns[host].list_buckets().get("Buckets", [])
            else:
                self.log.error("No connection to host %s found", host)
        else:
            self.log.error("No host given")

        return bucket_list

    def create_multipart_upload(self, src_url=None, dst_url=None):
        """
        Create a multipart upload, holding session info in a dict

        TODO: Hold this in the class vars
        """
        multipart_info = {}

        multipart_info["dst_info"] = self.parse_url(url=dst_url)
        multipart_info["src_info"] = self.parse_url(url=src_url)
        multipart_info["stream_buffer"] = io.BytesIO()
        multipart_info["mp_chunk_size"] = self.mp_chunk_size
        multipart_info["download_chunk_size"] = self.chunk_size
        multipart_info["cur_size"] = 0
        multipart_info["chunk_index"] = 1
        multipart_info["total_size"] = 0
        multipart_info["manifest"] = {"Parts": []}
        multipart_info["md5_sum"] = hashlib.md5()
        multipart_info["sha256_sum"] = hashlib.sha256()
        multipart_info["start_time"] = time.perf_counter()
        mp_info = self.conns[
            multipart_info["dst_info"]["s3_loc"]
        ].create_multipart_upload(
            Bucket=multipart_info["dst_info"]["bucket_name"],
            Key=multipart_info["dst_info"]["key_name"],
        )
        multipart_info["mp_id"] = mp_info.get("UploadId", None)
        if not multipart_info["mp_id"]:
            raise Exception("Unable to get valid ID for multipart upload: %s" % mp_info)

        return multipart_info

    def complete_multipart_upload(self, mp_info=None):
        """
        Completes a multipart upload, using the
        manifest aggregated by uploading parts
        """
        try:
            self.conns[mp_info["dst_info"]["s3_loc"]].complete_multipart_upload(
                Bucket=mp_info["dst_info"]["bucket_name"],
                Key=mp_info["dst_info"]["key_name"],
                MultipartUpload=mp_info["manifest"],
                UploadId=mp_info["mp_id"],
            )
        except ClientError as exception:
            raise Exception(
                "Unable to complete mulitpart {}: {}".format(
                    mp_info["mp_id"], exception
                )
            )

    def upload_multipart_chunk(self, mp_info):
        """Uploads a multipart chunk of an object"""

        mp_info["stream_buffer"].seek(0)
        try:
            result = self.conns[mp_info["dst_info"]["s3_loc"]].upload_part(
                Body=mp_info["stream_buffer"],
                Bucket=mp_info["dst_info"]["bucket_name"],
                Key=mp_info["dst_info"]["key_name"],
                PartNumber=mp_info["chunk_index"],
                UploadId=mp_info["mp_id"],
            )
        except ClientError as exception:
            raise Exception(
                "Error writing %d bytes to %s: %s"
                % (mp_info["cur_size"], mp_info["dst_info"]["url"], exception)
            )
        else:
            mp_info["cur_size"] = 0
            mp_info["stream_buffer"].close()
            mp_info["stream_buffer"] = io.BytesIO()
            mp_info_part = {
                "ETag": result["ETag"],
                "PartNumber": mp_info["chunk_index"],
            }
            mp_info["manifest"]["Parts"].append(mp_info_part)
            mp_info["chunk_index"] += 1

    def download_object_part(self, key):
        """Downloads a chunk of an object"""
        return key.read(amt=self.chunk_size)

    def copy_multipart_file(
        self, src_info=None, dst_info=None, stream_status=True, msg_id=0
    ):
        """
        Routine to use boto3 to copy a file
        multipart between object stores
        """

        if isinstance(src_info, str):
            src_url = src_info
            src_info = self.parse_url(url=src_url)
        if isinstance(dst_info, str):
            dst_url = dst_info
            dst_info = self.parse_url(url=dst_url)

        self.log.info("Copying %s to %s", src_info["url"], dst_info["url"])

        # get the source key
        self.log.info(
            "Getting %s (%s %s)",
            src_info["url"],
            src_info["bucket_name"],
            src_info["key_name"],
        )
        try:
            src_key_info = self.conns[src_info["s3_loc"]].get_object(
                Bucket=src_info["bucket_name"], Key=src_info["key_name"]
            )
        except ClientError as exception:
            raise Exception("Unable to get {}: {}".format(src_info["url"], exception))

        if src_key_info:
            src_key = src_key_info.get("Body", None)
            src_key_size = src_key_info.get("ContentLength", None)
            mp_info = self.create_multipart_upload(
                src_url=src_info["url"], dst_url=dst_info["url"]
            )
            chunk = self.download_object_part(key=src_key)
            while chunk:
                mp_info["stream_buffer"].write(chunk)
                mp_info["cur_size"] += len(chunk)
                mp_info["total_size"] += len(chunk)
                if stream_status:
                    print_running_status(
                        transferred_bytes=mp_info["total_size"],
                        start_time=mp_info["start_time"],
                        total_size=src_key_size,
                        msg_id=msg_id,
                    )

                mp_info["md5_sum"].update(chunk)
                mp_info["sha256_sum"].update(chunk)

                if mp_info["cur_size"] >= mp_info["mp_chunk_size"]:
                    self.upload_multipart_chunk(mp_info=mp_info)
                try:
                    chunk = self.download_object_part(key=src_key)
                except ClientError as exception:
                    raise Exception(f"Unable to read from {src_key.name}: {exception}")

            # write the remaining data
            self.upload_multipart_chunk(mp_info=mp_info)

            cur_time = time.perf_counter()
            size_info = get_nearest_file_size(mp_info["total_size"])
            base_transfer_rate = float(mp_info["total_size"]) / float(
                cur_time - mp_info["start_time"]
            )
            transfer_info = get_nearest_file_size(base_transfer_rate)
            cur_conv_size = float(mp_info["total_size"]) / float(size_info[0])
            cur_conv_rate = base_transfer_rate / float(transfer_info[0])
            self.log.info(
                "Complete, %7.02f %s : %6.02f %s per sec",
                cur_conv_size,
                size_info[1],
                cur_conv_rate,
                transfer_info[1],
            )

            self.complete_multipart_upload(mp_info=mp_info)
            self.log.info(
                "Upload complete, md5 = %s, %d bytes transferred",
                mp_info["md5_sum"].hexdigest(),
                mp_info["total_size"],
            )
        else:
            self.log.warning("Unable to get %s", src_info["url"])

        return {
            "md5_sum": str(mp_info["md5_sum"].hexdigest()),
            "sha256_sum": str(mp_info["sha256_sum"].hexdigest()),
            "bytes_transferred": mp_info["total_size"],
        }

    def load_file(self, url=None, stream_status=False):
        """Load an object into memory"""

        downloading = True
        file_data = bytearray()
        total_transfer = 0
        chunk = []

        # get the key from the bucket
        self.log.info("Getting %s", url)
        try:
            file_key = self.get_url(url=url)
        except Exception as exception:
            self.log.error("Unable to get %s: %s", url, exception)
        else:
            if file_key:
                while downloading:
                    try:
                        chunk = self.download_object_part(key=file_key["Body"])
                    except ClientError as exception:
                        downloading = False
                        self.log.error(
                            "Error %s reading bytes, got %d bytes",
                            str(exception),
                            len(chunk),
                        )
                        total_transfer = total_transfer + len(chunk)
                    else:
                        if len(chunk) < self.chunk_size:
                            downloading = False
                        total_transfer += len(chunk)
                        file_data.extend(chunk)
                        if stream_status:
                            sys.stdout.write(
                                "%6.02%%\r",
                                float(total_transfer) / float(file_key.size) * 100.0,
                            )
                            sys.stdout.flush()
            else:
                self.log.warn("Unable to find %s", url)

        self.log.info("%d lines received", len(str(file_data)))
        return file_data.decode()

    def parse_data_file(self, uri=None, data_type="tsv", custom_delimiter=None):
        """
        Processes loaded data as a tsv, csv, or
        json, returning it as a list of dicts
        """
        key_data = []
        header = None
        skipped_lines = 0
        delimiters = {"tsv": "\t", "csv": ",", "json": "", "other": ""}
        # other_delimiters = [' ', ',', ';']

        file_data = self.load_file(url=uri)

        if data_type not in delimiters:
            self.log.warning("Unable to process data type %s", data_type)
            self.log.warning("Valid data types:")
            self.log.warning("%s", list(delimiters.keys()))
        else:
            if data_type == "other":
                if custom_delimiter:
                    delimiter = custom_delimiter
                else:
                    raise Exception("With data_type 'other', a delimiter is needed")
            else:
                delimiter = delimiters[data_type]

            if data_type == "json":
                for line in file_data.split("\n"):
                    line_data = json.loads(line)
                    key_data.append(line_data)
            # load as tsv/csv, assuming the first row is the header
            # that provides keys for the dict
            else:
                for line in file_data.split("\n"):
                    if delimiter in line:
                        if line.strip("\n").strip():
                            if not header:
                                header = line.strip("\n").split(delimiter)
                            else:
                                line_data = dict(
                                    list(zip(header, line.strip("\n").split(delimiter)))
                                )
                                key_data.append(line_data)
                    else:
                        # ok, let's see if we can be smart here
                        # if not header:
                        #    remaining_chars = set([c for c in line if not c.isalnum()])
                        skipped_lines += 1

        self.log.info(
            "%d lines in file, %d processed", len(file_data.split("\n")), len(key_data)
        )
        return key_data

    def checksum_s3_key(self, url=None):
        """Get the checksum of an s3 object"""
        result = {"transfer_time": 0, "bytes_transferred": 0}
        md5sum = hashlib.md5()
        sha = hashlib.sha256()
        retries = 0
        result["start_time"] = time.time()
        running = False
        total_transfer = 0
        file_key_info = self.get_url(url=url)
        if file_key_info:
            file_key = file_key_info.get("Body", None)
            file_key_size = file_key_info.get("ContentLength", None)
            running = True
            # file_key.BufferSize = self.chunk_size
        else:
            self.log.warning("Unable to get %s ", url)

        while running:
            try:
                chunk = self.download_object_part(key=file_key)
            except ClientError as exception:
                if chunk:
                    if retries > 10:
                        self.log.error("Error reading: %s", exception)
                        break
                    else:
                        retries += 1
                        self.log.error("Error reading: %s retry %d", exception, retries)
                        time.sleep(2)
                else:
                    self.log.error(
                        "Error reading %s, got %d bytes", exception, len(chunk)
                    )
                    total_transfer += len(chunk)
                    md5sum.update(chunk)
                    sha.update(chunk)
                    retries = 0
            else:
                result["bytes_transferred"] += len(chunk)
                if (len(chunk) < self.chunk_size) and (
                    result["bytes_transferred"] >= file_key_size
                ):
                    running = False

                if file_key_size > 0:
                    sys.stdout.write(
                        "{:6.02f}%\r".format(
                            float(result["bytes_transferred"])
                            / float(file_key_size)
                            * 100.0
                        )
                    )
                else:
                    sys.stdout.write("0.00%%\r")
                sys.stdout.flush()
                md5sum.update(chunk)
                sha.update(chunk)
                retries = 0

        result["transfer_time"] = time.time() - result["start_time"]
        result["md5_sum"] = md5sum.hexdigest()
        result["sha256_sum"] = sha.hexdigest()
        return result
