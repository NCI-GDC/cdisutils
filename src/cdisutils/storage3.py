"""
cdisutils.storage3
----------------------------------

Utilities for working with object stores using boto3

"""

import hashlib
import _hashlib
import io
import json
import logging
import os
import re
import sys
import time
from urllib.parse import urlparse
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef, HeadObjectOutputTypeDef, BucketTypeDef

from typing import TypedDict
import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

logger = logging.getLogger(__name__)

# magic number here for multipart chunk size, change with care
DEFAULT_MP_CHUNK_SIZE = 1073741824  # 1GiB

# 16 MiB is used because it was tested for performance, if
# speed issues are seen, this is a good value to try and
# tweak. Probably good to keep it powers of 2, and an
# even interval of the mp_chunk_size above
DEFAULT_DOWNLOAD_CHUNK_SIZE = 16777216  # 16MiB

class S3Info(TypedDict):
    url: str
    s3_loc: str
    bucket_name: str
    key_name: str
        
class MultipartInfoDict(TypedDict):
    dst_info: S3Info
    src_info: S3Info
    stream_buffer: io.BytesIO
    mp_chunk_size: int
    download_chunk_size: int
    cur_size: int
    chunk_index: int
    total_size: int
    manifest: dict
    md5_sum: _hashlib.HASH
    sha256_sum: _hashlib.HASH
    start_time: float
    mp_id: str
        
class MultiPartCopyDict(TypedDict):
    md5_sum: str
    sha256_sum: str
    bytes_transferred: int

class ChecksumResultDict(TypedDict):
    transfer_time: float
    bytes_transferred: int
    start_time: float
    md5_sum: str
    sha256_sum: str

def get_nearest_file_size(size: int | float) -> tuple[int, str]:
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


def print_running_status(transferred_bytes: int, start_time: float, total_size: int, msg_id: int = 0):
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


def load_creds() -> dict:
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
                        s3_creds[s3_key][s3_key_mapping[key]].append(str(os.environ[env]))
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

    log = logger

    def __init__(self, config: dict | None = None, lazy: bool = False, host_aliases: dict | None = None, stream_status: bool = False):
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

        self.conns: dict[str, S3Client] = {}
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

    def __getitem__(self, host: str) -> S3Client:
        """Internal call for getting a connection"""
        return self.get_connection(host)

    def harmonize_host(self, host: str) -> str:
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

    def get_connection(self, host: str) -> S3Client:
        """Get an s3 connection handle"""
        return self.conns[self.harmonize_host(host)]

    def connect(self) -> None:
        """Connect to all hosts in config"""
        for host in self.config:
            self.conns[host] = self.new_connection_to(host)

    def new_connection_to(self, host: str) -> S3Client:
        """Connect to a given host"""
        if "https" not in host:
            s3_url = f"https://{host}"
        else:
            s3_url = host
        # TODO: Allow the location to be passed in via config
        cur_dict = dict(self.config[host])
        del cur_dict["host"]
        if cur_dict.get("verify") == "false":
            print("Skipping verify")
            cur_dict.pop("verify")
            conn = boto3.client("s3", "us-east-1", endpoint_url=s3_url, verify=False, **cur_dict)
        else:
            print("No verify found, using dict")
            conn = boto3.client("s3", "us-east-1", endpoint_url=s3_url, **cur_dict)

        return conn

    def parse_url(self, url: str) -> S3Info:
        """Parse a URL into a dictionary with component parts"""
        parts = urlparse(url)
        for key in self.config:
            if key in parts.netloc:
                s3_loc = key
                break
        bucket_name = str(parts.path).split("/")[1]
        key_name = "/".join(str(parts.path).split("/")[2:])
        s3_info = S3Info(
            url=url, s3_loc=s3_loc, bucket_name=bucket_name, key_name=key_name)

        return s3_info

    def get_url(self, url: str) -> GetObjectOutputTypeDef:
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

    def head_url(self, url: str) -> HeadObjectOutputTypeDef | None:
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

    def list_buckets(self, host: str | None = None) -> list[BucketTypeDef]:
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

    def create_multipart_upload(self, src_url: str, dst_url: str) -> MultipartInfoDict:
        """
        Create a multipart upload, holding session info in a dict

        TODO: Hold this in the class vars
        """
        dst_info: S3Info = self.parse_url(url=dst_url)
        src_info: S3Info = self.parse_url(url=src_url)
        
        mp_info = self.conns[dst_info["s3_loc"]].create_multipart_upload(
            Bucket=dst_info["bucket_name"],
            Key=dst_info["key_name"],
        )
        try:
            upload_id = mp_info["UploadId"]
        except KeyError:
            raise Exception("Unable to get valid ID for multipart upload: %s" % mp_info)
        
        multipart_info = MultipartInfoDict(
            dst_info=dst_info,
            src_info=src_info,
            stream_buffer=io.BytesIO(),
            mp_chunk_size=self.mp_chunk_size,
            download_chunk_size=self.chunk_size,
            cur_size=0,
            chunk_index=1,
            total_size=0,
            manifest={"Parts": []},
            md5_sum=hashlib.md5(usedforsecurity=False),
            sha256_sum=hashlib.sha256(),
            start_time=time.perf_counter(),
            mp_id=upload_id,
        )

        return multipart_info

    def complete_multipart_upload(self, mp_info: MultipartInfoDict) -> None:
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
                "Unable to complete mulitpart {}: {}".format(mp_info["mp_id"], exception)
            )

    def upload_multipart_chunk(self, mp_info: MultipartInfoDict) -> None:
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

    def download_object_part(self, key: StreamingBody) -> bytes:
        """Downloads a chunk of an object"""
        return key.read(amt=self.chunk_size)

    def copy_multipart_file(self, src_info: S3Info | str, dst_info: S3Info | str, stream_status: bool = True, msg_id: int = 0) -> MultiPartCopyDict:
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
            src_key: StreamingBody = src_key_info["Body"]
            src_key_size = src_key_info["ContentLength"]
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
                chunk = self.download_object_part(key=src_key)

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

        return MultiPartCopyDict(**{
            "md5_sum": str(mp_info["md5_sum"].hexdigest()),
            "sha256_sum": str(mp_info["sha256_sum"].hexdigest()),
            "bytes_transferred": mp_info["total_size"],
        })

    def load_file(self, url: str, stream_status: bool = False) -> str:
        """Load an object into memory"""

        downloading = True
        file_data = bytearray()
        total_transfer = 0

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
                self.log.warning("Unable to find %s", url)

        self.log.info("%d lines received", len(str(file_data)))
        return file_data.decode()

    def parse_data_file(self, uri: str, data_type: str = "tsv", custom_delimiter: str | None = None) -> list:
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

        self.log.info("%d lines in file, %d processed", len(file_data.split("\n")), len(key_data))
        return key_data

    def checksum_s3_key(self, url: str) -> ChecksumResultDict:
        """Get the checksum of an s3 object"""
        transfer_time = 0
        bytes_transferred = 0
        start_time = time.time()
        md5sum = hashlib.md5(usedforsecurity=False)
        sha = hashlib.sha256()
        retries = 0
        running = False
        total_transfer = 0
        file_key_info = self.get_url(url=url)
        if file_key_info:
            file_key = file_key_info["Body"]
            file_key_size = file_key_info["ContentLength"]
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
                    self.log.error("Error reading %s, got %d bytes", exception, len(chunk))
                    total_transfer += len(chunk)
                    md5sum.update(chunk)
                    sha.update(chunk)
                    retries = 0
            else:
                bytes_transferred += len(chunk)
                if (len(chunk) < self.chunk_size) and (
                    bytes_transferred >= file_key_size
                ):
                    running = False

                if file_key_size > 0:
                    sys.stdout.write(
                        "{:6.02f}%\r".format(
                            float(bytes_transferred]) / float(file_key_size) * 100.0
                        )
                    )
                else:
                    sys.stdout.write("0.00%%\r")
                sys.stdout.flush()
                md5sum.update(chunk)
                sha.update(chunk)
                retries = 0

        transfer_time = time.time() - start_time
        return ChecksumResultDict(
            transfer_time=transfer_time,
            md5_sum=md5sum.hexdigest(),
            sha256_sum = sha.hexdigest(),
            bytes_transferred=bytes_transferred,
            start_time=start_time,
        )
