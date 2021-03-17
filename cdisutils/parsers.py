import re

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


class S3URLParser:
    """Adapter for urlparse to facilitate working with S3 urls"""
    def __init__(self, s3_url):
        self._s3_url = s3_url
        self.scheme = None
        self.netloc = None
        self.bucket = None
        self.key = None
        self._parse()

    def __repr__(self):
        return "S3URLParser(scheme='{}', netloc='{}', bucket='{}', key='{}')".format(
            self.scheme,
            self.netloc,
            self.bucket,
            self.key,
        )

    def _parse(self):
        """Parsing options for new and old style s3 urls.

        All new buckets created on aws will use the new style of url.

        Old: s3://url/bucket_name/key/name/at/the/end
        New: https://bucket.url/key/name/at/the/end
        """

        # (scheme, bucket, netloc, path)
        new_style_match = re.match(
            r"^(s3|https?):\/\/([^\.\s]+)\.(s3.*\.amazonaws\.com)\/(.+)$",
            self._s3_url,
        )
        if new_style_match:
            # New style url (bucket in netloc).
            new_style_groups = new_style_match.groups()

            self.scheme = new_style_groups[0]
            self.netloc = new_style_groups[2]
            self.bucket = new_style_groups[1]
            self.key = new_style_groups[3]
        else:
            # Old style url (bucket in path).
            parse_object = urlparse(self._s3_url)

            self.scheme = parse_object.scheme
            self.netloc = parse_object.netloc
            # eg ['', 'bucket', 'key/name/goes/here']
            path_parts = parse_object.path.split('/', 2)

            # Do not add preceding forward slashes.
            self.bucket = path_parts[1]
            self.key = path_parts[2]

    def get_url(self, new_style=False):
        if new_style:
            return '{scheme}://{bucket}.{netloc}/{key}'.format(
                scheme=self.scheme,
                bucket=self.bucket,
                netloc=self.netloc,
                key=self.key,
            )

        return '{scheme}://{netloc}/{bucket}/{key}'.format(
            scheme=self.scheme,
            netloc=self.netloc,
            bucket=self.bucket,
            key=self.key,
        )
