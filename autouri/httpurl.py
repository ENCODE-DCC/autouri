#!/usr/bin/env python3
"""
Features:    
    - Wraps Python requests for HTTP URLs.
    - Can convert a bucket URI into a public URL by presigning
        - e.g. for genome browsers

Author: Jin Lee (leepc12@gmail.com)
"""

import requests
from binascii import hexlify
from base64 import b64decode
from datetime import datetime
from dateutil.parser import parse as parse_timestamp
from .autouri import AutoURI, AutoURIMetadata, logger


DEFAULT_HTTP_CHUNK_SIZE = 256*1024


def init_httpurl(
    http_chunk_size=None):
    """
    Helper function to initialize HTTPURL class constants
        loc_prefix:
            Inherited from AutoURI
    """
    if http_chunk_size is not None:
        HTTPURL.HTTP_CHUNK_SIZE = http_chunk_size


class HTTPURL(AutoURI):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from AutoURI class.
        HTTP_CHUNK_SIZE:
            Dict to replace path prefix with URL prefix.
            Useful to convert absolute path into URL on a web server.
    """
    HTTP_CHUNK_SIZE = DEFAULT_HTTP_CHUNK_SIZE

    _LOC_SUFFIX = '.url'
    _SCHEME = ('http://', 'https://')

    def __init__(self, uri):
        super().__init__(uri, cls=self.__class__)

    def get_dirname(no_scheme=False):
        """
        Args:
            no_scheme:
                md5-hash the whole URL instead of removing a scheme
        """
        dirname = os.path.dirname(self._uri)
        if no_scheme:
            dirname = hashlib.md5(self._uri.encode('utf-8')).hexdigest()
        return dirname

    def get_basename():
        """Parses a URL to get a basename.
        This class can only work with a URL with an explicit basename
        which can be suffixed with extra parameters starting with ? only.
        """
        basename = super().get_basename()
        return basename.split('?', 1)[0]

    def get_metadata(self, make_md5_file=False):
        ex, mt, sz, md5 = None, None, None, None, None

        # get header only (make it lower case)
        r = requests.get(
            url, stream=True, allow_redirects=True,
            headers=requests.utils.default_headers())
        r.raise_for_status()
        h = {k.lower(): v for k, v in r.headers}

        if 'content-md5' in h:
            md5 = binascii.hexlify(b64decode(h['content-md5'])).decode()
        elif 'etag' in h and len(h['etag']) == 32:
            # if ETag is md5 hexdigest
            md5 = h['etag']

        if 'content-length' in h:
            sz = int(h['content-length'])

        if 'last-modified' in h:
            utc_t = parse_timestamp(h['last-modified'])
            mt = (utc_t - datetime(1970, 1, 1)).total_seconds()

        if md5 is None:
            md5 = self.get_md5_from_file(make_md5_file=make_md5_file)

        return AutoURIMetadata(
            exists=ex,
            mtime=mt,
            size=sz,
            md5=md5)

    def read(self, byte=False):
        r = requests.get(
            url, stream=True, allow_redirects=True,
            headers=requests.utils.default_headers())
        r.raise_for_status()
        b = r.raw.read()
        if byte:
            return b
        else:
            return b.decode()

    def _write(self, s):
        raise NotImplementedError('Read-only URI class.')

    def _rm(self):
        raise NotImplementedError('Read-only URI class.')

    def _cp(self, dest_uri):
        """Copy from HTTPURL to 
            AbsPath
        """
        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, AbsPath):
            r = requests.get(
                url, stream=True, allow_redirects=True,
                headers=requests.utils.default_headers())
            r.raise_for_status()
            dest_uri.mkdir_dirname()
            with open(dest_uri.get_uri(), 'wb') as f:
                for chunk in r.iter_content(chunk_size=HTTPURL.HTTP_CHUNK_SIZE): 
                    if chunk:
                        f.write(chunk)
            return True

        return None

    def _cp_from(self, src_uri):
        raise NotImplementedError('Read-only URI class.')

    @staticmethod
    def get_localized_uri(src_uri):
        """Unlike localization for other URI types.
        Localization to URLs can be defined by one of the followings:
            1) presigning bucket URIs.
                S3URI, GCSURI -> HTTPURL
            2) mapping of AbsPath's prefix to URL.
                AbsPath -> HTTPURL
        """
        src_uri = AutoURI(src_uri)

        from .s3uri import S3URI
        from .gcsuri import GCSURI
        from .abspath import AbsPath

        if isinstance(src_uri, S3URI) and S3URI.can_presign():
            return AutoURI(src_uri.get_presigned_url()), False
        if isinstance(src_uri, GCSURI) and GCSURI.can_presign():
            return AutoURI(src_uri.get_presigned_url()), False
        if isinstance(src_uri, AbsPath) and AbsPath.can_map_to_url():
            return AutoURI(src_uri.get_mapped_url()), False

        raise NotImplementedError('Cannot localize on read-only URI class.')

    @classmethod
    def get_http_chunk_size(cls):
        if cls.HTTP_CHUNK_SIZE is not None:
            if cls.HTTP_CHUNK_SIZE % (256*1024) > 0:
                raise ValueError('http_chunk_size must be a multiple of 256KB (256*1024) '
                                 'to be compatible with cloud storage APIs (GCS and AWS S3).')
        return cls.HTTP_CHUNK_SIZE
