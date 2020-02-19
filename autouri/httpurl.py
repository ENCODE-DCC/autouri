#!/usr/bin/env python3
"""HTTPURL class

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


def init_httpurl(
    http_chunk_size: Optional[int]=None):
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
    HTTP_CHUNK_SIZE: int = 256*1024

    _LOC_SUFFIX = '.url'
    _SCHEMES = ('http://', 'https://')

    def __init__(self, uri):
        super().__init__(uri, cls=self.__class__)

    @property
    def loc_dirname(self):
        """Dirname of URL is not very meaningful.
        Therefore, hash string of the whole URL string is used instead for localization.
        """
        return hashlib.md5(self._uri.encode('utf-8')).hexdigest()

    @property
    def basename(self):
        """Parses a URL to get a basename.
        This class can only work with a URL with an explicit basename
        which can be suffixed with extra parameters starting with ? only.
        """
        return super().basename.split('?', 1)[0]

    def get_metadata(self, skip_md5=False, make_md5_file=False):
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

        if md5 is None and not skip_md5:
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

        return False

    def _cp_from(self, src_uri):
        raise NotImplementedError('Read-only URI class.')

    @classmethod
    def get_http_chunk_size(cls) -> int:
        if cls.HTTP_CHUNK_SIZE is not None:
            if cls.HTTP_CHUNK_SIZE % (256*1024) > 0:
                raise ValueError('http_chunk_size must be a multiple of 256KB (256*1024) '
                                 'to be compatible with cloud storage APIs (GCS and AWS S3).')
        return cls.HTTP_CHUNK_SIZE
