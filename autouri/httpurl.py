#!/usr/bin/env python3
import binascii
import hashlib
import requests
from base64 import b64decode
from datetime import datetime
from dateutil.parser import parse as parse_timestamp
from dateutil.tz import tzutc
from typing import Optional
from .autouri import URIBase, URIMetadata, AutoURI, logger


def init_httpurl(
    http_chunk_size: Optional[int]=None):
    """
    Helper function to initialize HTTPURL class constants
        loc_prefix:
            Inherited from URIBase
    """
    if http_chunk_size is not None:
        HTTPURL.HTTP_CHUNK_SIZE = http_chunk_size


class ReadOnlyStorageError(Exception):
    pass


class HTTPURL(URIBase):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from URIBase class.
        HTTP_CHUNK_SIZE:
            Dict to replace path prefix with URL prefix.
            Useful to convert absolute path into URL on a web server.
    """
    HTTP_CHUNK_SIZE: int = 256*1024

    _LOC_SUFFIX = '.url'
    _SCHEMES = ('http://', 'https://')

    def __init__(self, uri):
        super().__init__(uri)

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
        ex, mt, sz, md5 = False, None, None, None
        # get header only
        r = requests.get(
            self._uri, stream=True, allow_redirects=True,
            headers=requests.utils.default_headers())
        try:
            r.raise_for_status()
            # make keys lower-case
            h = {k.lower(): v for k, v in r.headers.items()}
            ex = True

            md5_raw = None
            if 'content-md5' in h:
                md5_raw = h['content-md5']
            elif 'x-goog-hash' in h:
                hashes = h['x-goog-hash'].strip().split(',')
                for hs in hashes:
                    if hs.strip().startswith('md5='):
                        md5_raw = hs.strip().replace('md5=', '', 1)
            if md5_raw is None and 'etag' in h:
                md5_raw = h['etag']
            if md5_raw is not None:
                md5_raw = md5_raw.strip('"\'')
                if len(md5_raw) == 32:
                    md5 = md5_raw
                else:
                    md5 = binascii.hexlify(b64decode(md5_raw)).decode()

            if 'content-length' in h:
                sz = int(h['content-length'])
            elif 'x-goog-stored-content-length' in h:
                sz = int(h['x-goog-stored-content-length'])

            if 'last-modified' in h:
                utc_t = parse_timestamp(h['last-modified'])
            else:
                utc_t = None
            if utc_t is not None:              
                utc_epoch = datetime(1970, 1, 1, tzinfo=tzutc())      
                mt = (utc_t - utc_epoch).total_seconds()

            if md5 is None and not skip_md5:
                md5 = self.md5_from_file

        except Exception as e:
            print(e)
            pass

        return URIMetadata(
            exists=ex,
            mtime=mt,
            size=sz,
            md5=md5)

    def read(self, byte=False):
        r = requests.get(
            self._uri, stream=True, allow_redirects=True,
            headers=requests.utils.default_headers())
        r.raise_for_status()
        b = r.raw.read()
        if byte:
            return b
        else:
            return b.decode()

    def _write(self, s):
        raise ReadOnlyStorageError('Read-only URI class.')

    def _rm(self):
        raise ReadOnlyStorageError('Read-only URI class.')

    def _cp(self, dest_uri):
        """Copy from HTTPURL to 
            AbsPath
        """
        from autouri.abspath import AbsPath
        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, AbsPath):
            r = requests.get(
                self._uri, stream=True, allow_redirects=True,
                headers=requests.utils.default_headers())
            r.raise_for_status()
            dest_uri.mkdir_dirname()
            with open(dest_uri._uri, 'wb') as f:
                for chunk in r.iter_content(chunk_size=HTTPURL.HTTP_CHUNK_SIZE): 
                    if chunk:
                        f.write(chunk)
            return True
        return False

    def _cp_from(self, src_uri):
        raise ReadOnlyStorageError('Read-only URI class.')

    @staticmethod
    def get_http_chunk_size() -> int:
        if HTTPURL.HTTP_CHUNK_SIZE % (256*1024) > 0:
            raise ValueError('http_chunk_size must be a multiple of 256 KB (256*1024 B) '
                             'to be compatible with cloud storage APIs (GCS and AWS S3).')
        return HTTPURL.HTTP_CHUNK_SIZE
