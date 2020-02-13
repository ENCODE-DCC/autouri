#!/usr/bin/env python3
"""
Author: Jin Lee (leepc12@gmail.com)
"""

from autouri.autouri import AutoURI, AutoURIMetadata, logger
import os
import requests
from google.cloud import storage
from google.cloud.storage import Blob
from google.oauth2.service_account import Credentials
from subprocess import (
    check_output, check_call, Popen, PIPE, CalledProcessError)
from binascii import hexlify
from base64 import b64decode
from datetime import (datetime, timedelta)
from dateutil.parser import parse as parse_timestamp


def init_gcsuri(
    loc_prefix=None,
    private_key_file=None,
    sec_duration_presigned_url=None):
    """Helper for initializing GCSURI class constants

    Args:
        loc_prefix:
            prefix (root path) for localization
        private_key_file:
            private key file in JSON/P12 file format for presigned URLs
        sec_duration_presigned_url:
            duratino for presigned URLs
    """
    if loc_prefix is not None:
        GCSURI.LOC_PREFIX = loc_prefix
    if private_key_file is not None:
        GCSURI.PRIVATE_KEY_FILE = private_key_file
    if sec_duration_presigned_url is not None:
        GCSURI.SEC_DURATION_PRESIGNED_URL = sec_duration_presigned_url


class GCSURI(AutoURI):
    # protected constants
    _LOC_SUFFIX = '.gcs'
    _SCHEME = 'gs://'
    # original
    _GCS_CLIENT = None
    _CACHED_PRESIGNED_URLS = {}
    # protected constants
    PRIVATE_KEY_FILE = None
    SEC_DURATION_PRESIGNED_URL = 4233600

    def __init__(self, uri):
        super().__init__(uri, cls=self.__class__)

    def get_metadata(self, make_md5_file=False):
        ex, mt, sz, md5 = None, None, None, None
        blob = self.get_blob()

        try:
            m = cl.head_object(Bucket=bucket, Key=path)
        except ClientError:
            ex = False
            return
        # make it lowercase
        h = {k.lower(): v for k, v in m}

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
        blob = self.get_blob()
        b = blob.download_as_string()
        if byte:
            return b
        return b.decode()

    def _write(self, s):
        blob = self.get_blob(new=True)
        # if not isinstance(s, str):
        #     s = s.decode()
        blob.upload_from_string(s)
        return

    def _rm(self):
        blob = self.get_blob()
        blob.delete()
        return

    def _cp(self, dest_uri):
        """Copy from GCSURI to 
            GCSURI
            S3URI
            AbsPath
        """
        from autouri.s3uri import S3URI
        from autouri.abspath import AbsPath

        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, (GCSURI, AbsPath)):            
            src_bucket, src_path = self.get_bucket_path()
            src_blob = src_bucket.get_blob(src_path)

            if isinstance(dest_uri, GCSURI):
                dest_bucket, dest_path = dest_uri.get_bucket_path()
                return src_bucket.copy_blob(src_blob, dest_bucket, dest_path) is not None

            elif isinstance(dest_uri, AbsPath):
                dest_uri.mkdir_dirname()
                return src_blob.download_to_filename(dest_uri.get_uri()) is not None

        elif isinstance(dest_uri, S3URI):
            rc = check_call(['gsutil', '-q', 'cp', self._uri, dest_uri._uri])
            return rc == 0

        return dest_uri._cp_from(self)

    def _cp_from(self, src_uri):
        """Copy to GCSURI from
            S3URI
            AbsPath
            HTTPURL
        """
        from autouri.s3uri import S3URI
        from autouri.abspath import AbsPath

        src_uri = AutoURI(src_uri)

        if isinstance(src_uri, AbsPath):
            blob = self.get_blob(new=True)
            return blob.upload_from_filename(src_uri.get_uri()) is not None

        elif isinstance(src_uri, S3URI):
            rc = check_call(['gsutil', '-q', 'cp', src_uri._uri, self._uri])
            return rc == 0

        elif isinstance(src_uri, HTTPURL):
            r = requests.get(
                src_uri._uri, stream=True, allow_redirects=True,
                headers=requests.utils.default_headers())
            r.raise_for_status()
            b = BytesIO()
            with open(b, 'wb') as fp:
                for chunk in r.iter_content(HTTPURL.get_chunk_size()):
                    fp.write(chunk)
            blob = self.get_blob(new=True)
            with open(b, 'rb') as fp:
                return blob.upload_from_file(fp) is not None

        raise NotImplementedError

    def get_blob(self, new=False):
        bucket, path = self.get_bucket_path()
        if new:
            return Blob(path, bucket)
        else:
            cl = GCSURI.get_gcs_client()
            return cl.get_bucket(bucket).get_blob(path)

    def get_bucket_path(self):
        """Returns a tuple of URI's S3 bucket and path.
        """
        uri_wo_scheme = self._uri.replace(GCSURI.get_scheme(), '', 1)
        bucket, path = uri_wo_scheme.split(GCSURI.get_path_sep(), 1)
        return bucket, path

    def get_presigned_url(self, use_cached=False):
        cache = GCSSURI._CACHED_PRESIGNED_URLS
        if use_cached:
            if cache is not None and self._uri in cache:
                return cache[self._uri]
        blob = self.get_blob()
        private_key_file = os.path.expanduser(GCSURI.PRIVATE_KEY_FILE)
        if not os.path.exists(private_key_file):
            raise Exception('GCS private key file not found')
        credentials = Credentials.from_service_account_file(private_key_file)
        url = blob.generate_signed_url(
            expiration=timedelta(seconds=GCSURI._sec_duration_presigned_url),
            credentials=credentials)
        cache[self._uri] = url
        return url

    @classmethod
    def get_gcs_client(cls):
        if cls._GCS_CLIENT is None:
            cls._GCS_CLIENT = storage.Client()
        return cls._GCS_CLIENT
