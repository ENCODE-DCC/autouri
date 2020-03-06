#!/usr/bin/env python3
"""
Bucket rules:
    Object versioning must be turned off
        https://cloud.google.com/storage/docs/object-versioning
"""
import os
import requests
import binascii
from base64 import b64decode
from contextlib import contextmanager
from datetime import (datetime, timedelta)
from dateutil.parser import parse as parse_timestamp
from dateutil.tz import tzutc
from filelock import BaseFileLock
from google.api_core.exceptions import NotFound, Forbidden, GatewayTimeout, ServiceUnavailable
# from requests.exceptions import ConnectionError
from google.cloud import storage
from google.cloud.storage import Blob
from google.oauth2.service_account import Credentials
from subprocess import (
    check_output, check_call, Popen, PIPE, CalledProcessError)
from typing import Tuple, Optional
from .autouri import URIBase, URIMetadata, AutoURI, logger


def init_gcsuri(
    loc_prefix: Optional[str]=None,
    private_key_file: Optional[str]=None,
    sec_duration_presigned_url: Optional[int]=None):
    """Helper function to initialize GCSURI class constants
        loc_prefix:
            Inherited from URIBase
    """
    if loc_prefix is not None:
        GCSURI.LOC_PREFIX = loc_prefix
    if private_key_file is not None:
        GCSURI.PRIVATE_KEY_FILE = private_key_file
    if sec_duration_presigned_url is not None:
        GCSURI.SEC_DURATION_PRESIGNED_URL = sec_duration_presigned_url


class GCSURILock(BaseFileLock):
    def __init__(
        self, lock_file, timeout=900, poll_interval=0.1, no_lock=False):
        super().__init__(lock_file, timeout=timeout)
        self._poll_interval = poll_interval

    def acquire(self, timeout=None, poll_intervall=5.0):
        """Use self._poll_interval instead of poll_intervall in args
        """
        super().acquire(timeout=timeout, poll_intervall=self._poll_interval)

    def _acquire(self):
        u = GCSURI(self._lock_file)
        blob = u.get_blob(new=True)
        if blob is not None:
            try:
                blob.upload_from_string('')
                blob.temporary_hold = True
                blob.patch()
                self._lock_file_fd = id(self)
            except (Forbidden, GatewayTimeout, NotFound, ServiceUnavailable):
                pass
        return None

    def _release(self):
        u = GCSURI(self._lock_file)
        blob = u.get_blob()
        blob.temporary_hold = False
        try:
            # u.rm(no_lock=True)
            blob.patch()
            blob.delete()
            self._lock_file_fd = None
        except (NotFound,):
            pass
        return None


class GCSURI(URIBase):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from URIBase class.
        PRIVATE_KEY_FILE:
            Path for private key file used to get presigned URLs
        SEC_DURATION_PRESIGNED_URL:
            Duration for presigned URLs
    Protected class constants:
        _CACHED_GCS_CLIENT_PER_THREAD:
            Per-thread GCS client object is required since 
            GCS client is not thread-safe.
        _CACHED_PRESIGNED_URLS:
            Can use cached presigned URLs.
    """
    PRIVATE_KEY_FILE: str = None
    SEC_DURATION_PRESIGNED_URL: int = 4233600

    _CACHED_GCS_CLIENT_PER_THREAD = {}
    _CACHED_PRESIGNED_URLS = {}

    _LOC_SUFFIX = '.gcs'
    _SCHEMES = ('gs://',)

    def __init__(self, uri, thread_id=-1):
        super().__init__(uri, thread_id=thread_id)

    def get_lock(self, no_lock=False, timeout=None, poll_interval=None):
        if no_lock:
            return contextmanager(lambda: (yield))()
        if timeout is None:
            timeout = GCSURI.LOCK_TIMEOUT
        if poll_interval is None:
            poll_interval = GCSURI.LOCK_POLL_INTERVAL
        return GCSURILock(
            self._uri + AutoURI.LOCK_FILE_EXT,
            timeout=timeout,
            poll_interval=poll_interval)

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        ex, mt, sz, md5 = False, None, None, None

        try:
            b = self.get_blob()
            if b is not None:
                # make keys lower-case
                h = {k.lower(): v for k, v in b._properties.items()}
                ex = True

                md5_raw = None
                if 'md5hash' in h:
                    md5_raw = h['md5hash']
                elif 'etag' in h:
                    md5_raw = h['etag']
                if md5_raw is not None:
                    md5_raw = md5_raw.strip('"\'')
                    if len(md5_raw) == 32:
                        md5 = md5_raw
                    else:
                        md5 = binascii.hexlify(b64decode(md5_raw)).decode()

                if 'size' in h:
                    sz = int(h['size'])

                if 'updated' in h:
                    utc_t = parse_timestamp(h['updated'])
                elif 'timecreated' in h:
                    utc_t = parse_timestamp(h['timecreated'])
                else:
                    utc_t = None
                if utc_t is not None:              
                    utc_epoch = datetime(1970, 1, 1, tzinfo=tzutc())      
                    mt = (utc_t - utc_epoch).total_seconds()

                if md5 is None and not skip_md5:
                    md5 = self.md5_from_file
                    # make_md5_file is ignored for GCSURI
                        

        except Exception as e:
            pass

        return URIMetadata(
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
        from .s3uri import S3URI
        from .abspath import AbsPath

        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, (GCSURI, AbsPath)):            
            src_blob = self.get_blob()

            if isinstance(dest_uri, GCSURI):
                dest_bucket, dest_path = dest_uri.get_bucket_path()
                src_bucket.copy_blob(src_blob, dest_bucket, dest_path)
                return True

            elif isinstance(dest_uri, AbsPath):
                dest_uri.mkdir_dirname()
                src_blob.download_to_filename(dest_uri._uri)
                return True

        elif isinstance(dest_uri, S3URI):
            rc = check_call(['gsutil', '-q', 'cp', self._uri, dest_uri._uri])
            return rc == 0
        return False

    def _cp_from(self, src_uri):
        """Copy to GCSURI from
            S3URI
            AbsPath
            HTTPURL
        """
        from .s3uri import S3URI
        from .abspath import AbsPath

        src_uri = AutoURI(src_uri)

        if isinstance(src_uri, AbsPath):
            blob = self.get_blob(new=True)
            blob.upload_from_filename(src_uri._uri)
            return True

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
                blob.upload_from_file(fp)
            return True
        return False

    def get_blob(self, new=False) -> Blob:
        """GCS client has a bug that shows an outdated version of a file
        when using Blob() without update().

        For read-only functions (e.g. read()), need to directly call
        cl.get_bucket(bucket).get_blob(path) instead of using Blob() class.
        """
        bucket, path = self.get_bucket_path()
        cl = GCSURI.get_gcs_client(self._thread_id)
        if new:
            return Blob(name=path, bucket=cl.get_bucket(bucket))
        else:
            return cl.get_bucket(bucket).get_blob(path)

    def get_bucket_path(self) -> Tuple[str, str]:
        """Returns a tuple of URI's S3 bucket and path.
        """
        bucket, path = self.uri_wo_scheme.split(GCSURI.get_path_sep(), 1)
        return bucket, path        

    def get_presigned_url(self, sec_duration=None, use_cached=False) -> str:
        """
        Args:
            sec_duration: Duration in seconds. This is ignored if use_cached is on.
            use_cached: Use a cached URL. 
        """
        cache = GCSSURI._CACHED_PRESIGNED_URLS
        if use_cached:
            if cache is not None and self._uri in cache:
                return cache[self._uri]
        blob = self.get_blob()
        private_key_file = os.path.expanduser(GCSURI.PRIVATE_KEY_FILE)
        if not os.path.exists(private_key_file):
            raise Exception('GCS private key file not found')
        credentials = Credentials.from_service_account_file(private_key_file)
        duration = sec_duration if sec_duration is not None else GSSURI.SEC_DURATION_PRESIGNED_URL        
        url = blob.generate_signed_url(
            expiration=timedelta(seconds=duration),
            credentials=credentials)
        cache[self._uri] = url
        return url

    @staticmethod
    def get_gcs_client(thread_id=-1) -> storage.Client:
        if thread_id in GCSURI._CACHED_GCS_CLIENT_PER_THREAD:
            return GCSURI._CACHED_GCS_CLIENT_PER_THREAD[thread_id]
        else:
            cl = storage.Client()
            GCSURI._CACHED_GCS_CLIENT_PER_THREAD[thread_id] = cl
            return cl
