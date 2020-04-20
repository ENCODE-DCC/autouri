"""GCS Bucket configuration:
    Object versioning must be turned off
        Check it with "gsutil versioning get gs://BUCKET-NAME"
        https://cloud.google.com/storage/docs/object-versioning
"""
import logging
import os
import requests
import time
from datetime import timedelta
from filelock import BaseFileLock
from google.api_core.exceptions import NotFound, Forbidden, GatewayTimeout, ServiceUnavailable
from google.cloud import storage
from google.cloud.storage import Blob
from google.oauth2.service_account import Credentials
from subprocess import check_call, PIPE, CalledProcessError
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Tuple, Optional
from urllib3.exceptions import HTTPError
from .autouri import URIBase, AutoURI, logger
from .metadata import URIMetadata, get_seconds_from_epoch, parse_md5_str


logger = logging.getLogger(__name__)


class GCSURILock(BaseFileLock):
    """Slow but stable locking with using GCS temporary_hold
    """
    def __init__(
        self, lock_file, thread_id=-1, timeout=900, poll_interval=10.0, no_lock=False):
        super().__init__(lock_file, timeout=timeout)
        self._poll_interval = poll_interval
        self._thread_id = thread_id

    def acquire(self, timeout=None, poll_intervall=5.0):
        """Use self._poll_interval instead of poll_intervall in args
        """
        super().acquire(timeout=timeout, poll_intervall=self._poll_interval)

    def _acquire(self):
        u = GCSURI(self._lock_file, thread_id=self._thread_id)
        blob, bucket_obj = u.get_blob(new=True)
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
        u = GCSURI(self._lock_file, thread_id=self._thread_id)
        blob, _ = u.get_blob()
        if blob is not None:
            blob.temporary_hold = False
            try:
                blob.patch()
                blob.delete()
                self._lock_file_fd = None
            except (NotFound,):
                pass
        return None


class GCSURI(URIBase):
    """
    Class constants:
        LOC_PREFIX (inherited):
            Path prefix for localization. Inherited from URIBase class.
        PRIVATE_KEY_FILE:
            Path for private key file used to get presigned URLs
        DURATION_PRESIGNED_URL:
            Duration for presigned URLs in seconds.
        RETRY_BUCKET:
            Number of retrial to access a bucket.
        RETRY_BUCKET_DELAY:
            Delay for each retrial in seconds.
        USE_GSUTIL_FOR_S3 (experimental):
            This is only for direct transfer between S3 and GCS buckets.
            WARNING:
                gsutil must be configured correctly to have all
                AWS credentials in ~/.boto file.
                Run "aws configure" first and then
                run "gsutil config" to generate corrensponding ~/.boto file.

    Protected class constants:
        _CACHED_GCS_CLIENT_PER_THREAD:
            Per-thread GCS client object is required since 
            GCS client is not thread-safe.
        _CACHED_PRESIGNED_URLS:
            Can use cached presigned URLs.
        _GCS_PUBLIC_URL_FORMAT:
            End point for a bucket with public access + key path
    """
    PRIVATE_KEY_FILE: str = ''
    DURATION_PRESIGNED_URL: int = 4233600

    RETRY_BUCKET: int = 3
    RETRY_BUCKET_DELAY: int = 1
    USE_GSUTIL_FOR_S3: bool = False

    _CACHED_GCS_CLIENT_PER_THREAD = {}
    _CACHED_PRESIGNED_URLS = {}
    _GCS_PUBLIC_URL_FORMAT = 'http://storage.googleapis.com/{bucket}/{path}'

    _LOC_SUFFIX = '.gcs'
    _SCHEMES = ('gs://',)

    def __init__(self, uri, thread_id=-1):
        super().__init__(uri, thread_id=thread_id)

    def _get_lock(self, timeout=None, poll_interval=None):
        if timeout is None:
            timeout = GCSURI.LOCK_TIMEOUT
        if poll_interval is None:
            poll_interval = GCSURI.LOCK_POLL_INTERVAL
        return GCSURILock(
            self._uri + GCSURI.LOCK_FILE_EXT,
            thread_id=self._thread_id,
            timeout=timeout,
            poll_interval=poll_interval)

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        ex, mt, sz, md5 = False, None, None, None

        try:
            b, _ = self.get_blob()
            if b is not None:
                # make keys lower-case
                h = {k.lower(): v for k, v in b._properties.items()}
                ex = True

                if not skip_md5:
                    if 'md5hash' in h:
                        md5 = parse_md5_str(h['md5hash'])
                    elif 'etag' in h:
                        md5 = parse_md5_str(h['etag'])
                    if md5 is None:
                        # make_md5_file is ignored for GCSURI
                        md5 = self.md5_from_file

                if 'size' in h:
                    sz = int(h['size'])

                if 'updated' in h:
                    mt = get_seconds_from_epoch(h['updated'])
                elif 'timecreated' in h:
                    mt = get_seconds_from_epoch(h['timecreated'])

        except Exception as e:
            pass

        return URIMetadata(
            exists=ex,
            mtime=mt,
            size=sz,
            md5=md5)

    def read(self, byte=False):
        blob, _ = self.get_blob()
        b = blob.download_as_string()
        if byte:
            return b
        return b.decode()

    def _write(self, s):
        blob, _ = self.get_blob(new=True)
        blob.upload_from_string(s)
        # blob.update()
        return

    def _rm(self):
        blob, _ = self.get_blob()
        blob.delete()
        return

    def _cp(self, dest_uri):
        """Copy from GCSURI to 
            GCSURI
            S3URI: can use gsutil for direct transfer if USE_GSUTIL_FOR_S3 == True
            AbsPath
        """
        from .s3uri import S3URI
        from .abspath import AbsPath

        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, (GCSURI, AbsPath)):            
            src_blob, src_bucket = self.get_blob()

            if src_blob is None:
                raise ValueError('Blob does not exist for {f}'.format(f=self._uri))

            if isinstance(dest_uri, GCSURI):
                _, dest_path = dest_uri.get_bucket_path()
                _, dest_bucket = dest_uri.get_blob()
                src_bucket.copy_blob(src_blob, dest_bucket, dest_path)
                return True

            elif isinstance(dest_uri, AbsPath):
                dest_uri.mkdir_dirname()
                # mtime is not updated without update().
                src_blob.update()                
                src_blob.download_to_filename(dest_uri._uri)
                return True

        elif isinstance(dest_uri, S3URI):
            if GCSURI.USE_GSUTIL_FOR_S3:
                rc = check_call(['gsutil', '-q', 'cp', self._uri, dest_uri._uri])
                return rc == 0
            else:
                # use local temporary file instead
                with TemporaryDirectory() as tmp_d:
                    dest_uri_local = AbsPath(os.path.join(tmp_d, self.basename))
                    # lockless copy
                    self.cp(dest_uri=dest_uri_local, no_lock=True, no_checksum=True)
                    dest_uri_local.cp(dest_uri=dest_uri, no_lock=True, no_checksum=True)
                return True

        return False

    def _cp_from(self, src_uri):
        """Copy to GCSURI from
            S3URI: can use gsutil for direct transfer if USE_GSUTIL_FOR_S3 == True
            AbsPath
            HTTPURL
        """
        from .s3uri import S3URI
        from .abspath import AbsPath
        from .httpurl import HTTPURL

        src_uri = AutoURI(src_uri)

        if isinstance(src_uri, AbsPath):
            blob, _ = self.get_blob(new=True)
            blob.upload_from_filename(src_uri._uri)
            return True

        elif isinstance(src_uri, S3URI):
            if GCSURI.USE_GSUTIL_FOR_S3:
                rc = check_call(['gsutil', '-q', 'cp', src_uri._uri, self._uri])
                return rc == 0
            else:
                # use local temporary file instead
                with TemporaryDirectory() as tmp_d:
                    dest_uri_local = AbsPath(os.path.join(tmp_d, self.basename))
                    src_uri.cp(dest_uri=dest_uri_local, no_lock=True, no_checksum=True)
                    dest_uri_local.cp(dest_uri=self, no_lock=True, no_checksum=True)
                return True

        elif isinstance(src_uri, HTTPURL):
            r = requests.get(
                src_uri._uri, stream=True, allow_redirects=True,
                headers=requests.utils.default_headers())
            r.raise_for_status()
            with NamedTemporaryFile() as fp:
                for chunk in r.iter_content(HTTPURL.get_http_chunk_size()):
                    fp.write(chunk)
                fp.seek(0)
                blob, _ = self.get_blob(new=True)
                blob.upload_from_file(fp)
            return True
        return False

    def get_blob(self, new=False) -> Blob:
        """GCS client() has a bug that shows an outdated version of a file
        when using Blob() without update().
        For read-only functions (e.g. read()), need to directly call
        cl.get_bucket(bucket).get_blob(path) instead of using Blob() class.

        Also, GCS client() is not thread-safe and it fails for a variety of reasons.
        Retry several times for whatever reasons.

        Returns:
            blob: Blob object or None
            bucket_obj: Bucket object or None
        """
        bucket, path = self.get_bucket_path()
        cl = GCSURI.get_gcs_client(self._thread_id)

        bucket_obj = None
        blob = None
        for retry in range(GCSURI.RETRY_BUCKET):
            try:
                bucket_obj = cl.get_bucket(bucket)
                blob = bucket_obj.get_blob(path)
                if new and blob is None:
                    blob = Blob(name=path, bucket=bucket_obj)
                break
            except NotFound:
                raise
            except PermissionDenied:
                raise
            except:
                time.sleep(GCSURI.RETRY_BUCKET_DELAY)
        return blob, bucket_obj

    def get_bucket_path(self) -> Tuple[str, str]:
        """Returns a tuple of URI's S3 bucket and path.
        """
        bucket, path = self.uri_wo_scheme.split(GCSURI.get_path_sep(), 1)
        return bucket, path        

    def get_presigned_url(self, duration=None, private_key_file=None, use_cached=False) -> str:
        """
        Args:
            duration: Duration in seconds. This is ignored if use_cached is on.
            use_cached: Use a cached URL. 
        """
        cache = GCSURI._CACHED_PRESIGNED_URLS
        if use_cached:
            if cache is not None and self._uri in cache:
                return cache[self._uri]
        # if not self.exists:
        #     raise Exception('File does not exist. f={f}'.format(self._uri))
        if private_key_file is None:            
            private_key_file = os.path.expanduser(GCSURI.PRIVATE_KEY_FILE)
        else:
            private_key_file = os.path.expanduser(private_key_file)
        if not os.path.exists(private_key_file):
            raise Exception('GCS private key file not found. f:{f}'.format(
                f=private_key_file))
        credentials = Credentials.from_service_account_file(private_key_file)
        duration = duration if duration is not None else GCSURI.DURATION_PRESIGNED_URL        
        blob, _ = self.get_blob()
        if blob is None:
            raise ValueError('Blob does not exist for {f}'.format(f=self._uri))
        url = blob.generate_signed_url(
            expiration=timedelta(seconds=duration),
            credentials=credentials)
        cache[self._uri] = url
        return url

    def get_public_url(self) -> str:
        bucket, path = self.get_bucket_path()
        return GCSURI._GCS_PUBLIC_URL_FORMAT.format(bucket=bucket, path=path)

    @staticmethod
    def get_gcs_client(thread_id) -> storage.Client:
        if thread_id in GCSURI._CACHED_GCS_CLIENT_PER_THREAD:
            return GCSURI._CACHED_GCS_CLIENT_PER_THREAD[thread_id]
        else:
            cl = storage.Client()
            GCSURI._CACHED_GCS_CLIENT_PER_THREAD[thread_id] = cl
            return cl

    @staticmethod
    def init_gcsuri(
        loc_prefix: Optional[str]=None,
        private_key_file: Optional[str]=None,
        duration_presigned_url: Optional[int]=None,
        retry_bucket: Optional[int]=None,
        retry_bucket_delay: Optional[int]=None,
        use_gsutil_for_s3: Optional[bool]=None):
        if loc_prefix is not None:
            GCSURI.LOC_PREFIX = loc_prefix
        if private_key_file is not None:
            GCSURI.PRIVATE_KEY_FILE = private_key_file
        if duration_presigned_url is not None:
            GCSURI.DURATION_PRESIGNED_URL = duration_presigned_url
        if retry_bucket is not None:
            GCSURI.RETRY_BUCKET = retry_bucket
        if retry_bucket_delay is not None:
            GCSURI.RETRY_BUCKET_DELAY = retry_bucket_delay
        if use_gsutil_for_s3 is not None:
            GCSURI.USE_GSUTIL_FOR_S3 = use_gsutil_for_s3
