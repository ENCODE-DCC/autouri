"""GCS Bucket configuration:
    Object versioning must be turned off
        Check it with "gsutil versioning get gs://BUCKET-NAME"
        https://cloud.google.com/storage/docs/object-versioning
"""
import json
import logging
import os
import time
from datetime import timedelta
from subprocess import check_call
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Optional, Tuple

import requests
from filelock import BaseFileLock
from google.api_core.exceptions import (
    Forbidden,
    GatewayTimeout,
    NotFound,
    PermissionDenied,
    ServiceUnavailable,
)
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from google.cloud.storage import Blob
from google.oauth2.service_account import Credentials

from .autouri import AutoURI, URIBase
from .metadata import URIMetadata, get_seconds_from_epoch, parse_md5_str

logger = logging.getLogger(__name__)


ENV_VAR_GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"
GCS_TEMPORARY_HOLD_ERROR_MSG = "is under active Temporary hold"


def add_google_app_creds_to_env(service_account_key_file):
    """Google auth with a service account.
    To globally use the key file for all GCS Client() with different thread IDs,
    update environment variable `GOOGLE_APPLICATION_CREDENTIALS` with a given
    service account key JSON file.
    """
    service_account_key_file = os.path.abspath(
        os.path.expanduser(service_account_key_file)
    )
    if ENV_VAR_GOOGLE_APPLICATION_CREDENTIALS in os.environ:
        existing_key_file = os.environ[ENV_VAR_GOOGLE_APPLICATION_CREDENTIALS]
        if not os.path.samefile(existing_key_file, service_account_key_file):
            logger.warning(
                "Environment variable {env_var} does not match with "
                "given service_account_key_file. "
                "Using application default credentials? ".format(
                    env_var=ENV_VAR_GOOGLE_APPLICATION_CREDENTIALS
                )
            )
    logger.debug(
        "Adding service account key JSON {key_file} to "
        "environment variable {env_var}".format(
            key_file=service_account_key_file,
            env_var=ENV_VAR_GOOGLE_APPLICATION_CREDENTIALS,
        )
    )
    os.environ[ENV_VAR_GOOGLE_APPLICATION_CREDENTIALS] = service_account_key_file


class GCSURILock(BaseFileLock):
    """Slow but stable locking with using GCS temporary_hold
    Hold the lock file instead of the target file that lock file protects.

    Class constants:
        DEFAULT_RETRY_RELEASE:
            Retry if release (deletion) of a lock file fails.
        DEFAULT_RETRY_RELEASE_INTERVAL:
            Interval for retrial in seconds.
    """

    DEFAULT_RETRY_RELEASE = 3
    DEFAULT_RETRY_RELEASE_INTERVAL = 3

    def __init__(
        self,
        lock_file,
        thread_id=-1,
        timeout=900,
        poll_interval=10.0,
        retry_release=DEFAULT_RETRY_RELEASE,
        retry_release_interval=DEFAULT_RETRY_RELEASE_INTERVAL,
        no_lock=False,
    ):
        super().__init__(lock_file, timeout=timeout)
        self._poll_interval = poll_interval
        self._thread_id = thread_id
        self._retry_release = retry_release
        self._retry_release_interval = retry_release_interval

    def acquire(self, timeout=None, poll_intervall=5.0):
        """Use self._poll_interval instead of poll_intervall in args
        """
        super().acquire(timeout=timeout, poll_intervall=self._poll_interval)

    def _acquire(self):
        """Try to acquire a lock.
        Once successfully acquired, lock the .lock file temporarily by setting
        blob.temporary_hold as True (similary to `gsutil retention temp set URI`).
        This will be released in self._release().

        Parse Forbidden error to check if it's raised from temporary hold.
        It can also be raised from lack of write permission, which should be re-raised.
        """
        u = GCSURI(self._lock_file, thread_id=self._thread_id)
        try:
            blob, _ = u.get_blob(new=True)
            blob.upload_from_string("")
            blob.temporary_hold = True
            blob.patch()
            self._lock_file_fd = id(self)

        except Forbidden as e:
            err_msg = json.loads(e._response._content)["error"]["message"]
            if GCS_TEMPORARY_HOLD_ERROR_MSG not in err_msg:
                raise
            logger.debug(
                "Failed to acquire a file lock. "
                "It's already locked by another process. "
                "You need to wait until it's released. "
                "Retrying until timeout. "
            )

        except (GatewayTimeout, NotFound, ServiceUnavailable) as e:
            logger.debug(
                "Failed to acquire a file lock. "
                "Server is unavailable or busy? "
                "Or too many requests? "
                "Retrying until timeout. "
                "{err}".format(err=str(e))
            )

    def _release(self):
        u = GCSURI(self._lock_file, thread_id=self._thread_id)
        for retry in range(self._retry_release):
            try:
                blob, _ = u.get_blob()
                blob.temporary_hold = False
                blob.patch()
                blob.delete()
                self._lock_file_fd = None
                break
            except Exception as e:
                error_msg = "{err}. Failed to delete a lock file: file={file}. "
                if retry == self._retry_release - 1:
                    error_msg += (
                        "You may need to manually delete a lock file. "
                        'Use "gsutil retention temp release {file}" to unlock it first. '
                        'Then use "gsutil rm -f {file}" to delete it. '
                        "Deleting a lock file itself does not affect "
                        "the file protected by it."
                    )
                error_msg = error_msg.format(err=e, file=self._lock_file)

                logger.error(error_msg)

            time.sleep(self._retry_release_interval)


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
        _CACHED_GCS_CLIENTS:
            Per-thread GCS client object is required since
            GCS client is not thread-safe.
        _CACHED_PRESIGNED_URLS:
            Can use cached presigned URLs.
        _GCS_PUBLIC_URL_FORMAT:
            End point for a bucket with public access + key path
    """

    PRIVATE_KEY_FILE: str = ""
    DURATION_PRESIGNED_URL: int = 4233600

    RETRY_BUCKET: int = 3
    RETRY_BUCKET_DELAY: int = 1
    USE_GSUTIL_FOR_S3: bool = False

    _CACHED_GCS_CLIENTS = {}
    _CACHED_GCS_ANONYMOUS_CLIENTS = {}
    _CACHED_PRESIGNED_URLS = {}
    _GCS_PUBLIC_URL_FORMAT = "http://storage.googleapis.com/{bucket}/{path}"

    _LOC_SUFFIX = ".gcs"
    _SCHEMES = ("gs://",)

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
            poll_interval=poll_interval,
        )

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        exists, mt, sz, md5 = False, None, None, None

        try:
            b, _ = self.get_blob()
            # make keys lower-case
            headers = {k.lower(): v for k, v in b._properties.items()}
            exists = True

            if not skip_md5:
                if "md5hash" in headers:
                    md5 = parse_md5_str(headers["md5hash"])
                elif "etag" in headers:
                    md5 = parse_md5_str(headers["etag"])
                if md5 is None:
                    # make_md5_file is ignored for GCSURI
                    md5 = self.md5_from_file

            if "size" in headers:
                sz = int(headers["size"])

            if "updated" in headers:
                mt = get_seconds_from_epoch(headers["updated"])
            elif "timecreated" in headers:
                mt = get_seconds_from_epoch(headers["timecreated"])

        except Exception:
            logger.debug("Failed to get metadata from {uri}".format(uri=self._uri))

        return URIMetadata(exists=exists, mtime=mt, size=sz, md5=md5)

    def read(self, byte=False):
        blob, _ = self.get_blob()
        b = blob.download_as_string()
        if byte:
            return b
        return b.decode()

    def find_all_files(self):
        cl = GCSURI.get_gcs_client(self._thread_id)
        bucket, path = self.get_bucket_path()
        sep = GCSURI.get_path_sep()
        if path:
            path = path.rstrip(sep) + sep

        result = []
        blobs = cl.list_blobs(bucket, prefix=path)
        if blobs:
            for blob in blobs:
                scheme = GCSURI.get_schemes()[0]
                uri = scheme + sep.join([bucket, blob.name])
                result.append(uri)
        return result

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
                raise ValueError("Blob does not exist for {f}".format(f=self._uri))

            if isinstance(dest_uri, GCSURI):
                _, dest_path = dest_uri.get_bucket_path()
                _, dest_bucket = dest_uri.get_blob(new=True)
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
                rc = check_call(["gsutil", "-q", "cp", self._uri, dest_uri._uri])
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
                rc = check_call(["gsutil", "-q", "cp", src_uri._uri, self._uri])
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
                src_uri._uri,
                stream=True,
                allow_redirects=True,
                headers=requests.utils.default_headers(),
            )
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
        """GCS Client() has a bug that shows an outdated version of a file
        when using Blob() without update().
        For read-only functions (e.g. read()), need to directly call
        cl.get_bucket(bucket).get_blob(path) instead of using Blob() class.

        Also, GCS Client() is not thread-safe and it fails for a variety of reasons.
        Retry several times for whatever reasons.

        Client.get_bucket() and Client.get_bucket().get_blob() can fail
        even if the bucket is public (Storage Reader permission for allUsers
        or allAuthenticatedUsers).
        Needs an anonymous client (Client.create_anonymous_client()) for public buckets.
        If this error occurs, then retry with an anonymous client.

        Returns:
            blob:
                Blob object
            bucket_obj:
                Bucket object
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
            except Forbidden:
                logger.debug(
                    "Bucket/blob is forbidden. Trying again with anonymous client."
                )
                cl = GCSURI.get_gcs_anonymous_client(self._thread_id)
            except NotFound:
                raise
            except PermissionDenied:
                raise
            except Exception:
                time.sleep(GCSURI.RETRY_BUCKET_DELAY)
        if blob is None:
            raise ValueError(
                "GCS blob does not exist. lack of {access_type} permission? {uri}".format(
                    access_type="write" if new else "read", uri=self._uri
                )
            )

        return blob, bucket_obj

    def get_bucket_path(self) -> Tuple[str, str]:
        """Returns a tuple of URI's S3 bucket and path.
        """
        arr = self.uri_wo_scheme.split(GCSURI.get_path_sep(), maxsplit=1)
        if len(arr) == 1:
            # root directory without path (key)
            bucket, path = arr[0], ""
        else:
            bucket, path = arr
        return bucket, path

    def get_presigned_url(
        self, duration=None, private_key_file=None, use_cached=False
    ) -> str:
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
            raise Exception(
                "GCS private key file not found. f:{f}".format(f=private_key_file)
            )
        credentials = Credentials.from_service_account_file(private_key_file)
        duration = duration if duration is not None else GCSURI.DURATION_PRESIGNED_URL
        blob, _ = self.get_blob()
        if blob is None:
            raise ValueError("Blob does not exist for {f}".format(f=self._uri))
        url = blob.generate_signed_url(
            expiration=timedelta(seconds=duration), credentials=credentials
        )
        cache[self._uri] = url
        return url

    def get_public_url(self) -> str:
        bucket, path = self.get_bucket_path()
        return GCSURI._GCS_PUBLIC_URL_FORMAT.format(bucket=bucket, path=path)

    @staticmethod
    def get_gcs_client(thread_id) -> storage.Client:
        """Get GCS client per thread_id.

        Get default credentials (internally calling google.auth.default()) from:
            - Environment variable GOOGLE_APPLICATION_CREDENTIALS
              - Set a service account key JSON file path as this environment variable.
            - JSON file ~/.config/gcloud/application_default_credentials.json
              - To use end-user's credentials.
              - This file is created by `gcloud auth application-default login`.

        If default credentials are not found, then
            - Make/return an anonymous client instead
            - For this thread_id, it will cache anonymous client instead of failed
              client with credentials.
        """
        cl = GCSURI._CACHED_GCS_CLIENTS.get(thread_id)

        if cl is None:
            try:
                logger.debug("New GCS client for thread {id}.".format(id=thread_id))
                cl = storage.Client()
            except DefaultCredentialsError:
                cl = GCSURI.get_gcs_anonymous_client(thread_id)
            # anonymous client can also be cached here
            GCSURI._CACHED_GCS_CLIENTS[thread_id] = cl

        return cl

    @staticmethod
    def get_gcs_anonymous_client(thread_id) -> storage.Client:
        """Get GCS anonymous client per thread_id.
        """
        cl = GCSURI._CACHED_GCS_ANONYMOUS_CLIENTS.get(thread_id)

        if cl is None:
            logger.debug(
                "New GCS anonymous client for thread {id}.".format(id=thread_id)
            )
            cl = storage.Client.create_anonymous_client()
            GCSURI._CACHED_GCS_ANONYMOUS_CLIENTS[thread_id] = cl

        return cl

    @staticmethod
    def init_gcsuri(
        loc_prefix: Optional[str] = None,
        private_key_file: Optional[str] = None,
        duration_presigned_url: Optional[int] = None,
        retry_bucket: Optional[int] = None,
        retry_bucket_delay: Optional[int] = None,
        use_gsutil_for_s3: Optional[bool] = None,
    ):
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
