"""S3 Bucket configuration:
    S3 Object versioning must be turned off
"""
import logging
import time
from tempfile import NamedTemporaryFile
from typing import Optional, Tuple

import requests
from boto3 import client
from botocore.exceptions import ClientError
from filelock import BaseFileLock

from .autouri import AutoURI, URIBase
from .metadata import URIMetadata, get_seconds_from_epoch, parse_md5_str

logger = logging.getLogger(__name__)


class S3URILock(BaseFileLock):
    """Locking without using S3 Object Lock.
    Without S3 object lock, boto3's put_object(), which is used for _write() and _cp() in this module,
    does not ensure consistency of multiple write operations at the same time.
    It overwrites for all write requests but the last object written.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object

    To make this lock as stable as possible, this module uses .lock file with id(self) written on it.
    This module first checks if .lock does not exist, then tries to write .lock with id(self).
    It waits for a short time (self._lock_read_delay) and checks if written .lock has the same id(self).
    self._lock_read_delay is set as poll_interval/10.
    """

    def __init__(self, lock_file, timeout=900, poll_interval=10.0, no_lock=False):
        super().__init__(lock_file, timeout=timeout)
        self._poll_interval = poll_interval
        self._lock_read_delay = self._poll_interval / 10.0

    def acquire(self, timeout=None, poll_intervall=5.0):
        """To use self._poll_interval instead of poll_intervall in args.
        """
        super().acquire(timeout=timeout, poll_intervall=self._poll_interval)

    def _acquire(self):
        """Unlike GCSURI, this module does not use S3 Object locking.
        This will write id(self) on a .lock file.
        """
        u = S3URI(self._lock_file)
        str_id = str(id(self))
        try:
            if not u.exists:
                u.write(str_id, no_lock=True)
                time.sleep(self._lock_read_delay)
            if u.read() == str_id:
                self._lock_file_fd = id(self)
        except ClientError as e:
            status = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if status in (403, 404):
                raise
        return None

    def _release(self):
        u = S3URI(self._lock_file)
        try:
            u.rm(no_lock=True)
            self._lock_file_fd = None
        except ClientError:
            pass
        return None


class S3URI(URIBase):
    """
    Class constants:
        LOC_PREFIX (inherited):
            Path prefix for localization. Inherited from URIBase class.
        DURATION_PRESIGNED_URL:
            Duration for presigned URLs in seconds.

    Protected class constants:
        _CACHED_BOTO3_CLIENTS:
        _CACHED_PRESIGNED_URLS:
        _S3_PUBLIC_URL_FORMAT:
            End point for a bucket with public access + key path
    """

    DURATION_PRESIGNED_URL: int = 4233600

    _CACHED_BOTO3_CLIENTS = {}
    _CACHED_PRESIGNED_URLS = {}
    _S3_PUBLIC_URL_FORMAT = "http://{bucket}.s3.amazonaws.com/{path}"

    _LOC_SUFFIX = ".s3"
    _SCHEMES = ("s3://",)

    def __init__(self, uri, thread_id=-1):
        super().__init__(uri, thread_id=thread_id)

    def _get_lock(self, timeout=None, poll_interval=None):
        if timeout is None:
            timeout = S3URI.LOCK_TIMEOUT
        if poll_interval is None:
            poll_interval = S3URI.LOCK_POLL_INTERVAL
        return S3URILock(
            self._uri + S3URI.LOCK_FILE_EXT,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        exists, mt, sz, md5 = False, None, None, None

        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        try:
            m = cl.head_object(Bucket=bucket, Key=path)["ResponseMetadata"][
                "HTTPHeaders"
            ]
            # make keys lower-case
            headers = {k.lower(): v for k, v in m.items()}
            exists = True

            if not skip_md5:
                if "content-md5" in headers:
                    md5 = parse_md5_str(headers["content-md5"])
                elif "etag" in headers:
                    md5 = parse_md5_str(headers["etag"])
                if md5 is None:
                    # make_md5_file is ignored for S3URI
                    md5 = self.md5_from_file

            if "content-length" in headers:
                sz = int(headers["content-length"])

            if "last-modified" in headers:
                mt = get_seconds_from_epoch(headers["last-modified"])

        except Exception:
            pass

        return URIMetadata(exists=exists, mtime=mt, size=sz, md5=md5)

    def read(self, byte=False):
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        obj = cl.get_object(Bucket=bucket, Key=path)
        if byte:
            return obj["Body"].read()
        return obj["Body"].read().decode()

    def find_all_files(self):
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()
        sep = S3URI.get_path_sep()
        if path:
            path = path.rstrip(sep) + sep

        result = []
        objs = cl.list_objects_v2(Bucket=bucket, Prefix=path).get("Contents")
        if objs:
            for obj in objs:
                scheme = S3URI.get_schemes()[0]
                uri = scheme + sep.join([bucket, obj["Key"]])
                result.append(uri)
        return result

    def _write(self, s):
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        if isinstance(s, str):
            b = s.encode("ascii")
        else:
            b = s
        cl.put_object(Bucket=bucket, Key=path, Body=b)
        return

    def _rm(self):
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        cl.delete_object(Bucket=bucket, Key=path)
        return

    def _cp(self, dest_uri):
        """Copy from S3URI to
            S3URI
            AbsPath
        """
        from .abspath import AbsPath

        dest_uri = AutoURI(dest_uri)
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        if isinstance(dest_uri, S3URI):
            dest_bucket, dest_path = dest_uri.get_bucket_path()
            cl.copy_object(
                CopySource={"Bucket": bucket, "Key": path},
                Bucket=dest_bucket,
                Key=dest_path,
            )
            return True

        elif isinstance(dest_uri, AbsPath):
            dest_uri.mkdir_dirname()
            with open(dest_uri._uri, "wb") as fp:
                cl.download_fileobj(Bucket=bucket, Key=path, Fileobj=fp)
            return True
        return False

    def _cp_from(self, src_uri):
        """Copy to S3URI from
            AbsPath
            HTTPURL
        """
        from .abspath import AbsPath
        from .httpurl import HTTPURL

        src_uri = AutoURI(src_uri)
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        if isinstance(src_uri, AbsPath):
            cl.upload_file(Filename=src_uri._uri, Bucket=bucket, Key=path)
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
                cl.upload_fileobj(Fileobj=fp, Bucket=bucket, Key=path)
            return True
        return False

    def get_bucket_path(self) -> Tuple[str, str]:
        """Returns a tuple of URI's S3 bucket and path.
        """
        arr = self.uri_wo_scheme.split(S3URI.get_path_sep(), maxsplit=1)
        if len(arr) == 1:
            # root directory without path (key)
            bucket, path = arr[0], ""
        else:
            bucket, path = arr
        return bucket, path

    def get_presigned_url(self, duration=None, use_cached=False) -> str:
        """
        Args:
            duration: Duration in seconds. This is ignored if use_cached is on.
            use_cached: Use a cached URL.
        """
        cache = S3URI._CACHED_PRESIGNED_URLS
        if use_cached:
            if cache is not None and self._uri in cache:
                return cache[self._uri]
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()
        duration = duration if duration is not None else S3URI.DURATION_PRESIGNED_URL
        url = cl.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": path}, ExpiresIn=duration
        )
        cache[self._uri] = url
        return url

    def get_public_url(self) -> str:
        bucket, path = self.get_bucket_path()
        return S3URI._S3_PUBLIC_URL_FORMAT.format(bucket=bucket, path=path)

    @staticmethod
    def get_boto3_client(thread_id=-1) -> client:
        if thread_id in S3URI._CACHED_BOTO3_CLIENTS:
            return S3URI._CACHED_BOTO3_CLIENTS[thread_id]
        else:
            cl = client("s3")
            S3URI._CACHED_BOTO3_CLIENTS[thread_id] = cl
            return cl

    @staticmethod
    def init_s3uri(
        loc_prefix: Optional[str] = None, duration_presigned_url: Optional[int] = None
    ):
        if loc_prefix is not None:
            S3URI.LOC_PREFIX = loc_prefix
        if duration_presigned_url is not None:
            S3URI.DURATION_PRESIGNED_URL = duration_presigned_url
