#!/usr/bin/env python3
import binascii
from base64 import b64decode
from boto3 import client
from botocore.errorfactory import ClientError
from contextlib import contextmanager
from datetime import datetime
from dateutil.parser import parse as parse_timestamp
from dateutil.tz import tzutc
from filelock import BaseFileLock
from io import BytesIO
from typing import Tuple, Optional
from .autouri import URIBase, URIMetadata, AutoURI, logger


def init_s3uri(
    loc_prefix: Optional[str]=None,
    sec_duration_presigned_url: Optional[int]=None):
    """
    Helper function to initialize S3URI class constants
        loc_prefix:
            Inherited from URIBase
    """
    if loc_prefix is not None:
        S3URI.LOC_PREFIX = loc_prefix
    if sec_duration_presigned_url is not None:
        S3URI.SEC_DURATION_PRESIGNED_URL = sec_duration_presigned_url


class S3URILock(BaseFileLock):
    def __init__(
        self, lock_file, timeout=900, poll_interval=0.1, no_lock=False):
        super().__init__(lock_file, timeout=timeout)
        self._poll_interval = poll_interval

    def acquire(self, timeout=None, poll_intervall=5.0):
        """Use self._poll_interval instead of poll_intervall in args
        """
        super().acquire(timeout=timeout, poll_intervall=self._poll_interval)

    def _acquire(self):
        u = S3URI(self._lock_file)
        try:
            u.write('', no_lock=True)
            self._lock_file_fd = id(self)
        except (ClientError,):
            pass
        return None

    def _release(self):
        u = S3URI(self._lock_file)
        try:
            u.rm(no_lock=True)
            self._lock_file_fd = None
        except (ClientError,):
            pass
        return None


class S3URI(URIBase):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from URIBase class.
        SEC_DURATION_PRESIGNED_URL:
            Duration for presigned URLs

    Protected class constants:
        _BOTO3_CLIENT:
        _CACHED_PRESIGNED_URLS:
    """
    SEC_DURATION_PRESIGNED_URL: int = 4233600

    _CACHED_BOTO3_CLIENT_PER_THREAD = {}
    _CACHED_PRESIGNED_URLS = {}

    _LOC_SUFFIX = '.s3'
    _SCHEMES = ('s3://',)

    def __init__(self, uri, thread_id=-1):
        super().__init__(uri, thread_id=thread_id)

    def get_lock(self, no_lock=False, timeout=None, poll_interval=None):
        if no_lock:
            return contextmanager(lambda: (yield))()
        if timeout is None:
            timeout = S3URI.LOCK_TIMEOUT
        if poll_interval is None:
            poll_interval = S3URI.LOCK_POLL_INTERVAL
        return S3URILock(
            self._uri + AutoURI.LOCK_FILE_EXT,
            timeout=timeout,
            poll_interval=poll_interval)

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        ex, mt, sz, md5 = False, None, None, None

        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        try:
            m = cl.head_object(Bucket=bucket, Key=path)['ResponseMetadata']['HTTPHeaders']
            # make keys lower-case
            h = {k.lower(): v for k, v in m.items()}
            ex = True

            md5_raw = None
            if 'content-md5' in h:
                md5_raw = h['content-md5']
            elif 'etag' in h:
                md5_raw = h['etag']
            if md5_raw is not None:
                md5_raw = md5_raw.strip('"\'')
                if len(md5_raw) == 32:
                    md5 = md5_raw
                else:
                    md5 = binascii.hexlify(b64decode(md5_raw)).decode()

            if 'content-length' in h:
                sz = int(h['content-length'])

            if 'last-modified' in h:
                utc_t = parse_timestamp(h['last-modified'])
            else:
                utc_t = None
            if utc_t is not None:              
                utc_epoch = datetime(1970, 1, 1, tzinfo=tzutc())      
                mt = (utc_t - utc_epoch).total_seconds()

            if md5 is None and not skip_md5:
                md5 = self.md5_from_file
                # make_md5_file is ignored for S3URI

        except (ClientError,):
            pass

        return URIMetadata(
            exists=ex,
            mtime=mt,
            size=sz,
            md5=md5)

    def read(self, byte=False):
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        obj = cl.get_object(Bucket=bucket, Key=path)
        if byte:
            return obj['Body'].read()
        return obj['Body'].read().decode()

    def _write(self, s):
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        if isinstance(s, str):
            b = s.encode('ascii')
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
            dest_bucket, dest_path = dest_uri.get_path()
            cl.copy_object(
                CopySource={
                    'Bucket': bucket,
                    'Key': path},
                Bucket=dest_bucket,
                Key=dest_path)
            return True

        elif isinstance(dest_uri, AbsPath):
            dest_uri.mkdir_dirname()
            with open(dest_uri._uri, 'wb') as fp:
                cl.download_fileobj(Bucket=bucket, Key=path, Fileobj=fp)
            return True
        return False

    def _cp_from(self, src_uri):
        """Copy to S3URI from
            AbsPath
            HTTPURL
        """
        from .abspath import AbsPath

        src_uri = AutoURI(src_uri)
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()

        if isinstance(src_uri, AbsPath):
            cl.upload_file(
                Filename=src_uri._uri,
                Bucket=bucket,
                Key=path)
            return True

        elif isinstance(src_uri, HTTPURL):
            r = requests.get(
                src_uri._uri, stream=True, allow_redirects=True,
                headers=requests.utils.default_headers())
            r.raise_for_status()
            b = BytesIO()
            with open(b, 'wb') as fp:
                for chunk in r.iter_content(HTTPURL.get_chunk_size()):
                    fp.write(chunk)
            with open(b, 'rb') as fp:
                cl.upload_fileobj(
                    Fileobj=fp,
                    Bucket=bucket,
                    Key=path)
            return True
        return False

    def get_bucket_path(self) -> Tuple[str, str]:
        """Returns a tuple of URI's S3 bucket and path.
        """
        bucket, path = self.uri_wo_scheme.split(S3URI.get_path_sep(), 1)
        return bucket, path

    def get_presigned_url(self, sec_duration=None, use_cached=False) -> str:
        """
        Args:
            sec_duration: Duration in seconds. This is ignored if use_cached is on.
            use_cached: Use a cached URL. 
        """
        cache = S3URI._CACHED_PRESIGNED_URLS
        if use_cached:
            if cache is not None and self._uri in cache:
                return cache[self._uri]
        cl = S3URI.get_boto3_client(self._thread_id)
        bucket, path = self.get_bucket_path()
        duration = sec_duration if sec_duration is not None else S3URI.SEC_DURATION_PRESIGNED_URL
        url = cl.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': path},
            ExpiresIn=duration)
        cache[self._uri] = url
        return url

    @staticmethod
    def get_boto3_client(thread_id=-1) -> client:
        if thread_id in S3URI._CACHED_BOTO3_CLIENT_PER_THREAD:
            return S3URI._CACHED_BOTO3_CLIENT_PER_THREAD[thread_id]
        else:
            cl = client('s3')
            S3URI._CACHED_BOTO3_CLIENT_PER_THREAD[thread_id] = cl
            return cl
