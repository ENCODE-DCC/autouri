#!/usr/bin/env python3
"""S3URI class

Author: Jin Lee (leepc12@gmail.com)
"""

from binascii import hexlify
from base64 import b64decode
from datetime import datetime
from dateutil.parser import parse as parse_timestamp
from boto3 import client
from io import BytesIO
from botocore.errorfactory import ClientError
from typing import Tuple
from .autouri import AutoURI, AutoURIMetadata, logger


def init_s3uri(
    loc_prefix: str=None,
    sec_duration_presigned_url: int=None):
    """
    Helper function to initialize S3URI class constants
        loc_prefix:
            Inherited from AutoURI
    """
    if loc_prefix is not None:
        S3URI.LOC_PREFIX = loc_prefix
    if sec_duration_presigned_url is not None:
        S3URI.SEC_DURATION_PRESIGNED_URL = sec_duration_presigned_url


class S3URI(AutoURI):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from AutoURI class.
        SEC_DURATION_PRESIGNED_URL:
            Duration for presigned URLs

    Protected class constants:
        _BOTO3_CLIENT:
        _CACHED_PRESIGNED_URLS:
    """
    SEC_DURATION_PRESIGNED_URL: int = 4233600

    _BOTO3_CLIENT = None
    _CACHED_PRESIGNED_URLS = {}

    _LOC_SUFFIX = '.s3'
    _SCHEMES = ('s3://',)

    def __init__(self, uri):
        super().__init__(uri, cls=self.__class__)

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        ex, mt, sz, md5 = None, None, None, None

        cl = S3URI.get_boto3_client()
        bucket, path = self.get_bucket_path()

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

        if md5 is None and not skip_md5:
            md5 = self.get_md5_from_file(make_md5_file=make_md5_file)

        return AutoURIMetadata(
            exists=ex,
            mtime=mt,
            size=sz,
            md5=md5)

    def read(self, byte=False):
        cl = S3URI.get_boto3_client()
        bucket, path = self.get_bucket_path()

        obj = cl.get_object(Bucket=bucket, Key=path)
        if byte:
            return obj['Body'].reads()
        return obj['Body'].reads().decode()

    def _write(self, s):
        cl = S3URI.get_boto3_client()
        bucket, path = self.get_bucket_path()

        if isinstance(s, str):
            b = s.encode('ascii')
        else:
            b = s
        cl.put_object(Bucket=bucket, Key=path, Body=b)
        return

    def _rm(self):
        cl = S3URI.get_boto3_client()
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
        cl = S3URI.get_boto3_client()
        bucket, path = self.get_bucket_path()

        if isinstance(dest_uri, S3URI):
            dest_bucket, dest_path = dest_uri.get_path()
            cl.copy_object(
                CopySource={
                    'Bucket': bucket,
                    'Key': path}
                Bucket=dest_bucket,
                Key=dest_path)
            return True

        elif isinstance(dest_uri, AbsPath):
            dest_uri.mkdir_dirname()
            with open(dest_uri._uri, 'wb') as fp:
                cl.download_file(Bucket=bucket, Key=path, Fileobj=fp)
            return True

        return False

    def _cp_from(self, src_uri):
        """Copy to S3URI from
            AbsPath
            HTTPURL
        """
        from .abspath import AbsPath

        cl = S3URI.get_boto3_client()
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

        raise NotImplementedError

    def get_bucket_path(self) -> Tuple[str, str]:
        """Returns a tuple of URI's S3 bucket and path.
        """
        bucket, path = self.uri_wo_scheme.split(S3URI.get_path_sep(), 1)
        return bucket, path

    def get_presigned_url(self, use_cached=False) -> str:
        cache = S3URI._CACHED_PRESIGNED_URLS
        if use_cached:
            if cache is not None and self._uri in cache:
                return cache[self._uri]
        cl = S3URI.get_boto3_client()
        bucket, path = self.get_bucket_path()
        url = cl.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': path},
            ExpiresIn=S3URI.SEC_DURATION_PRESIGNED_URL)
        cache[self._uri] = url
        return url

    @staticmethod
    def get_boto3_client():
        if cls._BOTO3_CLIENT is None:
            cls._BOTO3_CLIENT = client('s3')
        return cls._BOTO3_CLIENT
