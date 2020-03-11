#!/usr/bin/env python3
"""
Features:    
    - Wraps google-cloud-storage for gs:// URIs.
    - Wraps boto3 for s3:// URIs.
    - Wraps gsutil CLI for direct transfer between gs:// and s3:// URIs.
    - Wraps Python requests for HTTP URLs.
    - Can presign a bucket URI to get a temporary public URL (e.g. for genome browsers).
    - File locking (using .lock file for bucket URIs).
    - MD5 hash checking to prevent unnecessary re-uploading.
    - Localization on a different URI type.
        - Keeping the original directory structure.
        - Can recursively localize all files in a CSV/TSV/JSON(value only) file.
"""
import argparse

from .autouri import URIBase, AutoURI
from .abspath import AbsPath
from .httpurl import HTTPURL
from .s3uri import S3URI
from .gcsuri import GCSURI


__version__ = '0.1.0'


def parse_args():
    pass

