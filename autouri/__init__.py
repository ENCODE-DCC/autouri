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
from .uribase import URIBase, init_uribase
from .autouri import AutoURI
from .abspath import AbsPath, init_abspath
from .httpurl import HTTPURL, init_httpurl
from .s3uri import S3URI, init_s3uri
from .gcsuri import GCSURI, init_gcsuri


__version__ = '0.1.0'
