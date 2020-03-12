# autouri

## Introduction

It is a Python API for recursively localizing file URIs (e.g. `gs://`. `s3://`, `http://` and local path) on any target directory URI.

## Features:

- Wraps Python APIs for cloud URIs and URLs.
    - `google-cloud-storage` for `gs://` URIs.
    - `boto3` for `s3://` URIs.
    - `requests` for HTTP URLs `http://` and `https://`.
- Wraps `gsutil` CLI for direct transfer between `gs://` and `s3://` URIs.
- Can presign a bucket URI to get a temporary public URL (e.g. for genome browsers).
- File locking.
- MD5 hash checking to prevent unnecessary re-downloading.
- Localize files on a different URI type.
    - Keeping the original directory structure.
    - Recursively localize all files in CSV/TSV/JSON(value only) files.

## Installation

```
$ pip install autouri
```

## Usage

Python API example.
```python
import autouri
from autouri import AutoURI
from autouri import AbsPath


def example():
    """Example for basic functions
    """

    u = AutoURI('gs://test-bucket/hello-world.txt')
    u.write('some text here')

    u.cp('s3://test-bucket/hello-another-world.txt')

    if u.exists:
        u.rm()

    target_s = AutoURI('s3://test-bucket/hello-another-world.txt').read()
    print(target_s)


def example_loc_method1():
    """Example for localization    (method1)
    """
    u = AutoURI('gs://test-bucket/hello-world.json')

    # call directly from AutoURI (or URIBase)
    # loc_prefix defines destination URI directory for localization
    AutoURI.localize(
        'gs://test-bucket/hello-world.json',
        recursive=True,
        loc_prefix='/home/leepc12/loc_cache_dir/')


def example_loc_method2():
    """Example for localization    (method2)
    """
    u = AutoURI('gs://test-bucket/hello-world.json')

    # initialize that class' constant first
    # loc_prefix defines destination URI directory for localization
    AbsPath.init_abspath(
        loc_prefix='/home/leepc12/loc_cache_dir/')

    # call from a specific storage class
    AbsPath.localize(
        'gs://test-bucket/hello-world.json',
        recursive=True)


example()
example_loc_method1()
example_loc_method2()

```

CLI: Use `--help` for each sub-command.
```
$ autouri --help
usage: autouri [-h] {metadata,cp,read,write,rm,loc,presign} ...

positional arguments:
  {metadata,cp,read,write,rm,loc,presign}
    metadata            AutoURI(src).get_metadata(): Get metadata of source.
    cp                  AutoURI(src).cp(target): Copy source to target. target
                        must be a full filename/directory. Target directory
                        must have a trailing directory separator (e.g. /)
    read                AutoURI(src).read(): Read from source.
    write               AutoURI(src).write(text): Write text on source.
    rm                  AutoURI(src).rm(): Delete source.
    loc                 type(target_dir).localize(src): Localize source on
                        target directory (class)
    presign             AutoURI(src).get_presigned_url(). For cloud-based URIs
                        only.

optional arguments:
  -h, --help            show this help message and exit
```


## GCS/S3 bucket policies

`autouri` best works with default bucket configuration for both cloud storages.

GCS (`gs://bucket-name`)
  - Bucket versioning must be turned off.
    - Check with `gsutil versioning get gs://[YOUR_BUCKET_NAME]`

S3 (`s3://bucket-name`)
  - Object versioning must be turned off.


## netrc authentication

You can use `~/.netrc` to get access to private URLs.


## Using `gsutil`

autouri can use `gsutil` for a directory file transfer between S3 and GCS. Define `--use-gsutil-for-s3` in CLI or use `GCSURI.init_gcsuri(use_gsutil_for_s3=True)` in Python.

`gsutil` must be configured correctly with AWS credentials. Install both aws
```
$ aws configure
$ gsutil config
```

## Known issues

Race condition is tested with multiple threads trying to write on the same file. File locking based on [filelock](https://github.com/benediktschmitt/py-filelock). Such file locking is stable on local/GCS files but unstable on S3.
