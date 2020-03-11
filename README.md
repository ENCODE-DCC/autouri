# autouri

## Introduction

It is a Python API for recursively localizing file URIs (e.g. `gs://`. `s3://`, `http://` and local path) on any target directory URI.

## Features:

- Wraps `google-cloud-storage` for `gs://` URIs.
- Wraps `boto3` for `s3://` URIs.
- Wraps `gsutil` CLI for direct transfer between `gs://` and `s3://` URIs.
- Wraps Python `requests` for HTTP URLs `http://` and `https://`.
- Can presign a bucket URI to get a temporary public URL (e.g. for genome browsers).
- File locking (using `.lock` file and GCS/S3 object lock).
- MD5 hash checking to prevent unnecessary re-downloading.
- Localization on a different URI type.
    - Keeping the original directory structure.
    - Recursively localize all files in CSV/TSV/JSON(value only) files.

## Usage

Python API example.
```
import autouri
from autouri.autouri import AutoURI
from autouri.abspath import AbsPath


def example():
    # example for basic functions
    u = AutoURI('gs://test-bucket/hello-world.txt')
    u.write('some text here')

    u.cp('s3://test-bucket/hello-another-world.txt')

    if u.exists:
        u.rm()

    target_s = AutoURI('s3://test-bucket/hello-another-world.txt').read()
    print(target_s)


def example_loc():
    # example for localization
    u = AutoURI('gs://test-bucket/hello-world.json')

    AutoURI.localize(
        'gs://test-bucket/hello-world.json',
        recursive=True,
        loc_prefix='/home/leepc12/loc_cache_dir/')


example()
example_loc()

```

CLI
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
