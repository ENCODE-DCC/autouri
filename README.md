[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![CircleCI](https://circleci.com/gh/ENCODE-DCC/autouri.svg?style=svg)](https://circleci.com/gh/ENCODE-DCC/autouri)

> **IMPORTANT**: If you use `--use-gsutil-for-s3` or `GCSURI.USE_GSUTIL_FOR_S3` then you need to update your `gsutil`. This flag allows a direct transfer between `gs://` and `s3://`. This requires `gsutil` >= 4.47. See this [issue](https://github.com/GoogleCloudPlatform/gsutil/issues/935) for details.
```bash
$ pip install gsutil --upgrade
```

# Autouri

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
$ pip3 install autouri
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

## Requirements

- Python >= 3.6
    - Packages: `requests`, `dateparser` and `filelock`
        ```bash
        $ pip3 install requests dateparser filelock
        ```

- Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts) to get CLIs (`gcloud` and `gsutil`).

- Install `google-cloud-storage` Python API.
    ```bash
    $ pip3 install google-cloud-storage
    ```

- Install `boto3` Python API and AWS CLI.
    ```bash
    $ pip3 install boto3 awscli
    ```


## Authentication

- GCS: Use `gcloud` CLI.
    - Using end-user credentials: You will be asked to enter credentials of your Google account.
        ```
        $ gcloud auth application-default login --no-launch-browser
        ```
    - Using service account credentials: If you use a service account and a JSON key file associated with it.
        ```
        $ gcloud auth activate-service-account --key-file=[YOUR_JSON_KEY.json]
        $ GOOGLE_APPLICATION_CREDENTIALS="PATH/FOR/YOUR_JSON_KEY.json"
        ```

        Or import and call `add_google_app_creds_to_env()`.
        ```python
        import autouri
        from autouri.gcsuri import add_google_app_creds_to_env

        add_google_app_creds_to_env('YOUR_JSON_KEY.json')
        ```
    Then set your default project.
    ```
    $ gcloud config set project [YOUR_GCP_PROJECT_ID]
    ```

- S3: Use `aws` CLI. You will be asked to enter credentials of your AWS account.
    ```
    $ aws configure
    ```

- URL: Use `~/.netrc` file to get access to private URLs. Example `.netrc` file. You can define credential per site.
    ```
    machine www.encodeproject.org
    login XXXXXXXX
    password abcdefghijklmnop
    ```


## Using `gsutil` for direct trasnfer between GCS and S3

Autouri can use `gsutil` CLI for a direct file transfer between S3 and GCS. Define `--use-gsutil-for-s3` in command line arguments or use `GCSURI.init_gcsuri(use_gsutil_for_s3=True)` in Python. Otherwise, file transfer between GCS and S3 will be streamed through your local machine.

`gsutil` will take AWS credentials from `~/.aws/credentials` file, which is already generated in [Authentication](#authentication).


## GCS/S3 bucket configuration

Autouri best works with default bucket configuration for both cloud storages.

GCS (`gs://bucket-name`)
  - Bucket versioning must be turned off.
    - Check with `gsutil versioning get gs://[YOUR_BUCKET_NAME]`

S3 (`s3://bucket-name`)
  - Object versioning must be turned off.
