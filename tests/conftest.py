#!/usr/bin/env python3
"""
"""
import hashlib
import os
import pytest
from .files import (
    j1_json_contents,
    v41_txt_contents,
    v421_tsv_contents,
    v5_csv_contents,
    v6_txt_contents,
    j1_json,
    v41_txt,
    v421_tsv,
    v5_csv,
    v6_txt,
)
from autouri.httpurl import HTTPURL, init_httpurl
from autouri.abspath import AbsPath, init_abspath
from autouri.gcsuri import GCSURI, init_gcsuri
from autouri.s3uri import S3URI, init_s3uri


def pytest_addoption(parser):
    parser.addoption(
        '--ci-prefix', required=True,
        help='Prefix for CI test.'
    )
    parser.addoption(
        '--s3-root', default='s3://encode-test-autouri/tmp',
        help='S3 root path for CI test.'
    )
    parser.addoption(
        '--gcs-root', default='gs://encode-test-autouri/tmp',
        help='GCS root path for CI test.'
    )
    parser.addoption(
        '--url-root', default='https://storage.googleapis.com/encode-test-autouri/tmp',
        help='URL root path for CI test.'
    )


@pytest.fixture(scope="session")
def ci_prefix(request):
    return request.config.getoption("--ci-prefix").rstrip('/')


@pytest.fixture(scope="session")
def s3_root(request):
    return request.config.getoption("--s3-root").rstrip('/')


@pytest.fixture(scope="session")
def gcs_root(request):
    return request.config.getoption("--gcs-root").rstrip('/')


@pytest.fixture(scope="session")
def url_root(request):
    return request.config.getoption("--url-root").rstrip('/')


@pytest.fixture(scope='session')
def local_test_path(tmpdir_factory, ci_prefix):
    return tmpdir_factory.mktemp(ci_prefix).realpath()


@pytest.fixture(scope='session')
def s3_test_path(s3_root, ci_prefix):
    return '{s3_root}/{ci_prefix}'.format(
        s3_root=s3_root, ci_prefix=ci_prefix)


@pytest.fixture(scope='session')
def gcs_test_path(gcs_root, ci_prefix):
    return '{gcs_root}/{ci_prefix}'.format(
        gcs_root=gcs_root, ci_prefix=ci_prefix)


@pytest.fixture(scope='session')
def url_test_path(url_root, ci_prefix):
    return '{url_root}/{ci_prefix}'.format(
        url_root=url_root, ci_prefix=ci_prefix)


@pytest.fixture(scope="session")
def local_j1_json(local_test_path):
    return j1_json(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v41_txt(local_test_path):
    return v41_txt(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v421_tsv(local_test_path):
    return v421_tsv(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v5_csv(local_test_path):
    return v5_csv(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v6_txt(local_test_path):
    return v6_txt(local_test_path, make=True)


@pytest.fixture(scope="session")
def s3_v6_txt(s3_test_path):
    return v6_txt(s3_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_v6_txt(gcs_test_path):
    return v6_txt(gcs_test_path, make=True)


@pytest.fixture(scope="session")
def url_v6_txt(gcs_v6_txt, url_test_path):
    """URL is read-only. So this is a link to the actual file on GCS.
    """
    return v6_txt(url_test_path, make=False)


@pytest.fixture(scope="session")
def v6_txt_md5_hash():
    return hashlib.md5(v6_txt_contents().encode()).hexdigest()


@pytest.fixture(scope="session")
def v6_txt_size():
    return len(v6_txt_contents())


# @pytest.fixture(scope='session')
# def url_sm_file():
#     """Small file on public GC bucket.
#     """
#     return 'https://storage.googleapis.com/encode-test-autouri/data/sm_file.txt'


# @pytest.fixture(scope='session')
# def s3_sm_file():
#     return 's3://encode-test-autouri/data/sm_file.txt'


# @pytest.fixture(scope='session')
# def gcs_sm_file():
#     return 'gs://encode-test-autouri/data/sm_file.txt'


# @pytest.fixture(scope='session')
# def url_lg_file():
#     """Large file on public GC bucket.
#     """
#     return 'https://storage.googleapis.com/encode-test-autouri/data/lg_file.txt.gz'


# @pytest.fixture(scope='session')
# def s3_lg_file():
#     return 's3://encode-test-autouri/data/lg_file.txt.gz'


# @pytest.fixture(scope='session')
# def gcs_lg_file():
#     return 'gs://encode-test-autouri/data/lg_file.txt.gz'


# @pytest.fixture(scope='session')
# def sm_file_contents(url_sm_file):    
#     return HTTPURL(url_sm_file).read()


# @pytest.fixture(scope='session')
# def cache_dirs(tmpdir_factory, ci_prefix):
#     local_cache = tmpdir_factory.mktemp(ci_prefix).mkdir('cache').realpath()
#     s3_cache = 's3://encode-pipeline-test-runs/test_autouri/{ci_prefix}/cache'.format(
#         ci_prefix=ci_prefix)
#     gcs_cache = 'gs://encode-pipeline-test-runs/test_autouri/{ci_prefix}/cache'.format(
#         ci_prefix=ci_prefix)
#     init_abspath(loc_prefix=local_cache)
#     init_s3uri(loc_prefix=s3_cache)
#     init_gcsuri(loc_prefix=gcs_cache)
#     return local_cache, s3_cache, gcs_cache


# @pytest.fixture(scope='session')
# def local_test_dir(cache_dirs, tmpdir_factory, ci_prefix):
#     return tmpdir_factory.mktemp(ci_prefix).mkdir('data').realpath()


# @pytest.fixture(scope='session')
# def local_sm_file(sm_file_contents, local_test_dir):
#     uri = AbsPath(os.path.join(local_test_dir, 'sm_file.txt'))
#     uri.write(sm_file_contents)
#     return uri.uri

