#!/usr/bin/env python3
import os
import pytest
from autouri.httpurl import HTTPURL, init_httpurl
from autouri.abspath import AbsPath, init_abspath
from autouri.gcsuri import GCSURI, init_gcsuri
from autouri.s3uri import S3URI, init_s3uri


def pytest_addoption(parser):
    parser.addoption(
        '--ci-prefix',
        default='NOT_DEFINED',
        help='Prefix for CI test.'
    )


@pytest.fixture(scope="session")
def ci_prefix(request):
    return request.config.getoption("--ci-prefix")


@pytest.fixture(scope='session')
def url_sm_file():
    """Small file on public GC bucket.
    """
    return 'https://storage.googleapis.com/encode-test-autouri/data/sm_file.txt'


@pytest.fixture(scope='session')
def s3_sm_file():
    return 's3://encode-test-autouri/data/sm_file.txt'


@pytest.fixture(scope='session')
def gcs_sm_file():
    return 'gs://encode-test-autouri/data/sm_file.txt'


@pytest.fixture(scope='session')
def url_lg_file():
    """Large file on public GC bucket.
    """
    return 'https://storage.googleapis.com/encode-test-autouri/data/lg_file.txt.gz'


@pytest.fixture(scope='session')
def s3_lg_file():
    return 's3://encode-test-autouri/data/lg_file.txt.gz'


@pytest.fixture(scope='session')
def gcs_lg_file():
    return 'gs://encode-test-autouri/data/lg_file.txt.gz'


@pytest.fixture(scope='session')
def sm_file_contents(url_sm_file):    
    return HTTPURL(url_sm_file).read()


@pytest.fixture(scope='session')
def cache_dirs(tmpdir_factory, ci_prefix):
    local_cache = tmpdir_factory.mktemp(ci_prefix).mkdir('cache').realpath()
    s3_cache = 's3://encode-pipeline-test-runs/test_autouri/{ci_prefix}/cache'.format(
        ci_prefix=ci_prefix)
    gcs_cache = 'gs://encode-pipeline-test-runs/test_autouri/{ci_prefix}/cache'.format(
        ci_prefix=ci_prefix)
    init_abspath(loc_prefix=local_cache)
    init_s3uri(loc_prefix=s3_cache)
    init_gcsuri(loc_prefix=gcs_cache)
    return local_cache, s3_cache, gcs_cache


@pytest.fixture(scope='session')
def local_test_dir(cache_dirs, tmpdir_factory, ci_prefix):
    return tmpdir_factory.mktemp(ci_prefix).mkdir('data').realpath()


@pytest.fixture(scope='session')
def local_sm_file(sm_file_contents, local_test_dir):
    uri = AbsPath(os.path.join(local_test_dir, 'sm_file.txt'))
    uri.write(sm_file_contents)
    return uri.uri
