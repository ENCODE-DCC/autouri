#!/usr/bin/env python3
"""Notes about race condition:
We use soft locks, which watches .lock file to check if appears and disappears.

AbsPath:
    Almost race cond free.
    Fast polling (0.01sec)

GCSURI:
    Default slow polling (10sec)
    Python API is not thread-safe.
    API provides a way to lock an object.

S3URI:
    Default slow polling (10sec)

HTTPURL:
    Read-only storage so don't need to test

"""
import os
import pytest
import time
from multiprocessing import Pool
from typing import Any, Tuple, Union

from autouri.abspath import AbsPath
from autouri.autouri import AutoURI

from .files import v6_txt_contents


def write_v6_txt(x):
    """Lock -> write_lockfree -> read -> compare written vs read -> unlock.
    This writes different text for different thread.
    """
    uri, i = x
    s = v6_txt_contents() + str(i)
    u = AutoURI(uri, thread_id=i)

    with u.get_lock(no_lock=False) as lock:
        u.write(s, no_lock=True)
        assert u.read() == s


def test_race_cond_autouri_write(
    local_test_path,
    gcs_test_path,
    s3_test_path):
    """Test nth threads competing to write on the same file v6.txt.
    Compare written string vs read string.

    Important notes:
        Python API for GCS client() is not thread-safe.
            So we need to specify thread_id here.
            URIBase (and its child GCSURI) has a thread_id
            This will make a new GCS client instance for each thread.
        S3 Object Lock is based on Versioning.
            We don't allow versioning so keep using unstable soft file lock.
    """
    tests = (
        (local_test_path, 50),
        (gcs_test_path, 10),
        (s3_test_path, 5),
    )
    # (local_test_path, gcs_test_path, s3_test_path,):
    for (test_path, nth) in tests:
        prefix = os.path.join(test_path, 'test_race_cond_autouri_write')
        s = os.path.join(prefix, 'v6.txt')
        u = AutoURI(s)
        if u.exists:
            u.rm()
        p = Pool(nth)
        p.map(write_v6_txt, list(zip([s]*nth, range(nth))))
        p.close()
        p.join()