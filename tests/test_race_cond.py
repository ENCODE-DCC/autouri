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

Test nth threads competing to write on the same file v6.txt.
Compare written string vs read string.

Important notes:
    Python API for GCS client() is not thread-safe.
        So we need to specify thread_id here.
        URIBase (and its child GCSURI) has a thread_id
        This will make a new GCS client instance for each thread.
    S3 Object Lock is based on Versioning.
        We don't allow versioning so keep using unstable soft file lock.
"""
import os
from multiprocessing import Pool

from autouri.autouri import AutoURI

from .files import v6_txt_contents


def write_v6_txt(x):
    """Lock -> write_lockfree -> read -> compare written vs read -> unlock.
    This writes different text for different thread.
    """
    uri, i = x
    s = v6_txt_contents() + str(i)
    u = AutoURI(uri, thread_id=i)

    with u.get_lock(no_lock=False):
        u.write(s, no_lock=True)
        assert u.read() == s


def run_write_v6_txt(prefix, nth):
    s = os.path.join(prefix, "v6.txt")
    u = AutoURI(s)
    if u.exists:
        u.rm()
    p = Pool(nth)
    p.map(write_v6_txt, list(zip([s] * nth, range(nth))))
    p.close()
    p.join()


def test_race_cond_autouri_write_local(local_test_path):
    prefix = os.path.join(local_test_path, "test_race_cond_autouri_write_local")
    nth = 50
    run_write_v6_txt(prefix, nth)


def test_race_cond_autouri_write_gcs(gcs_test_path):
    prefix = os.path.join(gcs_test_path, "test_race_cond_autouri_write_gcs")
    nth = 10
    run_write_v6_txt(prefix, nth)


def test_race_cond_autouri_write_s3(s3_test_path):
    nth = 5
    prefix = os.path.join(s3_test_path, "test_race_cond_autouri_write_s3")
    run_write_v6_txt(prefix, nth)
