#!/usr/bin/env python3
"""Test possible race conditions:

"""
import os
import pytest
import time
from multiprocessing import Pool
from typing import Any, Tuple, Union
from autouri.abspath import AbsPath
from autouri.autouri import AutoURI
from .files import v6_txt_contents


def write_v6_txt(uri):
    AutoURI(uri).write(v6_txt_contents())


def test_race_cond_autouri_write(
    local_test_path,
    gcs_test_path,
    s3_test_path):
    """Test 100 threads competing to write on the same file v6.txt.
    """

    for test_path in (local_test_path,): # (s3_test_path, gcs_test_path, local_test_path gcs_test_path):
        prefix = os.path.join(test_path, 'test_race_cond_autouri_write')
        s = os.path.join(prefix, 'v6.txt')
        u = AutoURI(s)
        if u.exists:
            u.rm()

        p = Pool(20)
        p.map(write_v6_txt, [s]*20)
        p.close()
        p.join()
