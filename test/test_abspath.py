#!/usr/bin/env python3
"""
Author: Jin Lee (leepc12@gmail.com)
"""

import pytest
from io import (StringIO, BytesIO)

# import autouri
# from autouri import AbsPath

def test_x():
    assert(1==2)

def test_y():
    assert(3==4)

def test_needsfiles(tmpdir):
    print (tmpdir)
    assert 0
