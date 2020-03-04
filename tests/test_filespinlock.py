#!/usr/bin/env python3
"""Race condition tests will be done in test_race_cond.py
"""
import os
import pytest
import time
from autouri.autouri import AutoURI, URIBase
from autouri.filespinlock import AutoURIFileLock
from filelock import Timeout

class AutoURIFileLockTestException(Exception):
	pass


def test_filespinlock(
	local_v6_txt,
	gcs_v6_txt,
	s3_v6_txt):
	for v6_txt in (local_v6_txt, gcs_v6_txt, s3_v6_txt):
		u_lock = AutoURI(v6_txt + URIBase.LOCK_FILE_EXT)
		lock = AutoURI(v6_txt).get_lock(no_lock=False)

		lock.acquire()
		try:
			assert u_lock.exists
			time.sleep(1)
		finally:
			lock.release()
		assert not u_lock.exists

		lock = AutoURIFileLock(v6_txt, no_lock=True)
		lock.acquire()
		try:		
			assert not u_lock.exists
			time.sleep(1)
		finally:
			lock.release()
		assert not u_lock.exists


def test_filespinlock_raise(
	local_v6_txt,
	gcs_v6_txt,
	s3_v6_txt):
	for v6_txt in (local_v6_txt, gcs_v6_txt, s3_v6_txt):		
		u_lock = AutoURI(v6_txt + URIBase.LOCK_FILE_EXT)
		lock = AutoURI(v6_txt).get_lock(no_lock=False)

		lock.acquire()
		try:
			assert u_lock.exists
			time.sleep(1)
			raise AutoURIFileLockTestException
		except AutoURIFileLockTestException:
			assert True
		else:
			assert False
		finally:
			lock.release()
			assert not u_lock.exists


def test_filespinlock_with_context(
	local_v6_txt,
	gcs_v6_txt,
	s3_v6_txt):
	for v6_txt in (local_v6_txt, gcs_v6_txt, s3_v6_txt):		
		u_lock = AutoURI(v6_txt + URIBase.LOCK_FILE_EXT)

		with AutoURI(v6_txt).get_lock(no_lock=False) as lock:
			assert u_lock.exists
			time.sleep(1)
		assert not u_lock.exists

		with AutoURI(v6_txt).get_lock(no_lock=True) as lock:
			assert not u_lock.exists
			time.sleep(1)
		assert not u_lock.exists


def test_filespinlock_with_context_raise(
	local_v6_txt,
	gcs_v6_txt,
	s3_v6_txt):
	for v6_txt in (local_v6_txt, gcs_v6_txt, s3_v6_txt):		
		u_lock = AutoURI(v6_txt + URIBase.LOCK_FILE_EXT)

		try:
			with AutoURI(v6_txt).get_lock(no_lock=False) as lock:
				assert u_lock.exists
				time.sleep(1)
				raise AutoURIFileLockTestException
		except AutoURIFileLockTestException:
			assert not u_lock.exists
		else:
			assert False


def test_filespinlock_timeout(
	local_v6_txt):
	"""Timeout = 3, 8 sec
	For local storage (AbsPath) only.
	Default poll_interval (10 sec) is too long for test remote files.
	"""
	for v6_txt in (local_v6_txt,):
		u_lock = AutoURI(v6_txt + URIBase.LOCK_FILE_EXT)

		time_s = time.time()
		lock = AutoURI(v6_txt).get_lock(no_lock=False)
		lock.acquire()
		try:
			lock2 = AutoURI(v6_txt).get_lock(no_lock=False, timeout=3)
			lock2.acquire()
			try:
				pass
			finally:
				lock2.release()
		except Timeout:
			assert 2 < time.time() - time_s < 4
		else:
			assert False			
		finally:
			lock.release()
		assert not u_lock.exists

		time_s = time.time()
		lock = AutoURI(v6_txt).get_lock(no_lock=False)
		lock.acquire()
		try:
			lock2 = AutoURI(v6_txt).get_lock(no_lock=False, timeout=8)
			lock2.acquire()
			try:
				pass
			finally:
				lock2.release()
		except Timeout:
			assert 7 < time.time() - time_s < 9
		else:
			assert False			
		finally:
			lock.release()
		assert not u_lock.exists
