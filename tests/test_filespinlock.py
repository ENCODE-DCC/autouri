#!/usr/bin/env python3
"""Race condition tests will be done in test_race_cond.py
"""
import os
import pytest
import time
from autouri.autouri import AutoURI
from autouri.filespinlock import FileSpinLock


class FileSpinLockTestException(Exception):
	pass


def test_filespinlock(local_v6_txt):
	u_lock = AutoURI(local_v6_txt + FileSpinLock.LOCK_FILE_EXT)

	lock = FileSpinLock(local_v6_txt, no_lock=False)
	lock.acquire()
	try:
		assert u_lock.exists
		time.sleep(1)
	finally:
		lock.release()
	assert not u_lock.exists

	lock = FileSpinLock(local_v6_txt, no_lock=True)
	lock.acquire()
	try:		
		assert not u_lock.exists
		time.sleep(1)
	finally:
		lock.release()
	assert not u_lock.exists


@pytest.mark.xfail(raises=FileSpinLockTestException)
def test_filespinlock_raise(local_v6_txt):
	u_lock = AutoURI(local_v6_txt + FileSpinLock.LOCK_FILE_EXT)

	lock = FileSpinLock(local_v6_txt, no_lock=False)
	lock.acquire()
	try:
		assert u_lock.exists
		time.sleep(1)
		raise FileSpinLockTestException
	finally:
		lock.release()
		assert not u_lock.exists


def test_filespinlock_with_context(local_v6_txt):
	u_lock = AutoURI(local_v6_txt + FileSpinLock.LOCK_FILE_EXT)

	with FileSpinLock(local_v6_txt, no_lock=False) as lock:
		assert u_lock.exists
		time.sleep(1)
	assert not u_lock.exists

	with FileSpinLock(local_v6_txt, no_lock=True) as lock:
		assert not u_lock.exists
		time.sleep(1)
	assert not u_lock.exists


def test_filespinlock_with_context_raise(local_v6_txt):
	u = AutoURI(local_v6_txt)
	u_lock = AutoURI(local_v6_txt + FileSpinLock.LOCK_FILE_EXT)
	try:
		with FileSpinLock(u, no_lock=False) as lock:
			assert u_lock.exists
			time.sleep(1)
			raise FileSpinLockTestException
	except FileSpinLockTestException:
		assert not u_lock.exists


def test_filespinlock_timeout(local_v6_txt):
	"""Timeout = 3 sec
	Two cases:
		Slow:
			max_polling(3) x sec_polling_interval(1sec)
		Fast: (maybe only for local storage)
			max_polling(300) x sec_polling_interval(0.01sec)
	"""
	u_lock = AutoURI(local_v6_txt + FileSpinLock.LOCK_FILE_EXT)

	lock = FileSpinLock(local_v6_txt, max_polling=3, sec_polling_interval=1)
	lock.acquire()
	try:
		lock.acquire()
		time.sleep(1)
	except RuntimeError:
		pass
	finally:
		lock.release()
		assert not u_lock.exists

	lock = FileSpinLock(local_v6_txt, max_polling=300, sec_polling_interval=0.01)
	lock.acquire()
	try:
		lock.acquire()
		time.sleep(1)
	except RuntimeError:
		pass
	finally:
		lock.release()
		assert not u_lock.exists
