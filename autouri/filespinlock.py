#!/usr/bin/env python3
"""FileSpinLock class

Author: Jin Lee (leepc12@gmail.com) at ENCODE-DCC
"""

from autouri.autouri import AutoURI, logger


DEFAULT_MAX_ITER = 60
DEFAULT_SEC_INTERVAL = 30
DEFAULT_LOCK_FILE_EXT = '.lock'


def init_filespinlock(
    max_iter=DEFAULT_MAX_ITER,
    sec_interval=DEFAULT_SEC_INTERVAL,
    lock_file_ext=DEFAULT_LOCK_FILE_EXT):
    """
    Args:
        max_iter:
            Max iteration for checking a lock file
        sec_interval:
            Interval in seconds to check if a lock file still exists
        lock_file_ext:
            Lock file extension with dot
    """
    FileSpinLock._max_iter = max_iter
    FileSpinLock._sec_interval = sec_interval

    if not lock_file_ext.startswith('.'):
        lock_file_ext = '.' + lock_file_ext
    FileSpinLock._lock_file_ext = lock_file_ext


class FileSpinLock(object):
    """A spin lock based on a lock file (AutoURI)
    """
    _max_iter = DEFAULT_MAX_ITER
    _sec_interval = DEFAULT_SEC_INTERVAL
    _lock_file_ext = DEFAULT_LOCK_FILE_EXT

    def __init__(self, uri):
        if isinstance(uri, str):
            uri = AutoURI(uri)
        elif not isinstance(uri, AutoURI):
            raise ValueError('URI is not valid.')
        self._uri = uri
        self._lock = self.__get_lock_file_uri()

    def acquire(self):
        """Check if a lock file disappears every FileSpinLock._sec_lock_interval seconds
        up to FileSpinLock._max_iter times
        """
        if self._lock.exists():
            i = 0
            while i <= FileSpinLock._max_iter:
                i += 1
                logger.info('Waiting for a lock file to be released... '
                            'iter: {i}, f: {f}'.format(i=i, f=str(self._lock)))
                time.sleep(FileSpinLock._sec_interval)
                if not self._lock.exists()
                    return
            raise Exception(
                'A lock file has not been removed for {m} x {i} seconds. '
                'Possible race condition or dead lock? '
                'Check if another process is holding this lock file and '
                'remove it manually and try again. f: {f}'.format(
                    m=FileSpinLock._max_iter,
                    i=FileSpinLock._sec_interval,
                    f=str(lock)))
        else:
            logger.debug('Creating a lock file. f: {f}'.format(f=str(self._lock)))
            self._lock.write_lockless('')
        return

    def release(self):
        if self._lock.exists():
            lock.rm_lockless()
        else:
            raise Exception('Lock file has been removed by another process. '
                'Possible race condition. f: {f}'.format(f=str(lock)))
        return

    def __get_lock_file_uri():
        return AutoURI(self._uri._uri + FileSpinLock._lock_file_ext)
