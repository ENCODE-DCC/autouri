#!/usr/bin/env python3
"""FileSpinLock class

Author: Jin Lee (leepc12@gmail.com)
"""

from .autouri import AutoURI, logger


def init_filespinlock(
    max_polling=None,
    sec_polling_interval=None,
    lock_file_ext=None):
    """Helper function to initialize FileSpinLock class constants
    """
    if max_polling is not None:
        FileSpinLock.MAX_POLLING = max_polling
    if sec_polling_interval is not None:
        FileSpinLock.SEC_POLLING_INTERVAL = sec_polling_interval
    if lock_file_ext is not None:
        FileSpinLock.LOCK_FILE_EXT = lock_file_ext


class FileSpinLock(object):
    """A spin lock based on a lock file (AutoURI)

    Class constants:
        MAX_POLLING:
            Default maximum number of polling
        SEC_POLLING_INTERVAL:
            Default polling interval in seconds (total timeout = MAX_POLLING x SEC_POLLING_INTERVAL)
        LOCK_FILE_EXT
            Lock file extension (.lock)
    """
    MAX_POLLING: int = 60
    SEC_POLLING_INTERVAL: float = 30.0
    LOCK_FILE_EXT: str = '.lock'

    def __init__(self, uri, max_polling=None, sec_polling_interval=None):
        """
        Args:
            max_polling:                
                If not defined use default FileSpinLock.MAX_POLLING
            sec_polling_interval:
                If not defined use default FileSpinLock.SEC_POLLING_INTERVAL
        """
        if isinstance(uri, str):
            uri = AutoURI(uri)
        elif not isinstance(uri, AutoURI):
            raise ValueError('URI is not valid.')

        self._uri = uri
        self._lock = self.__get_lock_file_uri()
        if max_polling is None:
            self._max_polling = FileSpinLock.MAX_POLLING
        else:
            self._max_polling = max_polling
        if sec_polling_interval is None:
            self._sec_polling_interval = FileSpinLock.SEC_POLLING_INTERVAL
        else:
            self._sec_polling_interval = sec_polling_interval


    def acquire(self):
        """Check if a lock file disappears every self._sec_lock_interval seconds
        up to self._max_polling times.
        """
        if self._lock.exists():
            i = 0
            while i <= self._max_polling:
                i += 1
                if i == 1:
                    logger.info('Waiting for a lock file to be released... '
                        '{f}'.format(f=str(self._lock)))
                time.sleep(self._sec_polling_interval)
                if not self._lock.exists()
                    return
            raise Exception(
                'A lock file has not been removed for {m} x {i} seconds. '
                'Possible race condition or dead lock? '
                'Check if another process is holding this lock file and '
                'remove it manually and try again. {f}'.format(
                    m=self._max_polling,
                    i=self._sec_polling_interval,
                    f=str(lock)))
        else:
            logger.debug('Creating a lock file... {f}'.format(f=str(self._lock)))
            self._lock.write_lockless('')
        return

    def release(self):
        """Release lock.
        """
        if self._lock.exists():
            lock.rm_lockless()
        else:
            raise Exception('Lock file has been removed by another process. '
                'Possible race condition. f: {f}'.format(f=str(lock)))
        return

    def __get_lock_file_uri() -> AutoURI:
        """Get lock file URI.
        """
        return AutoURI(self._uri.uri + FileSpinLock.LOCK_FILE_EXT)
