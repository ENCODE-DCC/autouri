#!/usr/bin/env python3
import time
from typing import Optional, Union
from .autouri import AutoURI, logger


def init_filespinlock(
    max_polling: Optional[int]=None,
    sec_polling_interval: Optional[float]=None,
    lock_file_ext: Optional[str]=None):
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

    Usage-1 (without context):
        lock = FileSpinLock(file_path)
        lock.acquire()
        try:
            # do something
            pass
        except:
            # error handling
            pass
        finally:
            lock.release()

    Usage-2 (with context):
        with FileSpinLock(file_path) as lock:
            # do something
        pass

    Class constants:
        MAX_POLLING:
            Default maximum number of polling
        SEC_POLLING_INTERVAL:
            Default polling interval in seconds (total timeout = MAX_POLLING x SEC_POLLING_INTERVAL)
        LOCK_FILE_EXT
            Lock file extension (.lock)
    """
    MAX_POLLING: int = 180
    SEC_POLLING_INTERVAL: float = 10.0
    LOCK_FILE_EXT: str = '.lock'

    def __init__(
        self, uri: Union[AutoURI, str],
        max_polling: int=None,
        sec_polling_interval: float=None,
        no_lock=False):
        """
        Args:
            max_polling:                
                If not defined use default FileSpinLock.MAX_POLLING
            sec_polling_interval:
                If not defined use default FileSpinLock.SEC_POLLING_INTERVAL
            no_lock:
                Make it a dummy lock (no locking)
        """
        self._uri = uri
        self._no_lock = no_lock
        if self._no_lock:
            return
        self._uri = AutoURI(self._uri)
        if not self._uri.is_valid:
            raise ValueError('URI is not valid.')

        self._lock = self.__get_lock_file_uri()
        if max_polling is None:
            self._max_polling = FileSpinLock.MAX_POLLING
        else:
            self._max_polling = max_polling
        if sec_polling_interval is None:
            self._sec_polling_interval = FileSpinLock.SEC_POLLING_INTERVAL
        else:
            self._sec_polling_interval = sec_polling_interval

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        return None

    def __del__(self):
        self.release()
        return None

    def __str__(self):
        return str(self._uri)

    def __repr__(self):
        return str(self._uri)

    def acquire(self):
        """Check if a lock file disappears every self._sec_lock_interval seconds
        up to self._max_polling times.
        Timeout = self._sec_lock_interval x self._max_polling times.
        """
        if self._no_lock:
            return
        if not self.__try_create_lock_file_if_not_exist():
            i = 0
            while i <= self._max_polling:
                i += 1
                if i == 1:
                    logger.info('Waiting for a lock file to be released... '
                        '{f}'.format(f=str(self._lock)))
                time.sleep(self._sec_polling_interval)
                if self.__try_create_lock_file_if_not_exist():
                    return
            raise RuntimeError(
                'A lock file has been timed out. '
                'It has not been removed for {m} x {i} seconds. '
                'Possible race condition? f: {f}'.format(
                    m=self._max_polling,
                    i=self._sec_polling_interval,
                    f=str(self._lock)))
        return

    def release(self):
        """Release lock.
        """
        if self._no_lock:
            return
        self.__try_delete_lock_file_if_exist()
        return

    def __get_lock_file_uri(self) -> AutoURI:
        """Get lock file URI.
        """
        return AutoURI(self._uri.uri + FileSpinLock.LOCK_FILE_EXT)

    def __try_create_lock_file_if_not_exist(self) -> bool:
        if self._lock.exists:
            return False
        try:
            self._lock.write('', no_lock=True)
        except:
            logger.info('{id}: Failed to create a lock file. {f}'.format(
                id=id(self), f=self._lock))
            raise Exception('{id}: Failed to create a lock file. {f}'.format(
                id=id(self), f=self._lock))
            return False
        logger.info('{id}: Created a lock file. {f}'.format(
            id=id(self), f=self._lock))
        return True

    def __try_delete_lock_file_if_exist(self) -> bool:
        try:
            self._lock.rm(no_lock=True)
        except:
            logger.info('{id}: Failed to delete a lock file. {f}'.format(
                id=id(self), f=self._lock))
            raise Exception('{id}: Failed to delete a lock file. {f}'.format(
                id=id(self), f=self._lock))
            return False
        logger.info('{id}: Deleted a lock file. {f}'.format(
            id=id(self), f=self._lock))
        return True
