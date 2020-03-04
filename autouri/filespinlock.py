#!/usr/bin/env python3
from filelock import BaseFileLock, SoftFileLock
from typing import Optional, Union


class AutoURIFileLock(BaseFileLock):
    """Based on filelock.BaseFileLock.
    Args:
        timeout (int)
        poll_interval (float)
        no_lock (bool): if False, then lock becomes a dummy
        exceptions (tuple of Exception): Accepted 
    """
    def __init__(
        self, lock_file, timeout=900, poll_interval=10.0, no_lock=False,
        allowed_exceptions=None):
        super().__init__(lock_file, timeout=timeout)
        self._poll_interval = poll_interval
        self._no_lock = no_lock
        self._allowed_exceptions = allowed_exceptions
        if not self._no_lock:
            if self._allowed_exceptions is None \
                    or not isinstance(self._allowed_exceptions, tuple):
                raise ValueError(
                    'allowed_exceptions must be defined '
                    'and it should be a tuple of Exception.')

    def acquire(self, timeout=None, poll_intervall=0.05):
        """Use self._poll_interval instead of poll_intervall in args
        """
        super().acquire(timeout=timeout, poll_intervall=self._poll_interval)

    def _acquire(self):
        from .autouri import AutoURI

        if self._no_lock:
            self._lock_file_fd = id(self)
            return None
        u_lock = AutoURI(self._lock_file)
        try:
            if not u_lock.exists:
                u_lock.write('', no_lock=True)
        except self._allowed_exceptions:
            pass
        else:
            self._lock_file_fd = id(self)
        return None

    def _release(self):
        from .autouri import AutoURI

        if self._no_lock:
            self._lock_file_fd = None
            return
        u_lock = AutoURI(self._lock_file)
        try:
            u_lock.rm(no_lock=True)
        except self._allowed_exceptions:
            pass
        else:
            self._lock_file_fd = None
        return None
