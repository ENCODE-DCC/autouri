#!/usr/bin/env python3
"""
Author: Jin Lee (leepc12@gmail.com)
"""

import hashlib
import os
import shutil
from filelock import FileLock
from .autouri import AutoURI, AutoURIMetadata, logger


def init_abspath(
    loc_prefix=None):
    """
    Helper function to initialize AbsPath class constants
        loc_prefix:
            Inherited from AutoURI
    """
    if loc_prefix is not None:
        AbsPath.LOC_PREFIX = loc_prefix


class AbsPath(AutoURI):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from AutoURI class.
        MAP_PATH_TO_URL:
            Dict to replace path prefix with URL prefix.
            Useful to convert absolute path into URL on a web server.
        FILELOCK_MAX_POLLING:
            Maximum number of lock file polling (way more than default).
        FILELOCK_SEC_POLLING_INTERVAL:
            Default polling interval in seconds (way more frequent than default).
    """
    MAP_PATH_TO_URL = None
    FILELOCK_MAX_POLLING = 18000
    FILELOCK_SEC_POLLING_INTERVAL = 0.1

    _LOC_SUFFIX = '.local.'
    _OS_SEP = os.sep
    _SCHEME = None

    def __init__(self, uri):
        uri = os.path.expanduser(uri)
        super().__init__(uri, cls=self.__class__)

    @property
    def is_valid(self):
        return os.path.isabs(self._uri)

    @property
    def lock(self):
        """Locking mechanism uses FileSpinLock class with faster polling
        """
        from autouri.filespinlock import FileSpinLock
        return FileSpinLock(
            self,
            max_polling=AbsPath.FILELOCK_MAX_POLLING,
            sec_polling_interval=AbsPath.FILELOCK_SEC_POLLING_INTERVAL)

    def get_metadata(self, make_md5_file=False):
        """md5 hash is not included since it's expensive.
        Call AbsPath.get_md5() separately to get md5 hash
        """
        ex = os.path.exists(self._uri)
        mt, sz, md5 = None, None, None
        if ex:
            mt = os.path.getmtime(self._uri)
            sz = os.path.getsize(self._uri)
            md5 = self.get_md5_from_file(make_md5_file=make_md5_file)
            if md5 is None:
                md5 = hashlib.md5(self._uri).hexdigest()
        return AutoURIMetadata(
            exists=ex,
            mtime=mt,
            size=sz,
            md5=md5)

    def read(self, byte=False):
        if byte:
            param = 'rb'
        else:
            param = 'r'
        with open(self._uri, param) as fp:
            return fp.read()

    def _write(self, s):
        if isinstance(s, str):
            param = 'w'
        else:
            param = 'wb'
        with open(self._uri, param) as fp:
            fp.write(s)
        return

    def _rm(self):
        return os.remove(self._uri)

    def _cp(self, dest_uri):
        """Copy from AbsPath to other classes
        """
        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, AbsPath):            
            dest_uri.mkdir_dirname()
            shutil.copyfile(self._uri, dest_uri._uri, follow_symlinks=True)
            return True

        return None

    def _cp_from(self, src_uri):
        raise NotImplementedError

    def get_mapped_url(self):
        for k, v in AbsPath.MAP_PATH_TO_URL:
            if self._uri.startswith(k):
                return self._uri.replace(k, v, 1)
        raise ValueError('Cannot find a mapping from AbsPath to HTTPURL.')

    def mkdir_dirname(self):
        os.makedirs(self.get_dirname(), exist_ok=True)
        return

    @classmethod
    def can_map_to_url(cls):
        return cls.MAP_PATH_TO_URL is not None
