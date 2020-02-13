#!/usr/bin/env python3
"""
Author: Jin Lee (leepc12@gmail.com)
"""

from autouri.autouri import AutoURI, AutoURIMetadata, logger
import os
import shutil
import hashlib
from filelock import FileLock


class AbsPath(AutoURI):
    # protected constants
    _LOC_SUFFIX = '.local.'
    _OS_SEP = os.sep
    _SCHEME = None
    # public constants
    MAP_PATH_TO_URL = None

    def __init__(self, uri):
        uri = os.path.expanduser(uri)
        super().__init__(uri, cls=self.__class__)

    def is_valid(self):
        return os.path.isabs(self._uri)

    def get_lock(self):
        """For AbsPath, use Python package FileLock instead of .lock
        """
        return FileLock(self._uri)

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
        """Copy from AbsPath to 
            AbsPath            
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
