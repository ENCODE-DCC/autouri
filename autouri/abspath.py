#!/usr/bin/env python3
import hashlib
import os
import shutil
from typing import Dict, Optional, Union
from .autouri import URIBase, URIMetadata, AutoURI, logger
from filelock import SoftFileLock


def init_abspath(
    loc_prefix: Optional[str]=None,
    map_path_to_url: Optional[Dict[str, str]]=None,
    filelock_max_polling: Optional[int]=None,
    filelock_sec_polling_interval: Optional[float]=None,
    md5_calc_chunk_size: Optional[int]=None):
    """
    Helper function to initialize AbsPath class constants
        loc_prefix:
            Inherited from URIBase
    """
    if loc_prefix is not None:
        AbsPath.LOC_PREFIX = loc_prefix
    if map_path_to_url is not None:
        AbsPath.MAP_PATH_TO_URL = map_path_to_url
    if filelock_max_polling is not None:
        AbsPath.FILELOCK_MAX_POLLING = filelock_max_polling
    if filelock_sec_polling_interval is not None:
        AbsPath.FILELOCK_SEC_POLLING_INTERVAL = filelock_sec_polling_interval
    if md5_calc_chunk_size is not None:
        AbsPath.MD5_CALC_CHUNK_SIZE = md5_calc_chunk_size


class AbsPath(URIBase):
    """
    Class constants:
        LOC_PREFIX:
            Path prefix for localization. Inherited from URIBase class.
        MAP_PATH_TO_URL:
            Dict to replace path prefix with URL prefix.
            Useful to convert absolute path into URL on a web server.
        FILELOCK_MAX_POLLING:
            Maximum number of lock file polling (way more than default).
        FILELOCK_SEC_POLLING_INTERVAL:
            Default polling interval in seconds (way more frequent than default).

    """
    MAP_PATH_TO_URL: Dict[str, str] = dict()
    FILELOCK_MAX_POLLING: int = 18000
    FILELOCK_SEC_POLLING_INTERVAL: float = 0.1
    MD5_CALC_CHUNK_SIZE: int = 4096

    _LOC_SUFFIX = '.local'
    _PATH_SEP = os.sep

    def __init__(self, uri):
        uri = os.path.expanduser(uri)
        super().__init__(uri)

    @property
    def is_valid(self):
        return os.path.isabs(self._uri)

    def get_lock(self, no_lock=False) -> Union['FileSpinLock', SoftFileLock]:
        """Locking mechanism useing FileSpinLock class with much faster polling
        """
        from .filespinlock import FileSpinLock
        if no_lock:
            return FileSpinLock(self, no_lock=no_lock)
        else:
            u_lock = AutoURI(self._uri + FileSpinLock.LOCK_FILE_EXT)
            u_lock.mkdir_dirname()
            return SoftFileLock(u_lock._uri)

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        """If md5 file doesn't exists then use hashlib.md5() to calculate md5 hash
        """
        ex = os.path.exists(self._uri)
        mt, sz, md5 = None, None, None
        if ex:
            mt = os.path.getmtime(self._uri)
            sz = os.path.getsize(self._uri)
            if not skip_md5:
                md5 = self.md5_from_file
                if md5 is None:
                    md5 = self.__calc_md5sum()
                if make_md5_file:
                    self.md5_file_uri.write(md5)

        return URIMetadata(
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
        self.mkdir_dirname()
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
        return False

    def _cp_from(self, src_uri):
        return False

    def get_mapped_url(self) -> Optional[str]:
        for k, v in AbsPath.MAP_PATH_TO_URL.items():
            if k and self._uri.startswith(k):
                return self._uri.replace(k, v, 1)
        return None

    def mkdir_dirname(self):
        os.makedirs(self.dirname, exist_ok=True)
        return

    def __calc_md5sum(self):
        """Expensive md5 calculation
        """
        hash_md5 = hashlib.md5()
        with open(self._uri, 'rb') as fp:
            for chunk in iter(lambda: fp.read(AbsPath.MD5_CALC_CHUNK_SIZE), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
