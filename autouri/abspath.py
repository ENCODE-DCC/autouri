import errno
import glob
import hashlib
import logging
import os
import shutil
from shutil import SameFileError, copyfile
from typing import Dict, Optional

from filelock import SoftFileLock

from .autouri import AutoURI, URIBase
from .metadata import URIMetadata

logger = logging.getLogger(__name__)


class AbsPath(URIBase):
    """
    Class constants:
        LOC_PREFIX (inherited):
            Path prefix for localization. Inherited from URIBase class.
        MAP_PATH_TO_URL:
            Dict to replace path prefix with URL prefix.
            Useful to convert absolute path into URL on a web server.
        MD5_CALC_CHUNK_SIZE:
            Chunk size to calculate md5 hash of a local file.
    """

    MAP_PATH_TO_URL: Dict[str, str] = dict()
    MD5_CALC_CHUNK_SIZE: int = 4096

    _LOC_SUFFIX = ".local"
    _PATH_SEP = os.sep

    def __init__(self, uri, thread_id=-1):
        if isinstance(uri, str):
            uri = os.path.expanduser(uri)
        super().__init__(uri, thread_id=thread_id)

    @property
    def is_valid(self):
        return os.path.isabs(self._uri)

    def rmdir(self, dry_run=False, no_lock=False):
        """Do `rm -rf` instead of deleting individual files.
        For dry-run mode, call base class' method to show files to be deleted.
        """
        if not os.path.exists(self._uri):
            raise FileNotFoundError(
                "Directory does not exist. deleted already? {dir}".format(dir=self._uri)
            )
        if dry_run:
            super().rmdir(dry_run=True, no_lock=no_lock)
        else:
            shutil.rmtree(self._uri)

    def _get_lock(self, timeout=None, poll_interval=None):
        """Use filelock.SoftFileLock for AbsPath.
        filelock.SoftFileLock watches a .lock file with faster polling.
        It's stable and also platform-independent

        Args:
            poll_interval:
                This is dummy.
                Fixed polling rate defined in BaseFileLock is used.
        """
        if timeout is None:
            timeout = AbsPath.LOCK_TIMEOUT
        # create directory and use default poll_interval
        u_lock = AutoURI(self._uri + AbsPath.LOCK_FILE_EXT)
        u_lock.mkdir_dirname()
        return SoftFileLock(u_lock._uri, timeout=timeout)

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        """If md5 file doesn't exist then use hashlib.md5() to calculate md5 hash
        """
        exists = os.path.exists(self._uri)
        mt, sz, md5 = None, None, None
        if exists:
            mt = os.path.getmtime(self._uri)
            sz = os.path.getsize(self._uri)
            if not skip_md5:
                md5 = self.md5_from_file
                if md5 is None:
                    md5 = self.__calc_md5sum()
                if make_md5_file:
                    self.md5_file_uri.write(md5)

        return URIMetadata(exists=exists, mtime=mt, size=sz, md5=md5)

    def read(self, byte=False):
        if byte:
            param = "rb"
        else:
            param = "r"
        with open(self._uri, param) as fp:
            return fp.read()

    def find_all_files(self):
        query = os.path.join(self._uri, "**")
        result = []
        for f in glob.glob(query, recursive=True):
            if os.path.isfile(f):
                result.append(os.path.abspath(f))
        return result

    def _write(self, s):
        self.mkdir_dirname()
        if isinstance(s, str):
            param = "w"
        else:
            param = "wb"
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
            try:
                copyfile(self._uri, dest_uri._uri, follow_symlinks=True)
            except SameFileError:
                logger.debug(
                    "cp: ignored SameFileError. src={src}, dest={dest}".format(
                        src=self._uri, dest=dest_uri._uri
                    )
                )
                if os.path.islink(dest_uri._uri):
                    dest_uri._rm()
                    copyfile(self._uri, dest_uri._uri, follow_symlinks=True)

            return True
        return False

    def _cp_from(self, src_uri):
        return False

    def get_mapped_url(self, map_path_to_url=None) -> Optional[str]:
        """
        Args:
            map_path_to_url:
                dict with k, v where k is a path prefix and v is a URL prefix
                k will be replaced with v.
                If not given, defaults to use class constant AbsPath.MAP_PATH_TO_URL
        """
        if map_path_to_url is None:
            map_path_to_url = AbsPath.MAP_PATH_TO_URL
        for k, v in map_path_to_url.items():
            if k and self._uri.startswith(k):
                return self._uri.replace(k, v, 1)
        return None

    def mkdir_dirname(self):
        """Create a directory but raise if no write permission on it
        """
        os.makedirs(self.dirname, exist_ok=True)
        if not os.access(self.dirname, os.W_OK):
            raise PermissionError(
                "No permission to write on directory: {d}".format(d=self.dirname)
            )
        return

    def soft_link(self, target, force=False):
        """Make a soft link of self on target absolute path.
        If target already exists delete it and create a link.

        Args:
            target:
                Target file's absolute path or URI object.
            force:
                Delete target file (or link) if it exists
        """
        target = AbsPath(target)
        if not target.is_valid:
            raise ValueError(
                "Target path is not a valid abs path: {t}.".format(t=target.uri)
            )
        try:
            target.mkdir_dirname()
            os.symlink(self._uri, target._uri)
        except OSError as e:
            if e.errno == errno.EEXIST and force:
                target.rm()
                os.symlink(self._uri, target._uri)
            else:
                raise e

    def __calc_md5sum(self):
        """Expensive md5 calculation
        """
        logger.debug(
            "calculating md5sum hash of local file: {file}".format(file=self._uri)
        )
        hash_md5 = hashlib.md5()
        with open(self._uri, "rb") as fp:
            for chunk in iter(lambda: fp.read(AbsPath.MD5_CALC_CHUNK_SIZE), b""):
                hash_md5.update(chunk)

        logger.debug(
            "calculating md5sum is done for local file: {file}".format(file=self._uri)
        )
        return hash_md5.hexdigest()

    @staticmethod
    def get_abspath_if_exists(path):
        if isinstance(path, URIBase):
            path = path._uri
        if isinstance(path, str):
            if os.path.exists(os.path.expanduser(path)):
                return os.path.abspath(os.path.expanduser(path))
        return path

    @staticmethod
    def init_abspath(
        loc_prefix: Optional[str] = None,
        map_path_to_url: Optional[Dict[str, str]] = None,
        md5_calc_chunk_size: Optional[int] = None,
    ):
        if loc_prefix is not None:
            AbsPath.LOC_PREFIX = loc_prefix
        if map_path_to_url is not None:
            AbsPath.MAP_PATH_TO_URL = map_path_to_url
        if md5_calc_chunk_size is not None:
            AbsPath.MD5_CALC_CHUNK_SIZE = md5_calc_chunk_size
