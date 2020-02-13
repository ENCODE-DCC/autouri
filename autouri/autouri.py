#!/usr/bin/env python3
"""AutoURI class

Features:    
    - Wraps google-cloud-storage for gs:// URIs.
    - Wraps boto3 for s3:// URIs.
    - Wraps gsutil CLI for direct transfer between gs:// and s3:// URIs.
    - Wraps Python requests for HTTP URLs.
    - Can presign a bucket URI to get a temporary public URL (e.g. for genome browsers).
    - File locking (using .lock file for bucket URIs).
    - MD5 hash checking to prevent unnecessary re-uploading.
    - Localization on a different URI type.
        - Keeping the original directory structure.
        - Can recursively localize all files in a CSV/TSV/JSON(value only) file.

Author: Jin Lee (leepc12@gmail.com) at ENCODE-DCC
"""

from collections import namedtuple
from autouri.loc import get_loc_uri, loc_recurse
import os
import json
import time
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('autouri') 


AutoURIMetadata = namedtuple('AutoURIMetadata', ('exists', 'mtime', 'size', 'md5'))


def init_autouri(
    md5_file_ext=None,
    fn_get_loc_uri=None,
    fn_loc_recurse=None):
    """Helper for initializing AutoURI class constants

    Args:
        md5_file_ext:
            prefixed with dot ('.')
        fn_get_loc_uri:
            function to make a localized URI
        fn_loc_recurse:
            function to define recursive behavior of AutoURI's locationzation
            (AutoURI.localize())
    """
    if md5_file_ext is not None:
        AutoURI.MD5_FILE_EXT = md5_file_ext
    if fn_get_loc_uri is not None:
        AutoURI.FN_GET_LOC_URI = fn_get_loc_uri
    if fn_loc_recurse is not None:
        AutoURI.FN_LOC_RECURSE = fn_loc_recurse


class AutoURI(object):
    """A base class for a URI class. This class is for file only (no directory support).
    This class automatically detects (and converts self into) a URI class from a given URI string.
    It iterates over all IMPORT ED URI classes and find the first one making it valid.

    This class can also work as an undefined URI class,
    which can take/keep any type of variable and it's always read-only and not a valid URI.
    Therefore, you can use this class as a tester to check whether it's a valid URI or not.

    This class can also localize (recursively) an URI on a different URI class.
    A default localization strategy keeps the directory structure and basename of an original URI.
    """
    # protected constants
    _LOC_SUFFIX = None
    _OS_SEP = '/'
    _SCHEME = None
    LOC_PREFIX = None
    # public constants
    MD5_FILE_EXT = '.md5'
    FN_GET_LOC_URI = get_loc_uri
    FN_LOC_RECURSE = loc_recurse

    def __init__(self, uri, cls=None):
        if isinstance(uri, AutoURI):
            self._uri = uri.get_uri()
        else:
            self._uri = uri
        self.__auto_detect(cls=cls)

    def __repr__(self):
        return self._uri

    def __str__(self):
        return str(self._uri)

    def get_uri(self, wo_ext=False):
        if wo_ext:
            return os.path.splitext(self._uri)[0]
        else:
            return self._uri

    def is_valid(self):
        scheme = self.__class__.get_scheme()
        return scheme is not None and self._uri.startswith(scheme)

    def get_dirname(no_scheme=False):
        """Dirname with or without a scheme (gs://, s3://, http://, /, ...).

        Args:
            no_scheme:
                Remove a scheme from URI. 
        """
        dirname = os.path.dirname(self._uri)                
        if no_scheme:
            scheme = self.__class__.get_scheme()
            if isinstance(scheme, str):
                scheme = (scheme,)
            for s in scheme:
                if dirname.startswith(s):
                    dirname.replace(s, '', 1)
                    break
        return dirname

    def get_basename():
        """Basename.
        """
        return os.path.basename(self._uri)

    def get_ext():
        """File extension.
        """
        basename = self.get_basename()
        return os.path.splitext(basename)[1]

    def exists(self):
        return self.get_metadata().exists

    def get_mtime(self):
        """Seconds since the epoch.
        """
        return self.get_metadata().mtime

    def get_size(self): 
        """Size in bytes.
        """
        return self.get_metadata().size

    def get_md5(self):
        """Md5 hash hexadecimal digest string.        
        """
        return self.get_metadata().md5

    def get_md5_from_file(self):
        """Get md5 from a md5 file (.md5) if it exists.
        """
        u_md5 = self.__get_md5_file_uri()
        try:
            if u_md5.is_valid():
                m_md5 = u_md5.get_metadata()
                if m_md5.exists:
                    m = self.get_metadata()
                    if m_md5.mtime >= m.mtime():
                        return u_md5.read()
        except:
            pass

        logger.debug('Failed to get md5 hash from md5 file.')
        return None

    def cp(self, dest_uri, no_lock=False, no_checksum=False, make_md5_file=False):
        """Makes a copy on destination. It is protected by a locking mechanism.
        Check md5 hash, file size and last modified date if possible to prevent
        unnecessary re-uploading.

        Args:
            dest_uri:
                Target URI
            make_md5_file:
                Make an md5 file on destination if metadata doesn't have md5
                assuming that you have write permission on target's directory
        """
        d = AutoURI(dest_uri)

        if not no_checksum:
            # checksum (by md5, size, mdate)
            m_dest = d.get_metadata(make_md5_file=make_md5_file)
            if m_dest.exists():
                m_src = self.get_metadata()

                md5_matched = m_src.md5 is not None and m_dest.md5 is not None and \
                    m_src.md5 == m_dest.md5
                if md5_matched:
                    return d

                size_matched = m_src.size is not None and m_dest.size is not None and \
                    m_src.size == m_dest.size:
                src_is_not_newer = m_src.mtime is not None and m_dest.mtime is not None and \
                    m_src.mtime <= m_dest.mtime:
                if size_matched and src_is_now_newer:
                    return d

        if not no_lock:
            lock = self.get_lock()
            lock.acquire()

        # if src.cp(dest) fails then try dest.cp_from(src)
        if self._cp(dest_uri=d) is None:
            if d._cp_from(src_uri=self) is None:
                raise Exception('cp failed. src: {s} dest: {d}'.format(
                    s=str(self), d=str(d)))

        if not no_lock:
            lock.release()

        return d

    def write(self, s):
        """Write string/bytes to file. It is protected by a locking mechanism.
        """
        lock = self.get_lock()
        lock.acquire()
        self._write(s)
        lock.release()
        return

    def rm(self):
        """Remove a URI from its storage. It is protected by by a locking mechanism.
        """
        lock = self.get_lock()
        lock.acquire()
        self._rm(s)
        lock.release()
        return

    def get_lock(self):
        """Default locking mechanism: FileSpinLock
        """
        from autouri.filespinlock import FileSpinLock
        return FileSpinLock(self)

    def get_metadata(self, make_md5_file=False):
        """Metadata of a URI.
        This is more efficient than individually retrieving each item.
        md5 can be None. For example, HTTP URLs.

        Returns:
            exists:
            mtime: last modified time. seconds from the epoch
            size: bytes
            md5: md5 hexadecimal digest
        """
        raise NotImplementedError

    def read(self, byte=False):
        """Reads string/byte from a URI.
        """
        raise NotImplementedError

    def _write(self, s):
        """Writes string/bytes to a URI. This is NOT protected by a locking mechanism.
        A file lock is already implemented in a higher level AutoURI.write().
        """
        raise NotImplementedError

    def _rm(self):
        """Removes a URI from its storage. This is NOT protected by a locking mechanism.
        A file lock is already implemented in a higher level AutoURI.rm().
        """
        raise NotImplementedError

    def _cp(self, dest_uri):
        """Makes a copy on destination. This is NOT protected by a locking mechanism.
        Also, there is no checksum test for this function.
        A file lock/checksum is already implemented in a higher level AutoURI.cp().
        """
        raise NotImplementedError

    def _cp_from(self, src_uri):
        """Reversed version of "_cp".
        "cp" is a binary operation so it can be defined in either source or destination
        URI class.

        A member function "_cp" is called first and if it returns None
        then this function will be called with reversed source and destination.

        This function is useful to be defined for user's custom classes inherited from 
        AutoURI and existing URI classes.
        """
        raise NotImplementedError

    def __auto_detect(self, cls=None):
        """Detects URI's class by iterating over all sub URI classes
        """
        if self.__class__ is not AutoURI:
            return
        for c in AutoURI.__subclasses__():
            if cls is None or cls is c:
                i = c(self._uri)
                if i.is_valid():
                    self.__class__ = c
                    break
        return

    def __get_md5_file_uri():
        return AutoURI(self.get_path_sep().join([
            self.get_dirname(),
            self.get_basename() + AutoURI.MD5_FILE_EXT]))

    @classmethod
    def get_path_sep(cls):
        """Separator for directory.
        """
        return cls._OS_SEP

    @classmethod
    def get_scheme(cls):
        """Scheme or tuple of schemes.
        e.g. gs://, s3://, (http://, https://), ...
        """
        return cls._SCHEME

    @classmethod
    def get_loc_prefix(cls):
        """Cache directory root path for localization.
        """
        return cls.LOC_PREFIX

    @classmethod
    def get_loc_suffix(cls):
        """File suffix for a MODIFIED file for recursive localization.

        e.g. s3://temp1/tmp.json -> /scratch/cache_dir/tmp.s3.json
        """
        return cls._LOC_SUFFIX

    @classmethod
    def localize(cls, src_uri, make_md5_file=False, recursive=False):
        """Localize a URI on this URI class ("cls")

        Args:
            cls:
                Target URI class
            src_uri:
                Source URI
            make_md5_file:
                Make an md5 file on this if metadata doesn't have md5
                assuming that you have write permission on target's directory and
                its subdirectories recursively.
            recursive:
                Localize all files recursively in specified file extensions.
                Any temporary files suffixed with AutoURI subclass names will 
                always be written on user's LOCAL temporary directory.
        Returns:
            dest_uri:
                Localized URI on this storage
            updated:
                Updated contents in a localized this URI.
                Updated URI is suffixed with this storage type (e.g. .s3.)
                and should be cached in a local storage first due to 
                possible lack of write permission
        """
        updated = False
        dest_uri = cls.FN_GET_LOC_URI(src_uri=src_uri)

        if recursive:            
            assert(AutoURI.FN_LOC_RECURSE is not None)
            AutoURI.FN_LOC_RECURSE(src_uri, cls)
            # def recurse_dict(d, uri_type, d_parent=None, d_parent_key=None,
            #                  lst=None, lst_idx=None, updated=False):
            #     if isinstance(d, dict):
            #         for k, v in d.items():
            #             updated |= recurse_dict(v, uri_type, d_parent=d,
            #                                     d_parent_key=k, updated=updated)
            #     elif isinstance(d, list):
            #         for i, v in enumerate(d):
            #             updated |= recurse_dict(v, uri_type, lst=d,
            #                                     lst_idx=i, updated=updated)
            #     elif isinstance(d, str):
            #         assert(d_parent is not None or lst is not None)
            #         c = AutoURI(d)
            #         new_file, updated_ = c.deepcopy(
            #             uri_type=uri_type, uri_exts=uri_exts)
            #         updated |= updated_
            #         if updated_:
            #             if d_parent is not None:
            #                 d_parent[d_parent_key] = new_file
            #             elif lst is not None:
            #                 lst[lst_idx] = new_file
            #             else:
            #                 raise ValueError('Recursion failed.')
            #         return updated
            #     return updated
            # def recurse_loc(uri, cls):
            raise NotImplementedError

        if isinstance(src_uri, cls):
            return

        if need_to_copy:
            src_uri.cp(dest_uri=dest_uri, make_md5_file=make_md5_file)
        return dest_uri, updated
