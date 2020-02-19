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

Author: Jin Lee (leepc12@gmail.com)
"""

import hashlib
import logging
import os
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List, Tuple, Callable, Union
from .loc_aux import recurse_json, recurse_tsv, recurse_csv


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('autouri') 


AutoURIMetadata = namedtuple('AutoURIMetadata', ('exists', 'mtime', 'size', 'md5'))


def init_autouri(
    md5_file_ext: str=None,
    loc_recurse_ext_and_fnc: List[Tuple[str, Callable]]=None):
    """Helper for initializing AutoURI class constants
    """
    if md5_file_ext is not None:
        AutoURI.MD5_FILE_EXT = md5_file_ext
    if loc_recurse_ext_and_fnc is not None:
        AutoURI.LOC_RECURSE_EXT_AND_FNC = loc_recurse_ext_and_fnc


class AutoURI(ABC):
    """A base class for URI class. This class is for file only (no support for directory).
    This class automatically detects (and converts self into) URI class from a given URI string.
    It iterates over all IMPORTED URI subclasses and find the first one making it valid.

    This class can also work as an undefined URI class
    which can take/keep any type of variable and it's always read-only and not a valid URI.
    Therefore, you can use this class as a tester to check whether it's a valid URI or not.

    This class can also localize (recursively) URI on different URI class.
    A default localization strategy keeps the directory structure and basename of an original URI.

    Class constants:
        MD5_FILE_EXT:
            File extention for md5 (.md5).
        LOC_RECURSE_EXT_AND_FNC:
            List of tuples of file extension/function to recurse localization.
                e.g. [('.json', recurse_dict), ('.tsv', recurse_tsv), ...]
        LOC_PREFIX:
            Cache path prefix for localization on this class' storage.
            This should be None for this base class but must be specified for subclasses.

    Protected class constants:
        _OS_SEP:
            Separator for directory.
        _SCHEMES:
            Scheme strings (tuple of str)
        _LOC_SUFFIX:
            Suffix after recursive localization if file is modified
    """
    MD5_FILE_EXT = '.md5'
    LOC_RECURSE_EXT_AND_FNC: List[Tuple[str, Callable]] = [
        ('.json', recurse_json),
        ('.tsv', recurse_tsv),
        ('.csv', recurse_csv)
    ]
    LOC_PREFIX = ''

    _OS_SEP = '/'
    _SCHEMES = tuple()
    _LOC_SUFFIX = ''

    def __init__(self, uri, cls=None):
        """
        Args:
            cls:
                Class type can be specified to bypass automatic subclass type detection
        """
        if isinstance(uri, AutoURI):
            self._uri = uri.uri
        else:
            self._uri = uri
        self.__auto_detect(cls=cls)

    def __repr__(self):
        return self._uri

    def __str__(self):
        return str(self._uri)

    @property
    def uri(self):
        return self._uri

    @property
    def uri_wo_ext(self) -> str:
        return os.path.splitext(self._uri)[0]

    @property
    def uri_wo_scheme(self) -> str:
        for s in self.__class__.get_schemes():
            if s and self._uri.startswith(s):
                return self._uri.replace(s, '', 1)
        return self._uri

    @property
    def is_valid(self) -> bool:
        for s in self.__class__.get_schemes():
            if s and self._uri.startswith(s):
                return True
        return False

    @property
    def dirname(self) -> str:
        """Dirname with a scheme (gs://, s3://, http://, /, ...).
        """
        return os.path.dirname(self._uri)

    @property
    def dirname_wo_scheme(self) -> str:
        """Dirname without a scheme (gs://, s3://, http://, /, ...).
        """
        return os.path.dirname(self.uri_wo_scheme)

    @property
    def loc_dirname(self) -> str:
        """Dirname to be appended to target cls' LOC_PREFIX after localization.
    
        e.g. localization of src_uri on target cls
            = cls.LOC_PREFIX + src_uri.loc_dirname + src_uri.basename
        """
        return self.dirname_wo_scheme

    @property
    def basename(self) -> str:
        """Basename.
        """
        return os.path.basename(self._uri)

    @property
    def basename_wo_ext(self) -> str:
        """Basename without extension.
        """
        return os.path.splitext(self.basename)[0]

    @property
    def ext(self) -> str:
        """File extension.
        """
        return os.path.splitext(self.basename)[1]

    @property
    def exists(self) -> bool:
        return self.get_metadata(skip_md5=True).exists

    @property
    def mtime(self) -> float:
        """Seconds since the epoch.
        """
        return self.get_metadata(skip_md5=True).mtime

    @property
    def size(self) -> int: 
        """Size in bytes.
        """
        return self.get_metadata(skip_md5=True).size

    @property
    def md5(self) -> str:
        """Md5 hash hexadecimal digest string.        
        """
        return self.get_metadata().md5

    @property
    def md5_from_file(self) -> str:
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

    @property
    def lock(self) -> FileSpinLock:
        """Default locking mechanism using FileSpinLock class
        """
        from .filespinlock import FileSpinLock
        return FileSpinLock(self)

    def cp(self, dest_uri, no_lock=False, no_checksum=False, make_md5_file=False) -> AutoURI:
        """Makes a copy on destination. It is protected by a locking mechanism.
        Check md5 hash, file size and last modified date if possible to prevent
        unnecessary re-uploading.

        Args:
            dest_uri:
                Target URI
            no_lock:
                Do not use a locking mechanism
            no_checksum:
                Do not check md5 hash
            make_md5_file:
                Make an md5 file on destination if metadata doesn't have md5
                assuming that you have write permission on target's directory
        Returns:
            Copy on destination
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
            self.lock.acquire()

        # if src.cp(dest) fails then try dest.cp_from(src)
        try:
            if not self._cp(dest_uri=d):
                d._cp_from(src_uri=self)
        except:
            raise Exception('cp failed. src: {s} dest: {d}'.format(
                s=str(self), d=str(d)))

        if not no_lock:
            self.lock.release()

        return d

    def write(self, s):
        """Write string/bytes to file. It is protected by a locking mechanism.
        """
        self.lock.acquire()
        self._write(s)
        self.lock.release()
        return

    def rm(self):
        """Remove a URI from its storage. It is protected by by a locking mechanism.
        """
        self.lock.acquire()
        self._rm(s)
        self.lock.release()
        return

    @abstractmethod
    def get_metadata(self, skip_md5=False, make_md5_file=False) -> AutoURIMetadata:
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

    @abstractmethod
    def read(self, byte=False) -> Union[str, bytes]:
        """Reads string/byte from a URI.
        """
        raise NotImplementedError

    @abstractmethod
    def _write(self, s):
        """Writes string/bytes to a URI. This is NOT protected by a locking mechanism.
        A file lock is already implemented in a higher level AutoURI.write().
        """
        raise NotImplementedError

    @abstractmethod
    def _rm(self):
        """Removes a URI from its storage. This is NOT protected by a locking mechanism.
        A file lock is already implemented in a higher level AutoURI.rm().
        """
        raise NotImplementedError

    @abstractmethod
    def _cp(self, dest_uri) -> bool:
        """Makes a copy on destination. This is NOT protected by a locking mechanism.
        Also, there is no checksum test for this function.
        A file lock/checksum is already implemented in a higher level AutoURI.cp().
        """
        raise NotImplementedError

    @abstractmethod
    def _cp_from(self, src_uri) -> bool:
        """Reversed version of "_cp".
        _cp is a binary operation so it can be defined in either source or destination
        URI class.

        A member function "_cp" is called first and if it returns False
        then this function will be called with reversed source and destination.

        This function is useful to be defined for user's custom classes inherited from 
        AutoURI and existing URI classes.
        """
        raise NotImplementedError

    def __auto_detect(self, cls=None):
        """Detects and set URI's class by iterating over all sub URI classes.
        """
        if self.__class__ is not AutoURI:
            return
        for c in AutoURI.__subclasses__():
            if c is NoURI:
                continue
            if cls is None or cls is c:
                i = c(self._uri)
                if i.is_valid():
                    self.__class__ = c
                    return
        self.__class__ = NoURI
        return

    def __get_md5_file_uri() -> AutoURI:
        """Get md5 file URI.
        """
        return AutoURI(self._uri + AutoURI.MD5_FILE_EXT)

    @classmethod
    def get_path_sep(cls) -> str:
        """Separator for directory.
        """
        return cls._OS_SEP

    @classmethod
    def get_schemes(cls) -> Tuple[str, ...]:
        """Tuple of scheme strings.

        e.g. (gs://,), (s3://,), (http://, https://), tuple()
        """
        return cls._SCHEMES

    @classmethod
    def get_loc_suffix(cls) -> str:
        """File suffix for a MODIFIED file after recursive localization.
        This is required to distinguish a modified file from an original one.

        e.g. s3://temp1/tmp.json -> /scratch/cache_dir/tmp.s3.json
        """
        return cls._LOC_SUFFIX

    @classmethod
    def get_loc_prefix(cls) -> str:
        """Cache directory root path for localization.
        Tailing slash will be removed.
        """
        return cls.LOC_PREFIX.rstrip(cls.get_path_sep())

    @classmethod
    def localize(cls, src_uri, make_md5_file=False, recursive=False) -> Tuple[str, bool]:
        """Localize a URI on this URI class (cls).

        Args:
            src_uri:
                Source URI
            make_md5_file:
                Make an md5 file on this if metadata doesn't have md5
                assuming that you have write permission on target's directory and
                its subdirectories recursively.
            recursive:
                Localize all files recursively in specified TEXT file extensions.
        Returns:
            loc_uri:
                Localized URI STRING (not a AutoURI instance) since it should be used
                for external function as a callback function.
            modified:
                Whether localized file is modified or not.
                Modified URI is suffixed with this cls' storage type (e.g. .s3.).
        """
        src_uri = AutoURI(src_uri)

        # check if src and dest are on the same storage to skip localization (in most cases)
        on_different_storage = cls is not src_uri.__class__

        modified = False
        if recursive:
        	# use cls.localize() itself as a callback fnc in recursion
            fnc_loc = lambda x: cls.localize(x, make_md5_file=make_md5_file, recursive=recursive)
            for ext, fnc_recurse in AutoURI.LOC_RECURSE_EXT_AND_FNC:
                if src_uri.ext == ext:
		            # read source contents for recursive localization
                    src_contents = src_uri.read()
                    maybe_modified_contents, modified = fnc_recurse(src_contents, fnc_loc)
                    break

        if modified:
            # if modified, always suffix basename (before extension) with target storage cls
            basename = src_uri.basename_wo_ext + cls.get_loc_suffix() + src_uri.ext
            if on_different_storage:
                dirname = src_uri.loc_dirname
            else:
                # Use a hashed directory name since sometimes
                # we don't have write permission on this directory
                dirname = hashlib.md5(src_uri.uri.encode('utf-8')).hexdigest()

            loc_uri = cls.get_path_sep().join([cls.get_loc_prefix(), dirname, basename])
            AutoURI(loc_uri).write(maybe_modified_contents)

        elif on_different_storage:            
            basename = src_uri.basename
            dirname = src_uri.loc_dirname

            loc_uri = cls.get_path_sep().join([cls.get_loc_prefix(), dirname, basename])
            src_uri.cp(dest_uri=loc_uri, make_md5_file=make_md5_file)
        else:
            # do nothing
            loc_uri = src_uri.uri

        return loc_uri, modified


class NoURI(AutoURI):
    """Trivial class to represent all non-AutoURI types.
    This class is useful just to store value in self._uri.
    """
    def __init__(self, uri):
        super().__init__(uri, cls=self.__class__)

    def get_metadata(self, make_md5_file=False):
        raise NotImplementedError

    def read(self, byte=False):
        raise NotImplementedError

    def _write(self, s):
        raise NotImplementedError

    def _rm(self):
        raise NotImplementedError

    def _cp(self, dest_uri):
        raise NotImplementedError

    def _cp_from(self, src_uri):
        raise NotImplementedError
