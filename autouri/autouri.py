import logging
import multiprocessing
import os
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from filelock import BaseFileLock

from .loc_aux import recurse_csv, recurse_json, recurse_tsv
from .metadata import URIMetadata

logger = logging.getLogger(__name__)


def autouri_rm(uri, thread_id, no_lock=False):
    """Wrapper for AutoURI(uri).rm().
    This function is used for multiprocessing.map() which requires a picklable function
    outside the scope of the class.
    """
    AutoURI(uri, thread_id=thread_id).rm(no_lock=no_lock)


class AutoURIRecursionError(RuntimeError):
    pass


class URIBase(ABC):
    """A base actract class for all URI classes.
    This class is for file only except for the following two methods.
        - list(): Recursively list all files (full path URIs) on a directory.
        - rmdir(): Recursively remove all files on a directory.

    This class can localize (recursively) URI on different URI class.
    (loc. stands for localization.)
    A default localization strategy keeps the directory structure and basename of an original URI.

    Class constants:
        MD5_FILE_EXT:
            File extention for md5 (.md5).
        LOC_RECURSE_EXT_AND_FNC:
            Dict of (file extension, function) to recurse localization.
                e.g. {'.json': recurse_dict, '.tsv': recurse_tsv}
        LOC_RECURSION_DEPTH_LIMIT:
            Limit depth of recursive localization
            to prevent/detect direct/indirect self referencing.
        LOCK_FILE_EXT:
            Lock file's extention (.lock).
        LOCK_TIMEOUT:
            Lock file timeout (-1 for no timeout).
        LOCK_POLL_INTERVAL:
            Lock file polling interval in seconds.

    Class constants for subclasses only
        LOC_PREFIX:
            Cache path prefix for localization on this class' storage.
            This should be '' for this base class
            but must be specified for subclasses.

    Protected class constants:
        _PATH_SEP:
            Separator string for directory.
        _SCHEMES:
            Tuple of scheme strings
        _LOC_SUFFIX:
            Suffix for a localized file
            if file is modified during recursive localization
    """

    MD5_FILE_EXT: str = ".md5"
    LOC_RECURSE_EXT_AND_FNC: Dict[str, Callable] = {
        ".json": recurse_json,
        ".tsv": recurse_tsv,
        ".csv": recurse_csv,
    }

    LOCK_FILE_EXT: str = ".lock"
    LOCK_TIMEOUT: int = 900
    LOCK_POLL_INTERVAL: float = 10.0

    LOC_PREFIX: str = ""
    LOC_RECURSION_DEPTH_LIMIT: int = 10

    DEFAULT_NUM_THREADS = 6

    _PATH_SEP: str = "/"
    _SCHEMES: Tuple[str, ...] = tuple()
    _LOC_SUFFIX: str = ""

    def __init__(self, uri, thread_id=-1):
        if isinstance(uri, URIBase):
            self._uri = uri.uri
        else:
            self._uri = uri
        self._thread_id = thread_id
        self._uuid = str(uuid.uuid4())

    def __repr__(self):
        return self._uri

    def __str__(self):
        return str(self._uri)

    @property
    def thread_id(self):
        """Use for some URI types which are not thread-safe.
        e.g. GCSURI.
        """
        return self._thread_id

    @thread_id.setter
    def thread_id(self, i):
        self._thread_id = i

    @property
    def uuid(self):
        return self._uuid

    @property
    def short_uuid(self):
        return self._uuid[:8]

    @property
    def uri(self) -> Any:
        """Can store any type of variable."""
        return self._uri

    @property
    def uri_wo_ext(self) -> str:
        return os.path.splitext(str(self._uri))[0]

    @property
    def uri_wo_scheme(self) -> str:
        for s in self.__class__.get_schemes():
            if s and str(self._uri).startswith(s):
                return str(self._uri).replace(s, "", 1)
        return str(self._uri)

    @property
    def is_valid(self) -> bool:
        for s in self.__class__.get_schemes():
            if s and str(self._uri).startswith(s):
                return True
        return False

    @property
    def dirname(self) -> str:
        """Dirname with a scheme (gs://, s3://, http://, /, ...)."""
        return os.path.dirname(str(self._uri))

    @property
    def dirname_wo_scheme(self) -> str:
        """Dirname without a scheme (gs://, s3://, http://, /, ...)."""
        return os.path.dirname(self.uri_wo_scheme)

    @property
    def loc_dirname(self) -> str:
        """Dirname to be appended to target cls' LOC_PREFIX after localization.
        Such dirname is stripped with a path separator (e.g. /).

        e.g. localization of src_uri on target cls
            = cls.LOC_PREFIX + src_uri.loc_dirname + src_uri.basename
        """
        return self.dirname_wo_scheme.strip(self.__class__.get_path_sep())

    @property
    def basename(self) -> str:
        """Basename."""
        return os.path.basename(str(self._uri))

    @property
    def basename_wo_ext(self) -> str:
        """Basename without extension."""
        return os.path.splitext(self.basename)[0]

    @property
    def ext(self) -> str:
        """File extension."""
        return os.path.splitext(self.basename)[1]

    @property
    def exists(self) -> bool:
        return self.get_metadata(skip_md5=True).exists

    @property
    def mtime(self) -> float:
        """Seconds since the epoch."""
        return self.get_metadata(skip_md5=True).mtime

    @property
    def size(self) -> int:
        """Size in bytes."""
        return self.get_metadata(skip_md5=True).size

    @property
    def md5(self) -> str:
        """Md5 hash hexadecimal digest string."""
        return self.get_metadata().md5

    @property
    def md5_from_file(self) -> str:
        """Get md5 from a md5 file (.md5) if it exists.
        Check md5 file is newer than the file that it is associated with
        """
        u_md5 = self.md5_file_uri
        if u_md5.is_valid:
            try:
                m_md5 = u_md5.get_metadata(skip_md5=True)
                if m_md5.exists:
                    self_mtime = self.mtime
                    logger.debug(
                        "md5 file exists. mt={mt}, md5_mt={md5_mt}, uri={u}".format(
                            mt=self_mtime, md5_mt=m_md5.mtime, u=self._uri
                        )
                    )
                    if (
                        m_md5.mtime is not None
                        and self_mtime is not None
                        and m_md5.mtime >= self_mtime
                    ):
                        return u_md5.read()
            except Exception:
                pass

        logger.debug(
            "Failed to get md5 hash from md5 file. uri={u}".format(u=self._uri)
        )
        return None

    @property
    def md5_file_uri(self) -> "AutoURI":
        """Get md5 file URI. Not guaranteed to exist"""
        return AutoURI(str(self._uri) + AutoURI.MD5_FILE_EXT)

    def cp(
        self,
        dest_uri: Union[str, "AutoURI"],
        no_lock=False,
        no_checksum=False,
        make_md5_file=False,
        return_flag=False,
    ) -> "AutoURI":
        """Makes a copy on destination. It is protected by a locking mechanism.
        Check md5 hash, file name/size and last modified date if possible to prevent
        unnecessary re-uploading.

        Args:
            dest_uri:
                Target URI.
                If it's an explicit directory with slash (or os.sep) then
                make it a file URI by suffixing self.basename.
            no_lock:
                Do not use a locking mechanism
            no_checksum:
                Do not check md5 hash
            make_md5_file:
                Make an md5 file on destination if metadata doesn't have md5
                assuming that you have write permission on target's directory
                This flag will work only if no_checksum is activated.
                This flag will work only when it's required.
                If source/target storage's get_metadata() can already get md5
                hash then md5 file will not be created.

        Returns:
            Tuple of (s, rc)
                s:
                    URI string of copy on destination
                rc:
                    0:
                        made a copy
                    1:
                        skipped due to same md5 hash
                    2:
                        md5 not found but matched file size and mtime is not newer
        """
        d = AutoURI(dest_uri)
        sep = d.__class__.get_path_sep()
        if d._uri.endswith(sep):
            d = AutoURI(sep.join([d._uri.rstrip(sep), self.basename]))

        logger.info(
            "cp: ({uuid}) started. src={src}, dest={dest}".format(
                uuid=self.short_uuid, src=self._uri, dest=d.uri
            )
        )

        with d.get_lock(no_lock=no_lock):
            if not no_checksum:
                # checksum (by md5, size, mdate)
                m_dest = d.get_metadata(make_md5_file=make_md5_file)
                logger.debug(
                    "cp: ({uuid}) dest metadata={m}, dest={dest}".format(
                        uuid=self.short_uuid, m=m_dest, dest=d.uri
                    )
                )

                if m_dest.exists:
                    m_src = self.get_metadata()
                    logger.debug(
                        "cp: ({uuid}) src metadata={m}, src={src}".format(
                            uuid=self.short_uuid, m=m_src, src=self._uri
                        )
                    )

                    md5_matched = (
                        m_src.md5 is not None
                        and m_dest.md5 is not None
                        and m_src.md5 == m_dest.md5
                    )
                    if md5_matched:
                        logger.info(
                            "cp: ({uuid}) skipped due to md5_match. "
                            "md5={md5}".format(uuid=self.short_uuid, md5=m_src.md5)
                        )
                        return (d._uri, 1) if return_flag else d._uri

                    name_matched = self.basename == d.basename
                    size_matched = (
                        m_src.size is not None
                        and m_dest.size is not None
                        and m_src.size == m_dest.size
                    )
                    src_is_not_newer = (
                        m_src.mtime is not None
                        and m_dest.mtime is not None
                        and m_src.mtime <= m_dest.mtime
                    )
                    if name_matched and size_matched and src_is_not_newer:
                        logger.info(
                            "cp: ({uuid}) skipped due to name_size_match. "
                            "size={sz}, mt={mt}".format(
                                uuid=self.short_uuid, sz=m_src.size, mt=m_src.mtime
                            )
                        )
                        return (d._uri, 2) if return_flag else d._uri

            if not self._cp(dest_uri=d):
                if not d._cp_from(src_uri=self):
                    raise Exception("cp: ({uuid}) failed.".format(uuid=self.short_uuid))

        logger.info("cp: ({uuid}) done.".format(uuid=self.short_uuid))
        return (d._uri, 0) if return_flag else d._uri

    def write(self, s, no_lock=False):
        """Write string/bytes to file. It is protected by a locking mechanism."""
        with self.get_lock(no_lock=no_lock):
            self._write(s)
        return

    def rm(self, no_lock=False, silent=False):
        """Remove a URI from its storage. It is protected by by a locking mechanism."""
        with self.get_lock(no_lock=no_lock):
            self._rm()
            if not silent:
                logger.info(
                    "rm: ({uuid}) {uri}".format(uuid=self.short_uuid, uri=self._uri)
                )
        return

    def rmdir(self, dry_run=False, num_threads=DEFAULT_NUM_THREADS, no_lock=False):
        """Recursively delete all files on a directory.
        Use it at your own risk. This method is multi-threaded.

        Args:
            dry_run:
                Dry-run.
            num_threads:
                Number of threads for deletion.
        """
        files = self.find_all_files()
        if dry_run:
            for uri in files:
                logger.info(
                    "rm (dry-run): ({uuid}) {uri}".format(uuid=self.short_uuid, uri=uri)
                )
            return
        num_files = len(files)
        thread_ids = [i % num_threads for i in range(num_files)]
        no_locks = [no_lock] * num_files

        args = list(zip(files, thread_ids, no_locks))
        with multiprocessing.Pool(num_threads) as p:
            p.starmap(autouri_rm, args)

    def get_lock(self, no_lock=False, timeout=None, poll_interval=None) -> BaseFileLock:
        """
        Args:
            no_lock: make it a dummy lock (for better code readability for context)
        """
        if no_lock:
            return contextmanager(lambda: (yield))()
        else:
            return self._get_lock(timeout=timeout, poll_interval=poll_interval)

    def localize_on(
        self,
        loc_prefix,
        recursive=False,
        make_md5_file=False,
        return_flag=False,
        depth=0,
        no_lock=False,
        no_checksum=False,
    ) -> Tuple[str, bool]:
        """Wrapper for classmethod localize().
        Localizes self on target directory loc_prefix.
        """
        return AutoURI.localize(
            src_uri=self,
            recursive=recursive,
            make_md5_file=make_md5_file,
            loc_prefix=loc_prefix,
            return_flag=return_flag,
            depth=depth,
            no_lock=no_lock,
            no_checksum=no_checksum,
        )

    @abstractmethod
    def _get_lock(self, timeout=None, poll_interval=None) -> BaseFileLock:
        """Locking mechanism with "with" context.

        Args:
            timeout: timeout in seconds
            poll_interval (float): polling interval in seconds
        """
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, skip_md5=False, make_md5_file=False) -> URIMetadata:
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
        """Reads string/byte from a URI."""
        raise NotImplementedError

    @abstractmethod
    def find_all_files(self, recursive=False) -> List[str]:
        """Recursively list all files (with full path/URI) on a directory.
        This method is available for a directory only and does not list sub-directories.
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
    def _cp(self, dest_uri: Union[str, "AutoURI"]) -> bool:
        """Makes a copy on destination. This is NOT protected by a locking mechanism.
        Also, there is no checksum test for this function.
        A file lock/checksum is already implemented in a higher level AutoURI.cp().
        """
        raise NotImplementedError

    @abstractmethod
    def _cp_from(self, src_uri: Union[str, "AutoURI"]) -> bool:
        """Reversed version of "_cp".
        _cp is a binary operation so it can be defined in either source or destination
        URI class.

        A member function "_cp" is called first and if it returns False
        then this function will be called with reversed source and destination.

        This function is useful to be defined for user's custom classes inherited from
        AutoURI and existing URI classes.
        """
        raise NotImplementedError

    @classmethod
    def get_path_sep(cls) -> str:
        """Separator for directory."""
        return cls._PATH_SEP

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
    def localize(
        cls,
        src_uri,
        recursive=False,
        make_md5_file=False,
        loc_prefix=None,
        return_flag=False,
        depth=0,
        no_lock=False,
        no_checksum=False,
    ) -> Tuple[str, bool]:
        """Localize a source URI on this URI class (cls).

        Recursive localization is supported for the following file extensions:
            .json:
                Files defined only in values (not keys) can be recursively localized.
            .tsv/.csv:
                Files defined in all values can be recursively localized.
            Other extensions can be added through
                a global function init_uribase(loc_recurse_ext_and_fnc=).
                or a class constant LOC_RECURSE_EXT_AND_FNC directly.
                See loc_aux.py to define your own recursion function
                for a specific extension.
        Args:
            src_uri:
                Source URI
            recursive:
                Localize all files recursively in specified TEXT file extensions.
            make_md5_file:
                Make an md5 file on this if metadata doesn't have md5
                assuming that you have write permission on target's directory and
                its subdirectories recursively.
            loc_prefix:
                If defined, use it instead of cls.get_loc_prefix()
            return_flag:
                Returns a tuple of (localized uri string, flag)
                instead of localized uri string only
                    See "Returns" section for details about flag
            depth:
                To count recursion depth.
            no_lock:
                No file locking.
            no_checksum:
                Do not check md5 hash.
        Returns:
            loc_uri:
                Localized URI STRING (not a AutoURI instance) since it should be used
                for external function as a callback function.
            flag: (only if return_flag is on)
                Whether file is modified or localized on a different storage.
                "modified" means:
                    file contents are modified during recursive localization
                    so localized file is suffixed with
                    source's storage type. e.g. .s3, .gcs, and .local
                    not modified:
                "localized on a different storage" means:
                    file contents are NOT modified so localized file is not suffixed
                    and hence will keep the original file basename
                    but localiziation actually happened
                Otherwise, this flag will be False, which means that localization didn't
                happen because file's contents didn't change
                and it exists on the same storage so there is no need for localization.
                In this case, loc_uri will be identical to self._uri.
        """
        src_uri = AutoURI(src_uri)
        if not src_uri.is_valid:
            return (src_uri._uri, False) if return_flag else src_uri._uri

        if depth >= AutoURI.LOC_RECURSION_DEPTH_LIMIT:
            raise AutoURIRecursionError(
                "Maximum recursion depth {m} exceeded. "
                "Possible direct/indirect self-reference while "
                "recursive localization? related file: {f}".format(m=depth, f=src_uri)
            )

        if loc_prefix is None:
            loc_prefix = cls.get_loc_prefix()
        else:
            cls = AutoURI(loc_prefix).__class__
            loc_prefix = loc_prefix.rstrip(cls.get_path_sep())
        if not loc_prefix:
            raise ValueError("LOC_PREFIX is not defined.")

        # check if src and dest are on the same storage
        # to skip localization,
        # which means making a (possily modified) copy of original file
        on_different_storage = cls is not src_uri.__class__

        modified = False
        if recursive:

            def fnc_loc(uri):
                return cls.localize(
                    uri,
                    recursive=recursive,
                    make_md5_file=make_md5_file,
                    loc_prefix=loc_prefix,
                    return_flag=True,
                    depth=depth + 1,
                    no_lock=no_lock,
                    no_checksum=no_checksum,
                )

            for ext, fnc_recurse in AutoURI.LOC_RECURSE_EXT_AND_FNC.items():
                if src_uri.ext == ext:
                    # read source contents for recursive localization
                    src_contents = src_uri.read()
                    maybe_modified_contents, modified = fnc_recurse(
                        src_contents, fnc_loc
                    )
                    break

        if modified:
            # if modified, always suffix basename (before extension) with target storage cls
            basename = src_uri.basename_wo_ext + cls.get_loc_suffix() + src_uri.ext
            dirname = src_uri.loc_dirname
            loc_uri = cls.get_path_sep().join([loc_prefix, dirname, basename])
            AutoURI(loc_uri).write(maybe_modified_contents, no_lock=no_lock)

        elif on_different_storage:
            basename = src_uri.basename
            dirname = src_uri.loc_dirname

            loc_uri = cls.get_path_sep().join([loc_prefix, dirname, basename])
            src_uri.cp(
                dest_uri=loc_uri,
                make_md5_file=make_md5_file,
                no_lock=no_lock,
                no_checksum=no_checksum,
            )
        else:
            loc_uri = src_uri._uri

        return (loc_uri, modified or on_different_storage) if return_flag else loc_uri

    @staticmethod
    def init_uribase(
        md5_file_ext: Optional[str] = None,
        loc_recurse_ext_and_fnc: Optional[Dict[str, Callable]] = None,
        loc_recursion_depth_limit: Optional[int] = None,
        lock_file_ext: Optional[str] = None,
        lock_timeout: Optional[int] = None,
        lock_poll_interval: Optional[float] = None,
    ):
        if md5_file_ext is not None:
            URIBase.MD5_FILE_EXT = md5_file_ext
        if loc_recurse_ext_and_fnc is not None:
            URIBase.LOC_RECURSE_EXT_AND_FNC = loc_recurse_ext_and_fnc
        if loc_recursion_depth_limit is not None:
            URIBase.LOC_RECURSION_DEPTH_LIMIT = loc_recursion_depth_limit
        if lock_file_ext is not None:
            URIBase.LOCK_FILE_EXT = lock_file_ext
        if lock_timeout is not None:
            URIBase.LOCK_TIMEOUT = lock_timeout
        if lock_poll_interval is not None:
            URIBase.LOCK_POLL_INTERVAL = lock_poll_interval


class AutoURI(URIBase):
    """This class automatically detects and converts
    self into a URI class from a given URI string.
    It iterates over all IMPORTED URI subclasses and
    find the first one making it valid.

    This class can also work as an undefined URI class
    which can take/keep any type of variable and
    it's always read-only and not a valid URI.
    Therefore, you can use this class as a tester
    to check whether it's a valid URI or not.
    """

    def __init__(self, uri, thread_id=-1):
        super().__init__(uri, thread_id=thread_id)
        for c in URIBase.__subclasses__():
            if c is AutoURI:
                continue
            u = c(self._uri, thread_id=thread_id)
            if u.is_valid:
                self.__class__ = c
                self._uri = u._uri
                return

    def _get_lock(self, timeout=None, poll_interval=None):
        self.__raise_value_error()

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        self.__raise_value_error()

    def read(self, byte=False):
        self.__raise_value_error()

    def find_all_files(self):
        self.__raise_value_error()

    def rmdir(self):
        self.__raise_value_error()

    def _write(self, s):
        self.__raise_value_error()

    def _rm(self):
        self.__raise_value_error()

    def _cp(self, dest_uri):
        self.__raise_value_error()

    def _cp_from(self, src_uri):
        self.__raise_value_error()

    def __raise_value_error(self):
        raise ValueError("Not a valid URI?. {f}".format(f=self._uri))
