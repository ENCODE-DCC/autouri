import hashlib
import logging
from typing import Optional

import requests

from .autouri import AutoURI, URIBase
from .metadata import URIMetadata, get_seconds_from_epoch, parse_md5_str

logger = logging.getLogger(__name__)


class ReadOnlyStorageError(Exception):
    pass


class HTTPURL(URIBase):
    """
    Class constants:
        HTTP_CHUNK_SIZE:
            Dict to replace path prefix with URL prefix.
            Useful to convert absolute path into URL on a web server.
    """

    HTTP_CHUNK_SIZE: int = 256 * 1024

    _LOC_SUFFIX = ".url"
    _SCHEMES = ("http://", "https://")

    def __init__(self, uri, thread_id=-1):
        super().__init__(uri, thread_id=thread_id)

    @property
    def loc_dirname(self):
        """Dirname of URL is not very meaningful.
        Therefore, hash string of the whole URL string is used instead for localization.
        """
        return hashlib.md5(self._uri.encode("utf-8")).hexdigest()

    @property
    def basename(self):
        """Parses a URL to get a basename.
        This class can only work with a URL with an explicit basename
        which can be suffixed with extra parameters starting with ? only.
        """
        return super().basename.split("?", 1)[0]

    def _get_lock(self, timeout=None, poll_interval=None):
        raise ReadOnlyStorageError(
            "Cannot lock on a read-only storage. {f}".format(f=self._uri)
        )

    def get_metadata(self, skip_md5=False, make_md5_file=False):
        """Known issues about mtime:

        For URLs hosted on GCS (Google Cloud Storage) buckets, mtime will point to creation time
        if GCS object already existed and is modified. mtime of GCS object itself is accurate
        but corresponding URL on a public bucket will still have
        "Last-modified" property which is pointing to creation time.
        """
        exists, mt, sz, md5 = False, None, None, None
        try:
            # get header only
            r = requests.get(
                self._uri,
                stream=True,
                allow_redirects=True,
                headers=requests.utils.default_headers(),
            )
            r.raise_for_status()
            # make keys lower-case
            headers = {k.lower(): v for k, v in r.headers.items()}
            exists = True

            if not skip_md5:
                if "content-md5" in headers:
                    md5 = parse_md5_str(headers["content-md5"])
                elif "x-goog-hash" in headers:
                    hashes = headers["x-goog-hash"].strip().split(",")
                    for hs in hashes:
                        if hs.strip().startswith("md5="):
                            raw = hs.strip().replace("md5=", "", 1)
                            md5 = parse_md5_str(raw)
                if md5 is None and "etag" in headers:
                    md5 = parse_md5_str(headers["etag"])
                if md5 is None:
                    md5 = self.md5_from_file

            if "content-length" in headers:
                sz = int(headers["content-length"])
            elif "x-goog-stored-content-length" in headers:
                sz = int(headers["x-goog-stored-content-length"])

            if "last-modified" in headers:
                mt = get_seconds_from_epoch(headers["last-modified"])

        except requests.exceptions.ConnectionError:
            pass
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 403:
                raise

        return URIMetadata(exists=exists, mtime=mt, size=sz, md5=md5)

    def read(self, byte=False):
        r = requests.get(
            self._uri,
            stream=True,
            allow_redirects=True,
            headers=requests.utils.default_headers(),
        )
        r.raise_for_status()
        b = r.content
        if byte:
            return b
        else:
            return b.decode()

    def find_all_files(self):
        raise NotImplementedError("find_all_files() is not available for URLs.")

    def _write(self, s):
        raise ReadOnlyStorageError(
            "Cannot write on a read-only storage. {f}".format(f=self._uri)
        )

    def _rm(self):
        raise ReadOnlyStorageError(
            "Cannot remove a file on a read-only storage. {f}".format(f=self._uri)
        )

    def _cp(self, dest_uri):
        """Copy from HTTPURL to
        AbsPath
        """
        from autouri.abspath import AbsPath

        dest_uri = AutoURI(dest_uri)

        if isinstance(dest_uri, AbsPath):
            r = requests.get(
                self._uri,
                stream=True,
                allow_redirects=True,
                headers=requests.utils.default_headers(),
            )
            r.raise_for_status()
            dest_uri.mkdir_dirname()
            with open(dest_uri._uri, "wb") as f:
                for chunk in r.iter_content(chunk_size=HTTPURL.HTTP_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
            return True
        return False

    def _cp_from(self, src_uri):
        raise ReadOnlyStorageError(
            "Cannot copy to a read-only storage. {f}".format(f=self._uri)
        )

    @staticmethod
    def get_http_chunk_size() -> int:
        return HTTPURL.HTTP_CHUNK_SIZE

    @staticmethod
    def init_httpurl(http_chunk_size: Optional[int] = None):
        if http_chunk_size is not None:
            HTTPURL.HTTP_CHUNK_SIZE = http_chunk_size
        if HTTPURL.HTTP_CHUNK_SIZE % (256 * 1024) > 0:
            raise ValueError(
                "HTTPURL.HTTP_CHUNK_SIZE must be a multiple of 256 KB (256*1024) "
                "to be compatible with cloud storage APIs (GCS and AWS S3)."
            )
