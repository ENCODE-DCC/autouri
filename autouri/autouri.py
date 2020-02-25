#!/usr/bin/env python3
"""AutoURI class

Author: Jin Lee (leepc12@gmail.com)
"""

from .uribase import URIBase


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
    def __init__(self, uri):
        super().__init__(uri)
        for c in URIBase.__subclasses__():
            if c is AutoURI:
                continue
            u = c(self._uri)
            if u.is_valid:
                self.__class__ = c
                self._uri = u._uri
                return

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
