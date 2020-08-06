"""URIMetadata and helper functions for metadata
"""
import warnings
from base64 import b64decode
from binascii import hexlify
from collections import namedtuple
from datetime import datetime, timezone

from dateparser import parse as dateparser_parse
from dateutil.parser import parse as dateutil_parse

URIMetadata = namedtuple("URIMetadata", ("exists", "mtime", "size", "md5"))


def get_seconds_from_epoch(timestamp: str) -> float:
    """If dateutil.parser.parse cannot parse DST timezones
    (e.g. PDT, EDT) correctly, then use dateparser.parse instead.
    """
    utc_epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    utc_t = None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            utc_t = dateutil_parse(timestamp)
    except Exception:
        pass
    if utc_t is None or utc_t.tzname() not in ("UTC", "Z"):
        utc_t = dateparser_parse(timestamp)
    utc_t = utc_t.astimezone(timezone.utc)
    return (utc_t - utc_epoch).total_seconds()


def base64_to_hex(b: str) -> str:
    return hexlify(b64decode(b)).decode()


def parse_md5_str(raw: str) -> str:
    """Check if it's based on base64 then convert it to hexadecimal string.
    """
    raw = raw.strip("\"'")
    if len(raw) == 32:
        return raw
    else:
        try:
            return base64_to_hex(raw)
        except Exception:
            pass
