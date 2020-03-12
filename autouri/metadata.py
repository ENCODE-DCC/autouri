"""URIMetadata and helper functions for metadata
"""
from binascii import hexlify
from base64 import b64decode
from collections import namedtuple
from datetime import datetime
from dateutil.parser import parse as parse_timestamp
from dateutil.tz import tzutc


URIMetadata = namedtuple('URIMetadata', ('exists', 'mtime', 'size', 'md5'))


def get_seconds_from_epoch(timestamp: str) -> float:
    utc_t = parse_timestamp(timestamp)
    utc_epoch = datetime(1970, 1, 1, tzinfo=tzutc())
    return (utc_t - utc_epoch).total_seconds()


def base64_to_hex(b: str) -> str:
    return hexlify(b64decode(b)).decode()


def parse_md5_str(raw: str) -> str:
    """Check if it's based on base64 then convert it to hexadecimal string.
    """
    raw = raw.strip('"\'')
    if len(raw) == 32:
        return raw
    else:
        try:
            return base64_to_hex(raw)
        except:
            return None
