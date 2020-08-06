import hashlib
import os
import time
from typing import Any, Tuple

import pytest

from autouri.autouri import AutoURI, URIBase
from autouri.gcsuri import GCSURI
from autouri.httpurl import HTTPURL, ReadOnlyStorageError

from .files import common_paths, v6_txt_contents


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_uri(path) -> Any:
    assert HTTPURL(path).uri == path


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_uri_wo_ext(path) -> str:
    assert HTTPURL(path).uri_wo_ext == os.path.splitext(path.split("?", 1)[0])[0]


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_uri_wo_scheme(path) -> str:
    assert HTTPURL(path).uri_wo_scheme == path.replace("https://", "", 1).replace(
        "http://", "", 1
    )


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_is_valid(path) -> bool:
    """Also tests AutoURI auto-conversion since it's based on is_valid property
    """
    expected = path.startswith(("https://", "http://"))
    assert HTTPURL(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == HTTPURL


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_dirname(path) -> str:
    assert HTTPURL(path).dirname == os.path.dirname(path)


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_dirname_wo_scheme(path) -> str:
    assert HTTPURL(path).dirname_wo_scheme == os.path.dirname(path).replace(
        "https://", "", 1
    ).replace("http://", "", 1)


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_loc_dirname(path) -> str:
    assert HTTPURL(path).loc_dirname == hashlib.md5(path.encode("utf-8")).hexdigest()


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_basename(path) -> str:
    assert HTTPURL(path).basename == os.path.basename(path).split("?", 1)[0]


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_basename_wo_ext(path) -> str:
    assert (
        HTTPURL(path).basename_wo_ext
        == os.path.splitext(os.path.basename(path).split("?", 1)[0])[0]
    )


@pytest.mark.parametrize("path", common_paths())
def test_httpurl_ext(path) -> str:
    assert HTTPURL(path).ext == os.path.splitext(path.split("?", 1)[0])[1]


def test_httpurl_exists(url_v6_txt):
    assert HTTPURL(url_v6_txt).exists
    assert not HTTPURL(url_v6_txt + ".should-not-be-here").exists
    assert not HTTPURL("http://hey.this.should/not/be/here.txt").exists


def test_httpurl_mtime(gcs_v6_txt, url_v6_txt):
    """Write on GCS bucket which is open to public.
    Access to that file via URL.

    For URLs hosted on GCS, "last-modified" key in the header doesn't seem to be
    accurate for the following cases:
        - After writing on an existing bucket item. GCS object has a correct "update" time
        but corresponding URL's "last-modified" still points to creation time.
    """
    return

    u = GCSURI(gcs_v6_txt + ".tmp")
    if u.exists:
        # if we don't delete it, "last-modified" will be inaccurate (pointing ot creation time).
        # see the above comments in this function's docstring.
        u.rm()
    u.write("temp file for testing")
    u_url = HTTPURL(url_v6_txt + ".tmp")
    now = time.time()
    assert now - 10 < u_url.mtime < now + 10
    u.rm()
    assert not u.exists


def test_httpurl_size(url_v6_txt, v6_txt_size):
    assert HTTPURL(url_v6_txt).size == v6_txt_size


def test_httpurl_md5(url_v6_txt, v6_txt_md5_hash):
    assert HTTPURL(url_v6_txt).md5 == v6_txt_md5_hash


def test_httpurl_md5_from_file(gcs_v6_txt_url, url_v6_txt, v6_txt_md5_hash):
    u_gcs_md5 = GCSURI(gcs_v6_txt_url + URIBase.MD5_FILE_EXT)
    if u_gcs_md5.exists:
        u_gcs_md5.rm()
    u_md5 = HTTPURL(url_v6_txt + URIBase.MD5_FILE_EXT)

    assert not u_gcs_md5.exists
    assert not u_md5.exists
    u = HTTPURL(url_v6_txt)
    assert u.md5_from_file is None

    u.get_metadata(make_md5_file=True)
    # HTTPURL should not make md5 file even with make_md5_file=True
    assert not u_md5.exists
    assert u.md5_from_file is None


def test_httpurl_md5_file_uri(url_v6_txt):
    assert (
        HTTPURL(url_v6_txt + URIBase.MD5_FILE_EXT).uri
        == url_v6_txt + URIBase.MD5_FILE_EXT
    )


def test_httpurl_cp_url(url_v6_txt, url_test_path) -> "AutoURI":
    """Test copying local_v6_txt to the following destination storages:
        url_test_path: url -> url
            This will fail as intended since URL is read-only.
    """
    u = HTTPURL(url_v6_txt)
    basename = os.path.basename(url_v6_txt)

    for test_path in (url_test_path,):
        u_dest = AutoURI(os.path.join(test_path, "test_httpurl_cp", basename))

        with pytest.raises(ReadOnlyStorageError):
            _, ret = u.cp(u_dest, return_flag=True)


def test_httpurl_cp(
    url_v6_txt, local_test_path, s3_test_path, gcs_test_path
) -> "AutoURI":
    """Test copying local_v6_txt to the following destination storages:
        local_test_path: url -> local
        s3_test_path: url -> s3
        gcs_test_path: url -> gcs

    Parameters to be tested:
        no_lock:
            Copy with no locking mechanism. There is no way to test this thoroughly here.
            This will be tested with multiple threads later in test_rece_cond.py.
        no_checksum:
            Don't check md5-hash/size/mtime to skip copying (even if file already exists on destination).
        make_md5_file:
            Make md5 file on destination only when it's REQUIRED.
            It's required only if we need to compare md5 hash of source and target.
    """
    u = HTTPURL(url_v6_txt)
    basename = os.path.basename(url_v6_txt)

    for test_path in (local_test_path, s3_test_path, gcs_test_path):
        u_dest = AutoURI(os.path.join(test_path, "test_httpurl_cp", basename))
        if u_dest.exists:
            u_dest.rm()

        assert not u_dest.exists
        _, ret = u.cp(u_dest, return_flag=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 0
        u_dest.rm()

        assert not u_dest.exists
        # cp without lock will be tested throughly in test_race_cond.py
        _, ret = u.cp(u_dest, no_lock=True, return_flag=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 0
        u_dest.rm()

        # trivial: copy without checksum when target doesn't exists
        assert not u_dest.exists
        _, ret = u.cp(u_dest, no_checksum=True, return_flag=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 0

        # copy without checksum when target exists
        m_dest = u_dest.get_metadata()
        assert m_dest.exists
        time.sleep(1)
        _, ret = u.cp(u_dest, no_checksum=True, return_flag=True)
        # compare new mtime vs old mtime
        # new time should be larger if it's overwritten as intended
        assert u_dest.mtime > m_dest.mtime and u.read() == u_dest.read() and ret == 0

        # copy with checksum when target exists
        m_dest = u_dest.get_metadata()
        assert m_dest.exists
        _, ret = u.cp(u_dest, return_flag=True)
        # compare new mtime vs old mtime
        # new time should be the same as old time
        assert u_dest.mtime == m_dest.mtime and u.read() == u_dest.read() and ret == 1

        # make_md5_file works only when it's required
        # i.e. when we need to compare md5 hash of src vs target
        # so target must exist prior to test it
        assert u_dest.exists
        # delete md5 file if exists
        u_dest_md5_file = AutoURI(u_dest.uri + URIBase.MD5_FILE_EXT)
        if u_dest_md5_file.exists:
            u_dest_md5_file.rm()
        _, ret = u.cp(u_dest, make_md5_file=True, return_flag=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 1
        u_dest.rm()


def test_httpurl_write(url_test_path):
    u = HTTPURL(url_test_path + "/test_httpurl_write.tmp")
    with pytest.raises(ReadOnlyStorageError):
        u.write("test")


def test_httpurl_rm(url_test_path):
    u = HTTPURL(url_test_path + "/test_httpurl_rm.tmp")
    with pytest.raises(ReadOnlyStorageError):
        u.rm()


def test_httpurl_get_metadata(url_v6_txt, v6_txt_size, v6_txt_md5_hash):
    u = HTTPURL(url_v6_txt)

    m1 = u.get_metadata()
    assert m1.md5 == v6_txt_md5_hash
    assert m1.size == v6_txt_size

    m2 = u.get_metadata(skip_md5=True)
    assert m2.md5 is None
    assert m2.size == v6_txt_size

    u_md5 = HTTPURL(url_v6_txt + ".md5")
    if u_md5.exists:
        u_md5.rm()
    m3 = u.get_metadata(make_md5_file=True)
    assert m3.md5 == v6_txt_md5_hash
    assert m3.size == v6_txt_size
    # HTTPURL should not make md5 file even with make_md5_file=True
    assert not u_md5.exists


def test_httpurl_read(url_v6_txt):
    u = HTTPURL(url_v6_txt)
    assert u.read() == v6_txt_contents()
    assert u.read(byte=True) == v6_txt_contents().encode()


def test_httpurl_find_all_files(url_test_path):
    prefix = os.path.join(url_test_path, "test_httpurl_find_all_files")

    with pytest.raises(NotImplementedError):
        HTTPURL(prefix).find_all_files()


def test_httpurl_rmdir(url_test_path):
    prefix = os.path.join(url_test_path, "test_httpurl_rmdir")

    with pytest.raises(NotImplementedError):
        HTTPURL(prefix).rmdir()


# original methods in HTTPURL


# classmethods
def test_httpurl_get_path_sep() -> str:
    assert HTTPURL.get_path_sep() == os.path.sep


def test_httpurl_get_schemes() -> Tuple[str, ...]:
    assert HTTPURL.get_schemes() == ("http://", "https://")


def test_httpurl_get_loc_suffix() -> str:
    assert HTTPURL.get_loc_suffix() == ".url"


def test_httpurl_get_loc_prefix() -> str:
    """HTTPURL is read-only storage and hence loc_prefix == ''
    """
    assert HTTPURL.get_loc_prefix() == ""


def test_httpurl_localize(
    url_test_path, gcs_j1_json, gcs_v41_json, gcs_v421_tsv, gcs_v5_csv, gcs_v6_txt
) -> Tuple[str, bool]:
    """Localize should fail.
    """
    loc_prefix = os.path.join(url_test_path, "test_httpurl_localize")

    for j1_json in (gcs_j1_json,):
        # localization from local storage
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()

        # for localization both with or without recursive
        # nothing should be localized actually
        # since they are already on a local storage
        # so loc_prefix directory itself shouldn't be created
        with pytest.raises(ReadOnlyStorageError):
            loc_uri, localized = HTTPURL.localize(
                u_j1_json, recursive=False, loc_prefix=loc_prefix_
            )
