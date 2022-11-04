import os
import time
from typing import Any, Tuple

import pytest
from requests.exceptions import HTTPError

from autouri.autouri import AutoURI, URIBase
from autouri.httpurl import HTTPURL, ReadOnlyStorageError
from autouri.s3uri import S3URI

from .files import (
    common_paths,
    make_files_in_dir,
    recurse_raise_if_uri_not_exist,
    v6_txt_contents,
)


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_uri(path) -> Any:
    assert S3URI(path).uri == path


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_uri_wo_ext(path) -> str:
    assert S3URI(path).uri_wo_ext == os.path.splitext(path)[0]


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_uri_wo_scheme(path) -> str:
    assert S3URI(path).uri_wo_scheme == path.replace("s3://", "", 1)


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_is_valid(path) -> bool:
    """Also tests AutoURI auto-conversion since it's based on is_valid property"""
    expected = path.startswith("s3://")
    assert S3URI(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == S3URI


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_dirname(path) -> str:
    assert S3URI(path).dirname == os.path.dirname(path)


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_dirname_wo_scheme(path) -> str:
    assert S3URI(path).dirname_wo_scheme == os.path.dirname(path).replace(
        "s3://", "", 1
    )


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_loc_dirname(path) -> str:
    assert S3URI(path).loc_dirname == os.path.dirname(path).replace(
        "s3://", "", 1
    ).lstrip("/")


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_basename(path) -> str:
    assert S3URI(path).basename == os.path.basename(path)


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_basename_wo_ext(path) -> str:
    assert S3URI(path).basename_wo_ext == os.path.splitext(os.path.basename(path))[0]


@pytest.mark.parametrize("path", common_paths())
def test_s3uri_ext(path) -> str:
    assert S3URI(path).ext == os.path.splitext(path)[1]


def test_s3uri_exists(s3_v6_txt):
    assert S3URI(s3_v6_txt).exists
    assert not S3URI(s3_v6_txt + ".should-not-be-here").exists
    assert not S3URI("s3://hey/this/should/not/be/here.txt").exists


def test_s3uri_mtime(s3_v6_txt):
    u = S3URI(s3_v6_txt + ".tmp")
    u.write("temp file for testing")
    now = time.time()
    assert now - 10 < u.mtime < now + 10
    u.rm()
    assert not u.exists


def test_s3uri_size(s3_v6_txt, v6_txt_size):
    assert S3URI(s3_v6_txt).size == v6_txt_size


def test_s3uri_md5(s3_v6_txt, v6_txt_md5_hash):
    assert S3URI(s3_v6_txt).md5 == v6_txt_md5_hash


def test_s3uri_md5_from_file(s3_v6_txt, v6_txt_md5_hash):
    u_md5 = S3URI(s3_v6_txt + URIBase.MD5_FILE_EXT)
    if u_md5.exists:
        u_md5.rm()
    assert not u_md5.exists
    u = S3URI(s3_v6_txt)
    assert u.md5_from_file is None

    u.get_metadata(make_md5_file=True)
    # S3URI should not make md5 file even with make_md5_file=True
    assert not u_md5.exists
    assert u.md5_from_file is None

    # nevertheless AutoURI should be able to read from s3://*.md5
    # make a temporary .md5 file
    u_md5.write(v6_txt_md5_hash)
    assert u.md5_from_file == v6_txt_md5_hash
    u_md5.rm()


def test_s3uri_md5_file_uri(s3_v6_txt):
    assert (
        S3URI(s3_v6_txt + URIBase.MD5_FILE_EXT).uri == s3_v6_txt + URIBase.MD5_FILE_EXT
    )


def test_s3uri_cp_url(s3_v6_txt, url_test_path) -> "AutoURI":
    """Test copying local_v6_txt to the following destination storages:
    url_test_path: s3 -> url
        This will fail as intended since URL is read-only.
    """
    u = S3URI(s3_v6_txt)
    basename = os.path.basename(s3_v6_txt)

    for test_path in (url_test_path,):
        u_dest = AutoURI(os.path.join(test_path, "test_s3uri_cp", basename))

        with pytest.raises(ReadOnlyStorageError):
            _, ret = u.cp(u_dest, return_flag=True)


def test_s3uri_cp(s3_v6_txt, local_test_path, s3_test_path, gcs_test_path) -> "AutoURI":
    """Test copying local_v6_txt to the following destination storages:
        local_test_path: s3 -> local
        s3_test_path: s3 -> s3
        gcs_test_path: s3 -> gcs

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
    u = S3URI(s3_v6_txt)
    basename = os.path.basename(s3_v6_txt)

    for test_path in (local_test_path, s3_test_path, gcs_test_path):
        u_dest = AutoURI(os.path.join(test_path, "test_s3uri_cp", basename))
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


def test_s3uri_write(s3_test_path):
    u = S3URI(s3_test_path + "/test_s3uri_write.tmp")

    assert not u.exists
    u.write("test")
    assert u.exists and u.read() == "test"
    u.rm()

    # this will be tested more with multiple threads in test_race_cond.py
    assert not u.exists
    u.write("test2", no_lock=True)
    assert u.exists and u.read() == "test2"
    u.rm()
    assert not u.exists


def test_s3uri_rm(s3_test_path):
    u = S3URI(s3_test_path + "/test_s3uri_rm.tmp")

    assert not u.exists
    u.write("")
    assert u.exists
    u.rm()
    assert not u.exists

    # this will be tested more with multiple threads in test_race_cond.py
    assert not u.exists
    u.write("", no_lock=True)
    assert u.exists
    u.rm()
    assert not u.exists


def test_s3uri_get_metadata(s3_v6_txt, v6_txt_size, v6_txt_md5_hash):
    u = S3URI(s3_v6_txt)

    m1 = u.get_metadata()
    assert m1.md5 == v6_txt_md5_hash
    assert m1.size == v6_txt_size

    m2 = u.get_metadata(skip_md5=True)
    assert m2.md5 is None
    assert m2.size == v6_txt_size

    u_md5 = S3URI(s3_v6_txt + ".md5")
    if u_md5.exists:
        u_md5.rm()
    m3 = u.get_metadata(make_md5_file=True)
    assert m3.md5 == v6_txt_md5_hash
    assert m3.size == v6_txt_size
    # S3URI should not make md5 file even with make_md5_file=True
    assert not u_md5.exists


def test_s3uri_read(s3_v6_txt):
    u = S3URI(s3_v6_txt)
    assert u.read() == v6_txt_contents()
    assert u.read(byte=True) == v6_txt_contents().encode()


def test_s3uri_find_all_files(s3_test_path):
    """Make a directory structure with empty files.

    Check if find_all_files() returns correct file (not sub-directory) paths.
    """
    prefix = os.path.join(s3_test_path, "test_s3uri_find_all_files")
    all_files = make_files_in_dir(prefix, make_local_empty_dir_d_a=False)

    all_files_found = S3URI(prefix).find_all_files()
    assert sorted(all_files_found) == sorted(all_files)
    for file in all_files:
        assert S3URI(file).exists


def test_s3uri_rmdir(s3_test_path):
    """Make a directory structure with empty files.

    Check if rmdir() deletes all empty files on given $prefix.
    """
    prefix = os.path.join(s3_test_path, "test_s3uri_rmdir")
    all_files = make_files_in_dir(prefix, make_local_empty_dir_d_a=False)

    # test rmdir(dry_run=True)
    S3URI(prefix).rmdir(dry_run=True)
    for file in all_files:
        assert S3URI(file).exists

    # test rmdir(dry_run=False)
    S3URI(prefix).rmdir(dry_run=False)
    for file in all_files:
        assert not S3URI(file).exists


# original methods in S3URI
def test_s3uri_get_bucket_path():
    assert S3URI("s3://a/b/c/d/e.txt").get_bucket_path() == ("a", "b/c/d/e.txt")
    assert S3URI("s3://asdflskfjljkfc-asdf/ddfjlfd/d.log").get_bucket_path() == (
        "asdflskfjljkfc-asdf",
        "ddfjlfd/d.log",
    )
    assert S3URI("s3://ok-test-bucket/hello.txt").get_bucket_path() == (
        "ok-test-bucket",
        "hello.txt",
    )


def test_s3uri_get_presigned_url(s3_v6_txt):
    u = S3URI(s3_v6_txt)
    # 2 seconds duration
    url = u.get_presigned_url(duration=2)

    u_url = HTTPURL(url)
    assert u_url.is_valid and u_url.read() == v6_txt_contents()
    time.sleep(5)
    # should expire in 2 seconds
    with pytest.raises(HTTPError):
        # forbidden since it's already expired
        u_url.read()


def test_s3uri_get_public_url(s3_public_url_test_v6_file):
    url = S3URI(s3_public_url_test_v6_file).get_public_url()
    u_url = HTTPURL(url)
    assert u_url.is_valid
    assert u_url.read() == v6_txt_contents()


# classmethods
def test_s3uri_get_path_sep() -> str:
    assert S3URI.get_path_sep() == os.path.sep


def test_s3uri_get_schemes() -> Tuple[str, ...]:
    assert S3URI.get_schemes() == ("s3://",)


def test_s3uri_get_loc_suffix() -> str:
    assert S3URI.get_loc_suffix() == ".s3"


def test_s3uri_get_loc_prefix() -> str:
    test_loc_prefix = "test_s3uri_get_loc_prefix"
    S3URI.init_s3uri(loc_prefix=test_loc_prefix)
    assert S3URI.get_loc_prefix() == test_loc_prefix
    S3URI.init_s3uri(loc_prefix="")
    assert S3URI.get_loc_prefix() == ""


def test_s3uri_localize(
    s3_test_path,
    local_j1_json,
    local_v41_json,
    local_v421_tsv,
    local_v5_csv,
    local_v6_txt,
    s3_j1_json,
    s3_v41_json,
    s3_v421_tsv,
    s3_v5_csv,
    s3_v6_txt,
    gcs_j1_json,
    gcs_v41_json,
    gcs_v421_tsv,
    gcs_v5_csv,
    gcs_v6_txt,
    url_j1_json,
    url_v41_json,
    url_v421_tsv,
    url_v5_csv,
    url_v6_txt,
) -> Tuple[str, bool]:
    """Recursive localization is supported for the following file extensions:
        .json:
            Files defined only in values (not keys) can be recursively localized.
        .tsv/.csv:
            Files defined in all values can be recursively localized.

    This function will test localizing j1.json file on each remote storage.
    This JSON file has file paths including .tsv and .csv, which also include
    other files in its contents.
    Therefore, when the recursive flag is on, all files in these JSON, TSV, CSV
    files should be localized recursively with correct file names
    (controlled by cls.loc_prefix and cls.loc_suffix).

    Filenaming for (recursive) localization:
        cls.loc_prefix + remote_file_path_without_scheme + cls.loc_suffix (for recursvely only)

    For example,
    s3://test-bucket/j1.json has some file paths on s3://.

    With recursive localization, all these files must be localized on /tmp/user/loc_prefix/ with
    a correct directory structure (keeping original structure on source: i.e. bucket name, path)
    and the name of the JSON file should be j1.local.json since contents of this file should be
    modified to point to localized files in it. This is recursively done for all files in it too.

    Without recursive localization, autouri doesn't look inside that JSON file and just localize
    the file itself alone on /tmp/user/loc_prefix/ while keeping the same filename j1.local.json.

    Test localizing on a GCS storage from the following remote storages:
        local_test_path: local -> s3
        s3_test_path: s3 -> s3
        gcs_test_path: gcs -> s3
        url_test_path: url -> s3

    Parameters to be tested:
        make_md5_file:
            Make md5 file on destination only when it's REQUIRED.
            It's required only if we need to compare md5 hash of source and target.
            This is already tested cp and it's actually needed for local storage.
            Cloud URIs will provide md5 hash info in their metadata so md5 file
            is not required and hence will not be created even with this flag on.
        recursive:
            j1.json
    """
    loc_prefix = os.path.join(s3_test_path, "test_s3uri_localize")

    for j1_json in (s3_j1_json,):
        # localization from same storage
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()
        basename = u_j1_json.basename

        # for localization both with or without recursive
        # nothing should be localized actually
        # since they are already on same storage
        # so loc_prefix directory itself shouldn't be created
        loc_uri, localized = S3URI.localize(
            u_j1_json, recursive=False, return_flag=True, loc_prefix=loc_prefix_
        )
        assert loc_uri == u_j1_json.uri and not localized
        assert not AutoURI(os.path.join(loc_prefix_, basename)).exists

        loc_uri, localized = S3URI.localize(
            u_j1_json, recursive=True, return_flag=True, loc_prefix=loc_prefix_
        )
        assert loc_uri == u_j1_json.uri and not localized
        assert not AutoURI(os.path.join(loc_prefix_, basename)).exists
        # check if all URIs defeind in localized JSON file exist
        recurse_raise_if_uri_not_exist(loc_uri)

    # localization from remote storages
    for j1_json in (local_j1_json, gcs_j1_json, url_j1_json):
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()
        basename = u_j1_json.basename

        loc_uri, localized = S3URI.localize(
            u_j1_json, recursive=False, return_flag=True, loc_prefix=loc_prefix_
        )
        expected = os.path.join(loc_prefix_, u_j1_json.loc_dirname, u_j1_json.basename)
        assert loc_uri == expected
        assert localized and AutoURI(expected).exists

        loc_uri, localized = S3URI.localize(
            u_j1_json, recursive=True, return_flag=True, loc_prefix=loc_prefix_
        )
        expected = os.path.join(
            loc_prefix_,
            u_j1_json.loc_dirname,
            u_j1_json.basename_wo_ext + S3URI.get_loc_suffix() + u_j1_json.ext,
        )
        assert loc_uri == expected
        assert localized and AutoURI(expected).exists
        # check if all URIs defeind in localized JSON file exist
        recurse_raise_if_uri_not_exist(loc_uri)
