import os
import time
from typing import Any, Tuple

import pytest

from autouri.abspath import AbsPath, convert_relpath_to_abspath_if_valid
from autouri.autouri import AutoURI, URIBase
from autouri.httpurl import ReadOnlyStorageError

from .files import (
    common_paths,
    make_files_in_dir,
    recurse_raise_if_uri_not_exist,
    v6_txt_contents,
)


@pytest.mark.parametrize(
    "path,converted",
    [
        ("/asdfasf/sdaf/", "/asdfasf/sdaf/"),
        ("LICENSE", "LICENSE"),
        ("setup.py", "setup.py"),
        ("setup.py.", "setup.py."),
        ("license", "license"),
        ("does-not-exist.json", "does-not-exist.json"),
        ("~/sadfsf", "~/sadfsf"),
        ("/etc/passwd", "/etc/passwd"),
        ("conftest.py", os.getcwd() + "/conftest.py"),
        ("files.py", os.getcwd() + "/files.py"),
    ],
)
def test_convert_relpath_to_abspath_if_valid(path, converted) -> Any:
    assert convert_relpath_to_abspath_if_valid(path, allowed_exts=(".py")) == converted


@pytest.mark.parametrize("path", common_paths())
def test_abspath_uri(path) -> Any:
    assert AbsPath(path).uri == os.path.expanduser(path)


@pytest.mark.parametrize("path", common_paths())
def test_abspath_uri_wo_ext(path) -> str:
    assert AbsPath(path).uri_wo_ext == os.path.splitext(os.path.expanduser(path))[0]


@pytest.mark.parametrize("path", common_paths())
def test_abspath_uri_wo_scheme(path) -> str:
    assert AbsPath(path).uri_wo_scheme == os.path.expanduser(path)


@pytest.mark.parametrize("path", common_paths())
def test_abspath_is_valid(path) -> bool:
    """Also tests AutoURI auto-conversion since it's based on is_valid property"""
    expected = os.path.isabs(os.path.expanduser(path))
    assert AbsPath(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == AbsPath


@pytest.mark.parametrize("path", common_paths())
def test_abspath_dirname(path) -> str:
    assert AbsPath(path).dirname == os.path.dirname(os.path.expanduser(path))


@pytest.mark.parametrize("path", common_paths())
def test_abspath_dirname_wo_scheme(path) -> str:
    assert AbsPath(path).dirname_wo_scheme == os.path.dirname(os.path.expanduser(path))


@pytest.mark.parametrize("path", common_paths())
def test_abspath_loc_dirname(path) -> str:
    assert AbsPath(path).loc_dirname == os.path.dirname(os.path.expanduser(path)).strip(
        os.path.sep
    )


@pytest.mark.parametrize("path", common_paths())
def test_abspath_basename(path) -> str:
    assert AbsPath(path).basename == os.path.basename(os.path.expanduser(path))


@pytest.mark.parametrize("path", common_paths())
def test_abspath_basename_wo_ext(path) -> str:
    assert (
        AbsPath(path).basename_wo_ext
        == os.path.splitext(os.path.basename(os.path.expanduser(path)))[0]
    )


@pytest.mark.parametrize("path", common_paths())
def test_abspath_ext(path) -> str:
    assert AbsPath(path).ext == os.path.splitext(os.path.expanduser(path))[1]


def test_abspath_exists(local_v6_txt):
    assert AbsPath(local_v6_txt).exists
    assert not AbsPath(local_v6_txt + ".should-not-be-here").exists
    assert not AbsPath("/hey/this/should/not/be/here.txt").exists


def test_abspath_mtime(local_v6_txt):
    u = AbsPath(local_v6_txt + ".tmp")
    u.write("temp file for testing")
    now = time.time()
    assert now - 10 < u.mtime < now + 10
    u.rm()
    assert not u.exists


def test_abspath_size(local_v6_txt, v6_txt_size):
    assert AbsPath(local_v6_txt).size == v6_txt_size


def test_abspath_md5(local_v6_txt, v6_txt_md5_hash):
    assert AbsPath(local_v6_txt).md5 == v6_txt_md5_hash


def test_abspath_md5_from_file(local_v6_txt, v6_txt_md5_hash):
    u_md5 = AbsPath(local_v6_txt + URIBase.MD5_FILE_EXT)
    if u_md5.exists:
        u_md5.rm()
    assert not u_md5.exists
    u = AbsPath(local_v6_txt)
    assert u.md5_from_file is None

    u.get_metadata(make_md5_file=True)
    assert u_md5.exists
    assert u.md5_from_file == v6_txt_md5_hash
    u_md5.rm()
    assert not u_md5.exists


def test_abspath_md5_file_uri(local_v6_txt):
    assert (
        AbsPath(local_v6_txt + URIBase.MD5_FILE_EXT).uri
        == local_v6_txt + URIBase.MD5_FILE_EXT
    )


def test_abspath_cp_url(local_v6_txt, url_test_path) -> "AutoURI":
    """Test copying local_v6_txt to the following destination storages:
    url_test_path: local -> url
        This will fail as intended since URL is read-only.
    """
    u = AbsPath(local_v6_txt)
    basename = os.path.basename(local_v6_txt)

    for test_path in (url_test_path,):
        u_dest = AutoURI(os.path.join(test_path, "test_abspath_cp", basename))
        with pytest.raises(ReadOnlyStorageError):
            _, ret = u.cp(u_dest, return_flag=True)


def test_abspath_cp(
    local_v6_txt, local_test_path, s3_test_path, gcs_test_path, url_test_path
) -> "AutoURI":
    """Test copying local_v6_txt to the following destination storages:
        local_test_path: local -> local
        s3_test_path: local -> s3
        gcs_test_path: local -> gcs

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
    u = AbsPath(local_v6_txt)
    basename = os.path.basename(local_v6_txt)

    for test_path in (local_test_path, s3_test_path, gcs_test_path):
        u_dest = AutoURI(os.path.join(test_path, "test_abspath_cp", basename))
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


def test_abspath_write(local_test_path):
    u = AbsPath(local_test_path + "/test_abspath_write.tmp")

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


def test_abspath_write_no_permission():
    u = AbsPath("/test-permission-denied/x.tmp")
    with pytest.raises(PermissionError):
        u.write("test")


def test_abspath_rm(local_test_path):
    u = AbsPath(local_test_path + "/test_abspath_rm.tmp")

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


def test_abspath_get_metadata(local_v6_txt, v6_txt_size, v6_txt_md5_hash):
    u = AbsPath(local_v6_txt)

    m1 = u.get_metadata()
    assert m1.md5 == v6_txt_md5_hash
    assert m1.size == v6_txt_size

    m2 = u.get_metadata(skip_md5=True)
    assert m2.md5 is None
    assert m2.size == v6_txt_size

    u_md5 = AbsPath(local_v6_txt + ".md5")
    if u_md5.exists:
        u_md5.rm()
    m3 = u.get_metadata(make_md5_file=True)
    assert m3.md5 == v6_txt_md5_hash
    assert m3.size == v6_txt_size
    assert u_md5.exists
    assert u_md5.read() == v6_txt_md5_hash


def test_abspath_read(local_v6_txt):
    u = AbsPath(local_v6_txt)
    assert u.read() == v6_txt_contents()
    assert u.read(byte=True) == v6_txt_contents().encode()


def test_abspath_find_all_files(local_test_path):
    """Make a directory structure with empty files and an empty directory.

    Check if find_all_files() returns correct file (not sub-directory) paths.
    Also check if find_all_files() does not return an empty sub-directory.
    """
    prefix = os.path.join(local_test_path, "test_abspath_find_all_files")
    all_files = make_files_in_dir(prefix, make_local_empty_dir_d_a=True)
    empty_sub_dir = os.path.join(prefix, "d", "a")
    assert os.path.exists(empty_sub_dir) and os.path.isdir(empty_sub_dir)

    all_files_found = AbsPath(prefix).find_all_files()
    assert sorted(all_files_found) == sorted(all_files)
    for file in all_files:
        assert AbsPath(file).exists
        assert file.rstrip("/") != empty_sub_dir.rstrip("/")


def test_abspath_rmdir(local_test_path):
    """Make a directory structure with empty files and an empty directory.

    Check if rmdir() deletes the root directory itself including
    all empty files and empty directory on given $prefix.
    """
    prefix = os.path.join(local_test_path, "test_abspath_rmdir")
    all_files = make_files_in_dir(prefix, make_local_empty_dir_d_a=True)
    empty_sub_dir = os.path.join(prefix, "d", "a")
    assert os.path.exists(empty_sub_dir) and os.path.isdir(empty_sub_dir)

    # test rmdir(dry_run=True)
    AbsPath(prefix).rmdir(dry_run=True)
    for file in all_files:
        assert AbsPath(file).exists

    # test rmdir(dry_run=False)
    AbsPath(prefix).rmdir(dry_run=False)
    for file in all_files:
        assert not AbsPath(file).exists
    assert not os.path.exists(empty_sub_dir)
    assert not os.path.exists(prefix)


# original methods in AbsPath
def test_abspath_get_mapped_url(local_v6_txt):
    u = AbsPath(local_v6_txt)
    dirname = os.path.dirname(local_v6_txt)
    basename = os.path.basename(local_v6_txt)
    url_prefix = "http://my.test.com"

    AbsPath.init_abspath(map_path_to_url={dirname: url_prefix})
    assert u.get_mapped_url() == os.path.join(url_prefix, basename)

    AbsPath.init_abspath(map_path_to_url=dict())
    assert u.get_mapped_url() is None


def test_abspath_mkdirname(local_test_path):
    f = os.path.join(local_test_path, "test_abspath_mkdirname", "tmp.txt")
    AbsPath(f).mkdir_dirname()
    assert os.path.exists(os.path.dirname(f))


def test_abspath_soft_link(local_test_path, local_v6_txt):
    u_src = AbsPath(local_v6_txt)
    f = os.path.join(local_test_path, "test_abspath_soft_link", "v6.txt")
    u_target = AbsPath(f)
    u_target.mkdir_dirname()
    u_src.soft_link(u_target)
    assert u_target.exists and u_target.read() == v6_txt_contents()
    assert u_src.uri == os.path.realpath(u_target.uri)

    with pytest.raises(OSError):
        # file already exists
        u_src.soft_link(u_target)
    # no error if force
    u_src.soft_link(u_target, force=True)
    u_target.rm()


# staticmethods
def test_abspath_get_abspath_if_exists():
    # write a local file on CWD.
    test_local_file_abspath = os.path.join(os.getcwd(), "test.txt")
    u = AbsPath(test_local_file_abspath)
    if u.exists:
        u.rm()

    # if it doesn't exist
    assert AbsPath.get_abspath_if_exists("test.txt") == "test.txt"
    assert AbsPath.get_abspath_if_exists(AutoURI("test.txt")) == "test.txt"

    u.write("hello-world")

    # if it exists
    assert AbsPath.get_abspath_if_exists("test.txt") == test_local_file_abspath
    assert AbsPath.get_abspath_if_exists(AutoURI("test.txt")) == test_local_file_abspath

    assert AbsPath.get_abspath_if_exists("tttttttttest.txt") == "tttttttttest.txt"
    assert (
        AbsPath.get_abspath_if_exists(AutoURI("tttttttttest.txt")) == "tttttttttest.txt"
    )
    assert (
        AbsPath.get_abspath_if_exists("~/if-it-does-not-exist")
        == "~/if-it-does-not-exist"
    )
    assert AbsPath.get_abspath_if_exists("non-existing-file") == "non-existing-file"

    u.rm()


# classmethods
def test_abspath_get_path_sep() -> str:
    assert AbsPath.get_path_sep() == os.path.sep


def test_abspath_get_schemes() -> Tuple[str, ...]:
    assert AbsPath.get_schemes() == tuple()


def test_abspath_get_loc_suffix() -> str:
    assert AbsPath.get_loc_suffix() == ".local"


def test_abspath_get_loc_prefix() -> str:
    test_loc_prefix = "test_abspath_get_loc_prefix"
    AbsPath.init_abspath(loc_prefix=test_loc_prefix)
    assert AbsPath.get_loc_prefix() == test_loc_prefix
    AbsPath.init_abspath(loc_prefix="")
    assert AbsPath.get_loc_prefix() == ""


def test_abspath_localize(
    local_test_path,
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

    Test localizing on a local storage from the following remote storages:
        local_test_path: local -> local
        s3_test_path: s3 -> local
        gcs_test_path: gcs -> local
        url_test_path: url -> local

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
    loc_prefix = os.path.join(local_test_path, "test_abspath_localize")

    for j1_json in (local_j1_json,):
        # localization from local storage
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()

        # for localization both with or without recursive
        # nothing should be localized actually
        # since they are already on a local storage
        # so loc_prefix directory itself shouldn't be created
        loc_uri, localized = AbsPath.localize(
            u_j1_json, recursive=False, return_flag=True, loc_prefix=loc_prefix_
        )
        assert loc_uri == u_j1_json.uri and not localized
        assert not os.path.exists(loc_prefix)

        loc_uri, localized = AbsPath.localize(
            u_j1_json, recursive=True, return_flag=True, loc_prefix=loc_prefix_
        )
        assert loc_uri == u_j1_json.uri and not localized
        assert not os.path.exists(loc_prefix)
        # check if all URIs defeind in localized JSON file exist
        recurse_raise_if_uri_not_exist(loc_uri)

    # localization from remote storages
    for j1_json in (gcs_j1_json, s3_j1_json, url_j1_json):
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()

        loc_uri, localized = AbsPath.localize(
            u_j1_json, recursive=False, return_flag=True, loc_prefix=loc_prefix_
        )
        assert loc_uri == os.path.join(
            loc_prefix_, u_j1_json.loc_dirname, u_j1_json.basename
        )
        assert localized and os.path.exists(loc_uri)

        loc_uri, localized = AbsPath.localize(
            u_j1_json, recursive=True, return_flag=True, loc_prefix=loc_prefix_
        )
        assert loc_uri == os.path.join(
            loc_prefix_,
            u_j1_json.loc_dirname,
            u_j1_json.basename_wo_ext + AbsPath.get_loc_suffix() + u_j1_json.ext,
        )
        assert localized and os.path.exists(loc_uri)
        # check if all URIs defeind in localized JSON file exist
        recurse_raise_if_uri_not_exist(loc_uri)
