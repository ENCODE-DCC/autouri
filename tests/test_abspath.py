#!/usr/bin/env python3
import os
import pytest
import time
from typing import Any, Tuple, Union
from autouri.abspath import AbsPath, init_abspath
from autouri.autouri import AutoURI, URIBase
from autouri.filespinlock import FileSpinLock
from autouri.httpurl import ReadOnlyStorageError
from .files import v6_txt_contents, common_paths



@pytest.mark.parametrize('path', common_paths())
def test_abspath_uri(path) -> Any:
    assert AbsPath(path).uri == \
            os.path.expanduser(path)


@pytest.mark.parametrize('path', common_paths())
def test_abspath_uri_wo_ext(path) -> str:
    assert AbsPath(path).uri_wo_ext == \
            os.path.splitext(os.path.expanduser(path))[0]


@pytest.mark.parametrize('path', common_paths())
def test_abspath_uri_wo_scheme(path) -> str:
    assert AbsPath(path).uri_wo_scheme == \
            os.path.expanduser(path)


@pytest.mark.parametrize('path', common_paths())
def test_abspath_is_valid(path) -> bool:
    """Also tests AutoURI auto-conversion since it's based on is_valid property
    """
    expected = os.path.isabs(os.path.expanduser(path))
    assert AbsPath(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == AbsPath


@pytest.mark.parametrize('path', common_paths())
def test_abspath_dirname(path) -> str:
    assert AbsPath(path).dirname == \
            os.path.dirname(os.path.expanduser(path))


@pytest.mark.parametrize('path', common_paths())
def test_abspath_dirname_wo_scheme(path) -> str:
    assert AbsPath(path).dirname_wo_scheme == \
            os.path.dirname(os.path.expanduser(path))


@pytest.mark.parametrize('path', common_paths())
def test_abspath_loc_dirname(path) -> str:
    assert AbsPath(path).loc_dirname == \
            os.path.dirname(os.path.expanduser(path)).strip(os.path.sep)


@pytest.mark.parametrize('path', common_paths())
def test_abspath_basename(path) -> str:
    assert AbsPath(path).basename == \
            os.path.basename(os.path.expanduser(path))


@pytest.mark.parametrize('path', common_paths())
def test_abspath_basename_wo_ext(path) -> str:
    assert AbsPath(path).basename_wo_ext == \
            os.path.splitext(os.path.basename(os.path.expanduser(path)))[0]


@pytest.mark.parametrize('path', common_paths())
def test_abspath_ext(path) -> str:
    assert AbsPath(path).ext == \
            os.path.splitext(os.path.expanduser(path))[1]


def test_abspath_exists(local_v6_txt):
    assert AbsPath(local_v6_txt).exists
    assert not AbsPath(local_v6_txt + '.should-not-be-here').exists
    assert not AbsPath('/hey/this/should/not/be/here.txt').exists


def test_abspath_mtime(local_v6_txt):
    u = AbsPath(local_v6_txt + '.tmp')
    u.write('temp file for testing')
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
    assert u.md5_from_file == None

    m = u.get_metadata(make_md5_file=True)
    assert u_md5.exists
    assert u.md5_from_file == v6_txt_md5_hash
    u_md5.rm()
    assert not u_md5.exists
    

def test_abspath_md5_file_uri(local_v6_txt):
    assert AbsPath(local_v6_txt + URIBase.MD5_FILE_EXT).uri == local_v6_txt + URIBase.MD5_FILE_EXT


def test_abspath_lock(local_v6_txt) -> 'FileSpinLock':
    assert isinstance(AbsPath(local_v6_txt).lock, FileSpinLock)


@pytest.mark.xfail(raises=ReadOnlyStorageError)
def test_abspath_cp(
    local_v6_txt,
    s3_test_path,
    gcs_test_path,
    url_test_path) -> 'AutoURI':
    """Test on url_test_path will fail as intended since it's a read-only storage
    """
    u = AbsPath(local_v6_txt)
    basename = os.path.basename(local_v6_txt)

    for test_path in (s3_test_path, gcs_test_path, url_test_path):
        u_dest = AutoURI(os.path.join(test_path, 'test_abspath_cp', basename))
        if u_dest.exists:
            u_dest.rm()

        assert not u_dest.exists
        _, ret = u.cp(u_dest)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 0
        u_dest.rm()

        assert not u_dest.exists
        # cp without lock will be tested throughly in test_race_cond.py
        _, ret = u.cp(u_dest, no_lock=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 0
        u_dest.rm()

        # trivial: copy without checksum when target doesn't exists
        assert not u_dest.exists
        _, ret = u.cp(u_dest, no_checksum=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 0

        # copy without checksum when target exists
        m_dest = u_dest.get_metadata()
        assert m_dest.exists
        _, ret = u.cp(u_dest, no_checksum=True)
        # compare new mtime vs old mtime
        # new time should be larger if it's overwritten as intended
        assert u_dest.mtime > m_dest.mtime and u.read() == u_dest.read() and ret == 0

        # copy with checksum when target exists
        m_dest = u_dest.get_metadata()
        assert m_dest.exists
        _, ret = u.cp(u_dest)
        # compare new mtime vs old mtime
        # new time should be the same as old time
        assert u_dest.mtime == m_dest.mtime and u.read() == u_dest.read() and ret == 1

        # make_md5_file works only when it's required
        # i.e. need to compare md5 has of src vs target
        # so target must exist prior to test it
        assert u_dest.exists
        # delete md5 file if exists
        u_dest_md5_file = AutoURI(u_dest.uri + URIBase.MD5_FILE_EXT)
        if u_dest_md5_file.exists:
            u_dest_md5_file.rm()
        _, ret = u.cp(u_dest, make_md5_file=True)
        assert u_dest.exists and u.read() == u_dest.read() and ret == 1
        u_dest.rm()


def test_abspath_write(local_test_path):
    u = AbsPath(local_test_path + '/test_abspath_write.tmp')

    assert not u.exists
    u.write('test')
    assert u.exists and u.read() == 'test'
    u.rm()

    # this will be tested more with multiple threads in test_race_cond.py
    assert not u.exists
    u.write('test2', no_lock=True)
    assert u.exists and u.read() == 'test2'
    u.rm()
    assert not u.exists


def test_abspath_rm(local_test_path):
    u = AbsPath(local_test_path + '/test_abspath_rm.tmp')

    assert not u.exists
    u.write('')
    assert u.exists
    u.rm()
    assert not u.exists

    # this will be tested more with multiple threads in test_race_cond.py
    assert not u.exists
    u.write('', no_lock=True)
    assert u.exists
    u.rm()
    assert not u.exists


def test_abspath_get_metadata(local_v6_txt, v6_txt_size, v6_txt_md5_hash):
    u = AbsPath(local_v6_txt)

    m1 = u.get_metadata()
    assert m1.md5 == v6_txt_md5_hash
    assert m1.size == v6_txt_size

    m2 = u.get_metadata(skip_md5=True)
    assert m2.md5 == None
    assert m2.size == v6_txt_size

    u_md5 = AbsPath(local_v6_txt + '.md5')
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


# original methods in AbsPath
def test_abspath_get_mapped_url(local_v6_txt):
    u = AbsPath(local_v6_txt)
    dirname = os.path.dirname(local_v6_txt)
    basename = os.path.basename(local_v6_txt)
    url_prefix = 'http://my.test.com'

    init_abspath(map_path_to_url={dirname: url_prefix})
    assert u.get_mapped_url() == os.path.join(url_prefix, basename)

    init_abspath(map_path_to_url=dict())
    assert u.get_mapped_url() == None


def test_abspath_mkdirname(local_test_path):
    f = os.path.join(local_test_path, 'test_abspath_mkdirname', 'tmp.txt')
    AbsPath(f).mkdir_dirname()
    assert os.path.exists(os.path.dirname(f))


# classmethods
def test_abspath_get_path_sep() -> str:
    assert AbsPath.get_path_sep() == os.path.sep


def test_abspath_get_schemes() -> Tuple[str, ...]:
    assert AbsPath.get_schemes() == tuple()


def test_abspath_get_loc_suffix() -> str:
    assert AbsPath.get_loc_suffix() == '.local'


def test_abspath_get_loc_prefix() -> str:
    test_loc_prefix = 'test_abspath_get_loc_prefix'
    init_abspath(loc_prefix=test_loc_prefix)
    assert AbsPath.get_loc_prefix() == test_loc_prefix
    init_abspath(loc_prefix='')
    assert AbsPath.get_loc_prefix() == ''


# def test_abspath_localize(src_uri, make_md5_file=False, recursive=False) -> Tuple[str, bool]:
#     pass
#     # assert AbsPath.localize() == 

