#!/usr/bin/env python3
import pytest
from autouri.abspath import AbsPath
from autouri.autouri import AutoURI


@pytest.mark.parametrize('path,expected',[
        ('/testing/abspath', True),
        ('/testing/abspath/', True),
        ('~/os/expandable', True),
        ('~~/os/expandable', False),
        ('~/~/os/expandable', True),
        ('test/ok/man.csv', False),
        ('http://hello.world.com/ok.txt', False),
        ('https://hello.world.com/ok.txt', False),
        ('http://hello.world.com/ok.txt?parameter1=true&parameter2=false', False),
        ('https://hello.world.com/ok.txt?parameter1=true&parameter2=false', False),
        ('http:/hello.world.com/notok.txt', False),
        ('ftp:/hello.world.com/notok.txt', False),
        ('dx://dnanexus-prj/not/supported.txt', False),
        ('file:/notok.txt', False),
        ('file://hostname/notok.txt', False),
        ('s3://hello-world/ok.txt', False),
        ('s3:/hello-world/not-ok.txt', False),
        ('s3:\\hello-world\\not-ok.txt', False),
        ('gs://hello-world/ok.txt', False),
        ('gs:/hello-world/not-ok.txt', False),
        ('gs:\\hello-world\\not-ok.txt', False),
        ('!@#:;$@!#$F', False),
    ])
def test_abspath_is_valid(path, expected):
    """Also tests AutoURI auto-conversion since it's based on is_valid property
    """
    assert AbsPath(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == AbsPath


def test_abspath_read(local_sm_file, sm_file_contents):
    assert AbsPath(local_sm_file).read() == sm_file_contents
    assert AbsPath(local_sm_file).read(byte=True) == sm_file_contents.encode()


