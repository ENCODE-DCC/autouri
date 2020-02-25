#!/usr/bin/env python3
import pytest
from autouri.httpurl import HTTPURL
from autouri.autouri import AutoURI


@pytest.mark.parametrize('path,expected',[
        ('/testing/abspath', False),
        ('/testing/abspath/', False),
        ('~/os/expandable', False),
        ('~~/os/expandable', False),
        ('~/~/os/expandable', False),
        ('test/ok/man.csv', False),
        ('http://hello.world.com/ok.txt', True),
        ('https://hello.world.com/ok.txt', True),
        ('http://hello.world.com/ok.txt?parameter1=true&parameter2=false', True),
        ('https://hello.world.com/ok.txt?parameter1=true&parameter2=false', True),
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
def test_httpurl_is_valid(path, expected):
    """Also tests AutoURI auto-conversion since it's based on is_valid property
    """
    assert HTTPURL(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == HTTPURL


def test_httpurl_read(url_sm_file, sm_file_contents):
    assert HTTPURL(url_sm_file).read() == sm_file_contents
    assert HTTPURL(url_sm_file).read(byte=True) == sm_file_contents.encode()
