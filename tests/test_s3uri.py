#!/usr/bin/env python3
import pytest
from autouri.s3uri import S3URI
from autouri.autouri import AutoURI


@pytest.mark.parametrize('path,expected',[
        ('/testing/abspath', False),
        ('/testing/abspath/', False),
        ('~/os/expandable', False),
        ('~~/os/expandable', False),
        ('~/~/os/expandable', False),
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
        ('s3://hello-world/ok.txt', True),
        ('s3:/hello-world/not-ok.txt', False),
        ('s3:\\hello-world\\not-ok.txt', False),
        ('gs://hello-world/ok.txt', False),
        ('gs:/hello-world/not-ok.txt', False),
        ('gs:\\hello-world\\not-ok.txt', False),
        ('!@#:;$@!#$F', False),
    ])
def test_s3uri_is_valid(path, expected):
    """Also tests AutoURI auto-conversion since it's based on is_valid property
    """
    assert S3URI(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == S3URI


def test_s3uri_read(s3_sm_file, sm_file_contents):
    assert S3URI(s3_sm_file).read() == sm_file_contents
    assert S3URI(s3_sm_file).read(byte=True) == sm_file_contents.encode()
