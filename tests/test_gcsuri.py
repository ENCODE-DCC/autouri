#!/usr/bin/env python3
import pytest
from autouri.gcsuri import GCSURI
from autouri.autouri import AutoURI
from .files import v6_txt_contents


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
        ('s3://hello-world/ok.txt', False),
        ('s3:/hello-world/not-ok.txt', False),
        ('s3:\\hello-world\\not-ok.txt', False),
        ('gs://hello-world/ok.txt', True),
        ('gs:/hello-world/not-ok.txt', False),
        ('gs:\\hello-world\\not-ok.txt', False),
        ('!@#:;$@!#$F', False),
    ])
def test_gcsuri_is_valid(path, expected):
    """Also tests AutoURI auto-conversion since it's based on is_valid property
    """
    assert GCSURI(path).is_valid == expected
    assert not expected or type(AutoURI(path)) == GCSURI


def test_gcsuri_read(gcs_v6_txt):
    assert GCSURI(gcs_v6_txt).read() == v6_txt_contents()
    assert GCSURI(gcs_v6_txt).read(byte=True) == v6_txt_contents().encode()
