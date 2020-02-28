#!/usr/bin/env python3
"""Files for testing

To test recursive localization.
Refer to j1_json_contents for structure.
    j1_json,
    v41_txt,
    v421_tsv,
    v5_csv,
    v6_txt,

To test member functions in class
    v6_txt,

"""
from autouri.autouri import AutoURI


def j1_json_contents(prefix, loc_suffix=''):
    return '''{
        "k1": "v1",
        "k2": 1,
        "k3": 2.2,
        "k4": {
            "k41": "{prefix}/v41.txt",
            "k42": {
                "k421": "{prefix}/deeper/v421{loc_suffix}.tsv"
            },
            "k43": null
        },
        "k5": "{prefix}/even/deeper/v5{loc_suffix}.csv"
        }
    '''.format(prefix=prefix)


def v41_txt_contents():
    return 'v41: Hello World'


def v421_tsv_contents(prefix, loc_suffix=''):
    return '\n'.join([
        'k1\tv1',
        'k2\t1',
        'k3\t2.2',
        'k4\tnot/absolute/path',
        'k5\t{prefix}/even/deeper/v5{loc_suffix}.csv',
        'k6\t{prefix}/v6.txt'.format(prefix=prefix)
    ])


def v5_csv_contents(prefix):
    return '\n'.join([
        'k1,v1',
        'k2,1',
        'k3,2.2',
        'k4,not/absolute/path',
        'k5,s33://not-valid-bucket-address',
        'k6,{prefix}/v6.txt'.format(prefix=prefix)
    ])


def v6_txt_contents():
    return 'v6: Hello World'


def j1_json(prefix, loc_suffix='', make=False):
    u = '{prefix}/j1.json'.format(
        prefix=prefix, loc_suffix=loc_suffix)
    if make:
        AutoURI(u).write(j1_json_contents(
            prefix))
    return u


def v41_txt(prefix, loc_suffix='', make=False):
    u = '{prefix}/v41.txt'.format(
        prefix=prefix, loc_suffix=loc_suffix)
    if make:
        AutoURI(u).write(v41_txt_contents())
    return u   


def v421_tsv(prefix, loc_suffix='', make=False):
    u = '{prefix}/deeper/v421.tsv'.format(
        prefix=prefix, loc_suffix=loc_suffix)
    if make:
        AutoURI(u).write(v421_tsv_contents(
            prefix))
    return u


def v5_csv(prefix, make=False):
    u = '{prefix}/even/deeper/v5.csv'.format(
        prefix=prefix)
    if make:
        AutoURI(u).write(v5_csv_contents(
            prefix))
    return u


def v6_txt(prefix, make=False):
    u = '{prefix}/v6.txt'.format(
        prefix=prefix)
    if make:
        AutoURI(u).write(v6_txt_contents())
    return u


def common_paths():
    return [
        '/testing/abspath',
        '/testing/abspath/',
        '~/os/expandable',
        '~~/os/expandable',
        '~/~/os/expandable',
        'test/ok/man.csv',
        'http://hello.world.com/ok.txt',
        'https://hello.world.com/ok.txt',
        'http://hello.world.com/ok.txt?parameter1=true&parameter2=false',
        'https://hello.world.com/ok.txt?parameter1=true&parameter2=false',
        'http:/hello.world.com/notok.txt',
        'ftp:/hello.world.com/notok.txt',
        'dx://dnanexus-prj/not/supported.txt',
        'file:/notok.txt',
        'file://hostname/notok.txt',
        's3://hello-world/ok.txt',
        's3:/hello-world/not-ok.txt',
        's3:\\hello-world\\not-ok.txt',
        'gs://hello-world/ok.txt',
        'gs:/hello-world/not-ok.txt',
        'gs:\\hello-world\\not-ok.txt',
        '!@#:;$@!#$F',
    ]
