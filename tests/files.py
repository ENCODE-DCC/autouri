"""Files for testing

Use v6_txt to test basic/simple methods of AutoURI.

Use j1_json to test recursive localization of a file containing file paths in it.
j1_json has TSV/CSV files, which can also be recursively localized.

See j1_json_contents() for its detailed structure.
    j1_json
        v41_json
        v421_tsv
            v5_csv
                v6_txt
            v6_txt
            v1_json (optionally to check self-reference)
        v5_csv
            v6_txt
"""
import os

from autouri.autouri import AutoURI
from autouri.loc_aux import recurse_csv, recurse_json, recurse_tsv


def j1_json_contents(
    prefix, prefix_v41=None, prefix_v421=None, prefix_v5=None, loc_suffix=""
):
    if prefix_v41 is None:
        prefix_v41 = prefix
    if prefix_v421 is None:
        prefix_v421 = prefix
    if prefix_v5 is None:
        prefix_v5 = prefix
    return """{{
        "k1": "v1",
        "k2": 1,
        "k3": 2.2,
        "k4": {{
            "k41": "{prefix_v41}/v41.json",
            "k42": {{
                "k421": "{prefix_v421}/deeper/v421{loc_suffix}.tsv"
            }},
            "k43": null
        }},
        "k5": "{prefix_v5}/even/deeper/v5{loc_suffix}.csv"
    }}
    """.format(
        prefix_v41=prefix_v41,
        prefix_v421=prefix_v421,
        prefix_v5=prefix_v5,
        loc_suffix=loc_suffix,
    )


def v41_json_contents():
    """JSON without any files defined in it.
    """
    return '{ "k1": "v1" }'


def v421_tsv_contents(
    prefix,
    prefix_v5=None,
    prefix_v1=None,
    prefix_v6=None,
    loc_suffix="",
    make_link_to_j1_json=False,
):
    """
    Args:
        make_link_to_j1_json:
            Link j1.json in this TSV file to to intentionally make a self reference
            , and hence an infinite loop while recursive localization
            j1.json -> v421.tsv -> j1.json -> ...
    """
    if prefix_v5 is None:
        prefix_v5 = prefix
    if prefix_v1 is None:
        prefix_v1 = prefix
    if prefix_v6 is None:
        prefix_v6 = prefix
    arr = [
        "k1\tv1",
        "k2\t1",
        "k3\t2.2",
        "k4\tnot/absolute/path",
        "k5\t{prefix_v5}/even/deeper/v5{loc_suffix}.csv",
        "k6\t{prefix_v6}/v6.txt",
    ]
    if make_link_to_j1_json:
        arr.append("k7\t{prefix_v1}/j1.json")
    s = "\n".join(arr)
    return s.format(
        prefix_v5=prefix_v5,
        prefix_v1=prefix_v1,
        prefix_v6=prefix_v6,
        loc_suffix=loc_suffix,
    )


def v5_csv_contents(prefix, prefix_v6=None):
    if prefix_v6 is None:
        prefix_v6 = prefix
    s = "\n".join(
        [
            "k1,v1",
            "k2,1",
            "k3,2.2",
            "k4,not/absolute/path",
            "k5,s33://not-valid-bucket-address",
            "k6,{prefix_v6}/v6.txt",
        ]
    )
    return s.format(prefix_v6=prefix_v6)


def v6_txt_contents():
    return "v6: Hello World"


def j1_json(
    prefix, prefix_v41=None, prefix_v421=None, prefix_v5=None, loc_suffix="", make=False
):
    u = "{prefix}/j1{loc_suffix}.json".format(prefix=prefix, loc_suffix=loc_suffix)
    if make:
        AutoURI(u).write(
            j1_json_contents(
                prefix=prefix,
                prefix_v41=prefix_v41,
                prefix_v421=prefix_v421,
                prefix_v5=prefix_v5,
                loc_suffix=loc_suffix,
            )
        )
    return u


def v41_json(prefix, make=False):
    u = "{prefix}/v41.json".format(prefix=prefix)
    if make:
        AutoURI(u).write(v41_json_contents())
    return u


def v421_tsv(
    prefix,
    prefix_v5=None,
    prefix_v1=None,
    prefix_v6=None,
    loc_suffix="",
    make=False,
    make_link_to_j1_json=False,
):
    u = "{prefix}/deeper/v421{loc_suffix}.tsv".format(
        prefix=prefix, loc_suffix=loc_suffix
    )
    if make:
        AutoURI(u).write(
            v421_tsv_contents(
                prefix=prefix,
                prefix_v5=prefix_v5,
                prefix_v1=prefix_v1,
                prefix_v6=prefix_v6,
                loc_suffix=loc_suffix,
                make_link_to_j1_json=make_link_to_j1_json,
            )
        )
    return u


def v5_csv(prefix, prefix_v6=None, make=False):
    u = "{prefix}/even/deeper/v5.csv".format(prefix=prefix)
    if make:
        AutoURI(u).write(v5_csv_contents(prefix=prefix, prefix_v6=prefix_v6))
    return u


def v6_txt(prefix, make=False):
    u = "{prefix}/v6.txt".format(prefix=prefix)
    if make:
        AutoURI(u).write(v6_txt_contents())
    return u


def common_paths():
    return [
        "/testing/abspath",
        "/testing/abspath/",
        "~/os/expandable",
        "~~/os/expandable",
        "~/~/os/expandable",
        "test/ok/man.csv",
        "http://hello.world.com/ok.txt",
        "https://hello.world.com/ok.txt",
        "http://hello.world.com/ok.txt?parameter1=true&parameter2=false",
        "https://hello.world.com/ok.txt?parameter1=true&parameter2=false",
        "http:/hello.world.com/notok.txt",
        "ftp:/hello.world.com/notok.txt",
        "dx://dnanexus-prj/not/supported.txt",
        "file:/notok.txt",
        "file://hostname/notok.txt",
        "s3://hello-world/ok.txt",
        "s3:/hello-world/not-ok.txt",
        "s3:\\hello-world\\not-ok.txt",
        "gs://hello-world/ok.txt",
        "gs:/hello-world/not-ok.txt",
        "gs:\\hello-world\\not-ok.txt",
        "!@#:;$@!#$F",
    ]


def recurse_raise_if_uri_not_exist(uri):
    uri = AutoURI(uri)
    if uri.is_valid:
        if uri.exists:
            if uri.ext == ".json":
                recurse_json(uri.read(), recurse_raise_if_uri_not_exist)
            elif uri.ext == ".tsv":
                recurse_tsv(uri.read(), recurse_raise_if_uri_not_exist)
            elif uri.ext == ".csv":
                recurse_csv(uri.read(), recurse_raise_if_uri_not_exist)
        else:
            raise Exception("URI is a valid path but does not exist.")
    return None, False


def make_files_in_dir(prefix, make_local_empty_dir_d_a=False):
    """Make a compllicated directory structure with empty files.

    Directory structure and empty files in it:
        $prefix (as root)
            a
            b/
                a
                b
            c/
                a/
                    a
                    b
                b/
                    a/
                        a
                c
            d/ (optional if make_local_empty_dir_d_a)
                a/ (optional if make_local_empty_dir_d_a)
    Args:
        make_local_empty_dir_d_a:
            Make a local empty dir ($prefix/d/a/).
            This flag should not be used for cloud buckets since
            they don't support sub-directories.
    Returns:
        List of file URIs.
    """
    file_a = os.path.join(prefix, "a")
    file_b_a = os.path.join(prefix, "b/a")
    file_b_b = os.path.join(prefix, "b/b")
    file_c_a_a = os.path.join(prefix, "c/a/a")
    file_c_a_b = os.path.join(prefix, "c/a/b")
    file_c_b_a_a = os.path.join(prefix, "c/b/a/a")
    file_c_c = os.path.join(prefix, "c/c")

    all_files = [
        file_a,
        file_b_a,
        file_b_b,
        file_c_a_a,
        file_c_a_b,
        file_c_b_a_a,
        file_c_c,
    ]

    for uri in all_files:
        AutoURI(uri).write("")
    if make_local_empty_dir_d_a:
        path = os.path.join(prefix, "d/a")
        os.makedirs(path, exist_ok=True)

    return all_files
