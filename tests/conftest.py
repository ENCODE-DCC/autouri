import hashlib
import os

import pytest

from .files import j1_json, v5_csv, v6_txt, v6_txt_contents, v41_json, v421_tsv

GCS_ENDPOINT_URL = "https://storage.googleapis.com/"


def pytest_addoption(parser):
    parser.addoption("--ci-prefix", required=True, help="Prefix for CI test.")
    parser.addoption(
        "--s3-root",
        default="s3://encode-test-autouri/tmp",
        help="S3 root path for CI test. "
        "This S3 bucket must be configured without versioning. "
        "Make it publicly accessible. "
        "Read access for everyone is enough for testing. ",
    )
    parser.addoption(
        "--s3-public-url-test-v6-file",
        default="s3://encode-test-autouri/tmp/v6.txt",
        help='Write "v6: Hello World" to this file named "v6.txt" '
        'and grant "Read object" permission on it. '
        "Since S3 object does not inherit ACL from bucket/parent "
        "and S3URI does not have methods to control ACL of an object "
        "so this is the only way to test get_public_url(self) method in "
        "S3URI.",
    )
    parser.addoption(
        "--gcs-root",
        default="gs://encode-test-autouri/tmp",
        help="GCS root path for CI test. "
        "This GCS bucket must be publicly accessible "
        "(read access for everyone is enough for testing).",
    )
    parser.addoption(
        "--gcs-root-url",
        default="gs://encode-test-autouri/tmp_url",
        help="GCS root path for CI test for URLs. "
        "This GCS bucket must be public so that "
        "anyone can access to it via an URL with "
        "an endpoint GCS_ENDPOINT_URL. ",
    )
    parser.addoption(
        "--gcp-private-key-file",
        required=True,
        help="GCP private key file (JSON format) to test presigning GCS URIs. "
        "Generate one for a service account that has an admin access to both "
        "--gcs-root and --gcs-root-url.",
    )


@pytest.fixture(scope="session")
def ci_prefix(request):
    return request.config.getoption("--ci-prefix").rstrip("/")


@pytest.fixture(scope="session")
def s3_root(request):
    """S3 root to generate test S3 URIs on."""
    return request.config.getoption("--s3-root").rstrip("/")


@pytest.fixture(scope="session")
def s3_public_url_test_v6_file(request):
    return request.config.getoption("--s3-public-url-test-v6-file")


@pytest.fixture(scope="session")
def gcs_root(request):
    """GCS root to generate test GCS URIs on."""
    return request.config.getoption("--gcs-root").rstrip("/")


@pytest.fixture(scope="session")
def gcs_root_url(request):
    """GCS root to generate test URLs on. This GCS bucket should be public."""
    return request.config.getoption("--gcs-root-url").rstrip("/")


@pytest.fixture(scope="session")
def gcp_private_key_file(request):
    """GCS private key file to test presigning GCS URIs."""
    return request.config.getoption("--gcp-private-key-file")


@pytest.fixture(scope="session")
def local_test_path(tmpdir_factory, ci_prefix):
    return tmpdir_factory.mktemp(ci_prefix).realpath()


@pytest.fixture(scope="session")
def s3_test_path(s3_root, ci_prefix):
    return "{s3_root}/{ci_prefix}".format(s3_root=s3_root, ci_prefix=ci_prefix)


@pytest.fixture(scope="session")
def gcs_test_path(gcs_root, ci_prefix):
    return "{gcs_root}/{ci_prefix}".format(gcs_root=gcs_root, ci_prefix=ci_prefix)


@pytest.fixture(scope="session")
def gcs_test_path_url(gcs_root_url, ci_prefix):
    return "{gcs_root_url}/{ci_prefix}".format(
        gcs_root_url=gcs_root_url, ci_prefix=ci_prefix
    )


@pytest.fixture(scope="session")
def gcs_test_path_self_ref(gcs_test_path):
    return "{gcs_test_path}/self_ref".format(gcs_test_path=gcs_test_path)


@pytest.fixture(scope="session")
def url_test_path(gcs_root_url, ci_prefix):
    url_root = gcs_root_url.replace("gs://", GCS_ENDPOINT_URL, 1)
    return "{url_root}/{ci_prefix}".format(url_root=url_root, ci_prefix=ci_prefix)


@pytest.fixture(scope="session")
def mixed_local_test_path(local_test_path):
    d = os.path.join(local_test_path, "mixed")
    os.makedirs(d, exist_ok=True)
    return d


@pytest.fixture(scope="session")
def mixed_s3_test_path(s3_test_path):
    return "{s3_test_path}/mixed".format(s3_test_path=s3_test_path)


@pytest.fixture(scope="session")
def mixed_gcs_test_path(gcs_test_path):
    return "{gcs_test_path}/mixed".format(gcs_test_path=gcs_test_path)


@pytest.fixture(scope="session")
def mixed_gcs_test_path_url(gcs_test_path_url):
    return "{gcs_test_path_url}/mixed".format(gcs_test_path_url=gcs_test_path_url)


@pytest.fixture(scope="session")
def mixed_url_test_path(url_test_path):
    return "{url_test_path}/mixed".format(url_test_path=url_test_path)


@pytest.fixture(scope="session")
def local_j1_json(local_test_path):
    return j1_json(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v41_json(local_test_path):
    return v41_json(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v421_tsv(local_test_path):
    return v421_tsv(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v5_csv(local_test_path):
    return v5_csv(local_test_path, make=True)


@pytest.fixture(scope="session")
def local_v6_txt(local_test_path):
    return v6_txt(local_test_path, make=True)


@pytest.fixture(scope="session")
def s3_j1_json(s3_test_path):
    return j1_json(s3_test_path, make=True)


@pytest.fixture(scope="session")
def s3_v41_json(s3_test_path):
    return v41_json(s3_test_path, make=True)


@pytest.fixture(scope="session")
def s3_v421_tsv(s3_test_path):
    return v421_tsv(s3_test_path, make=True)


@pytest.fixture(scope="session")
def s3_v5_csv(s3_test_path):
    return v5_csv(s3_test_path, make=True)


@pytest.fixture(scope="session")
def s3_v6_txt(s3_test_path):
    return v6_txt(s3_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_j1_json(gcs_test_path):
    return j1_json(gcs_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_v41_json(gcs_test_path):
    return v41_json(gcs_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_v421_tsv(gcs_test_path):
    return v421_tsv(gcs_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_v5_csv(gcs_test_path):
    return v5_csv(gcs_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_v6_txt(gcs_test_path):
    return v6_txt(gcs_test_path, make=True)


@pytest.fixture(scope="session")
def gcs_j1_json_url(gcs_test_path_url, url_test_path):
    return j1_json(
        gcs_test_path_url,
        make=True,
        prefix_v41=url_test_path,
        prefix_v421=url_test_path,
        prefix_v5=url_test_path,
    )


@pytest.fixture(scope="session")
def gcs_v41_json_url(gcs_test_path_url):
    return v41_json(gcs_test_path_url, make=True)


@pytest.fixture(scope="session")
def gcs_v421_tsv_url(gcs_test_path_url, url_test_path):
    return v421_tsv(
        gcs_test_path_url, make=True, prefix_v5=url_test_path, prefix_v1=url_test_path
    )


@pytest.fixture(scope="session")
def gcs_v5_csv_url(gcs_test_path_url, url_test_path):
    return v5_csv(gcs_test_path_url, make=True, prefix_v6=url_test_path)


@pytest.fixture(scope="session")
def gcs_v6_txt_url(gcs_test_path_url):
    return v6_txt(gcs_test_path_url, make=True)


@pytest.fixture(scope="session")
def gcs_j1_json_self_ref(gcs_test_path_self_ref):
    return j1_json(gcs_test_path_self_ref, make=True)


@pytest.fixture(scope="session")
def gcs_v41_json_self_ref(gcs_test_path_self_ref):
    return v41_json(gcs_test_path_self_ref, make=True)


@pytest.fixture(scope="session")
def gcs_v421_tsv_self_ref(gcs_test_path_self_ref):
    return v421_tsv(gcs_test_path_self_ref, make=True, make_link_to_j1_json=True)


@pytest.fixture(scope="session")
def gcs_v5_csv_self_ref(gcs_test_path_self_ref):
    return v5_csv(gcs_test_path_self_ref, make=True)


@pytest.fixture(scope="session")
def gcs_v6_txt_self_ref(gcs_test_path_self_ref):
    return v6_txt(gcs_test_path_self_ref, make=True)


@pytest.fixture(scope="session")
def url_j1_json(gcs_j1_json_url, url_test_path):
    """URL is read-only. So this is a link to the actual file on GCS."""
    return j1_json(url_test_path, make=False)


@pytest.fixture(scope="session")
def url_v41_json(gcs_v41_json_url, url_test_path):
    """URL is read-only. So this is a link to the actual file on GCS."""
    return v41_json(url_test_path, make=False)


@pytest.fixture(scope="session")
def url_v421_tsv(gcs_v421_tsv_url, url_test_path):
    """URL is read-only. So this is a link to the actual file on GCS."""
    return v421_tsv(url_test_path, make=False)


@pytest.fixture(scope="session")
def url_v5_csv(gcs_v5_csv_url, url_test_path):
    """URL is read-only. So this is a link to the actual file on GCS."""
    return v5_csv(url_test_path, make=False)


@pytest.fixture(scope="session")
def url_v6_txt(gcs_v6_txt_url, url_test_path):
    """URL is read-only. So this is a link to the actual file on GCS."""
    return v6_txt(url_test_path, make=False)


@pytest.fixture(scope="session")
def mixed_j1_json(
    mixed_local_test_path, mixed_s3_test_path, mixed_gcs_test_path, mixed_url_test_path
):
    return j1_json(
        mixed_gcs_test_path,
        make=True,
        prefix_v41=mixed_url_test_path,
        prefix_v421=mixed_s3_test_path,
        prefix_v5=mixed_local_test_path,
    )


@pytest.fixture(scope="session")
def mixed_v41_json(mixed_gcs_test_path_url, mixed_url_test_path):
    """Actual file is created on GCS then it will have a correspondig URL."""
    gcs_path = v41_json(mixed_gcs_test_path_url, make=True)
    url = gcs_path.replace(mixed_gcs_test_path_url, mixed_url_test_path, 1)
    return url


@pytest.fixture(scope="session")
def mixed_v421_tsv(mixed_s3_test_path, mixed_gcs_test_path, mixed_local_test_path):
    return v421_tsv(
        mixed_s3_test_path,
        make=True,
        prefix_v5=mixed_local_test_path,
        prefix_v1=mixed_gcs_test_path,
    )


@pytest.fixture(scope="session")
def mixed_v5_csv(mixed_local_test_path, mixed_s3_test_path):
    return v5_csv(mixed_local_test_path, make=True, prefix_v6=mixed_s3_test_path)


@pytest.fixture(scope="session")
def mixed_v6_txt(mixed_s3_test_path):
    return v6_txt(mixed_s3_test_path, make=True)


@pytest.fixture(scope="session")
def v6_txt_size():
    return len(v6_txt_contents())


@pytest.fixture(scope="session")
def v6_txt_md5_hash():
    return hashlib.md5(v6_txt_contents().encode()).hexdigest()
