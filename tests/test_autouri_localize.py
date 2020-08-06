"""Basic recursive localization testing is already done each class' testing module.
Here, we test more extreme/edge cases for recursive localization:
    1) Direct/indirect self reference
    2) Remote files with mixed storage types
"""
import os
from typing import Tuple

import pytest

from autouri.abspath import AbsPath
from autouri.autouri import AutoURI, AutoURIRecursionError

from .files import recurse_raise_if_uri_not_exist


def test_localize_self_ref(
    local_test_path,
    gcs_j1_json_self_ref,
    gcs_v41_json_self_ref,
    gcs_v421_tsv_self_ref,
    gcs_v5_csv_self_ref,
    gcs_v6_txt_self_ref,
) -> Tuple[str, bool]:
    """Test detection of direct/indirect self reference
    while localizing GCS files on local storage.
    This is not only for detecting self references but also for any deep recursion
    beyond the depth limit (10 by default)

    Indirect self referencing in GCS files suffixed with _self_ref:
        v1.json -> v421.tsv -> v1.json -> ...
    """
    loc_prefix = os.path.join(local_test_path, "test_localize_self_ref")

    # localization from remote storages
    for j1_json in (gcs_j1_json_self_ref,):
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()

        with pytest.raises(AutoURIRecursionError):
            loc_uri, localized = AbsPath.localize(
                u_j1_json, recursive=True, loc_prefix=loc_prefix_
            )


def test_localize_mixed(
    local_test_path,
    mixed_j1_json,
    mixed_v41_json,
    mixed_v421_tsv,
    mixed_v5_csv,
    mixed_v6_txt,
) -> Tuple[str, bool]:
    """Test recursive localization of files on mixed storages
    Target is local storage.
    """
    loc_prefix = os.path.join(local_test_path, "test_localize_mixed")

    # localization from remote storages
    for j1_json in (mixed_j1_json,):
        u_j1_json = AutoURI(j1_json)
        loc_prefix_ = loc_prefix + u_j1_json.__class__.get_loc_suffix()

        loc_uri, localized = AbsPath.localize(
            u_j1_json, recursive=True, return_flag=True, loc_prefix=loc_prefix_
        )
        # check if all URIs defeind in localized JSON file exist
        recurse_raise_if_uri_not_exist(loc_uri)
