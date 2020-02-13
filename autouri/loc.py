#!/usr/bin/env python3
"""
Author: Jin Lee (leepc12@gmail.com) at ENCODE-DCC
"""

import os
import json


def init_loc():
    pass

def get_loc_uri(src_uri, force_loc=False):
    """Defines how source URI can be localized on this class' storage.
    Args:
        src_uri:
            Source URI
        force_loc:
            Force localization on a cache directory even for the same URI type

    Returns a tuple of:
        uri:
            a localized URI path without actually localizing a file.
        need_to_copy:
            whether it's already localized or not
    """
    if AbsPath.LOC_PREFIX is None:
        raise ValueError('loc_prefix must be defined for AbsPath')
    src_uri = AutoURI(src_uri)
    localized = AutoURI(__class__.get_path_sep().join([
        AbsPath._loc_prefix,
        src_uri.get_dirname(no_scheme=True),
        src_uri.get_basename()
    ]))
    return localized, True


def loc_recurse(uri, dest_uri_cls, make_md5_file=False):
    """Default recursion function for AutoURI's localize(recurse=True)    

    Supported file extensions:
        .json
        .csv
        .tsv
    Return:
        Localized URI, updated
    """
    from autouri.autouri import AutoURI, logger
    from autouri.abspath import AbsPath

    ext = uri.get_ext()
    if not uri.is_valid() or not ext:
        return None, False

    dest, updated = None, False

    if ext == '.json':
        def recurse_dict(d, dest_uri_cls, d_parent=None, d_parent_key=None,
                         lst=None, lst_idx=None, updated=False):
            if isinstance(d, dict):
                for k, v in d.items():
                    updated |= recurse_dict(v, dest_uri_cls, d_parent=d,
                                            d_parent_key=k, updated=updated)
            elif isinstance(d, list):
                for i, v in enumerate(d):
                    updated |= recurse_dict(v, dest_uri_cls, lst=d,
                                            lst_idx=i, updated=updated)
            elif isinstance(d, str):
                assert(d_parent is not None or lst is not None)
                c = AutoURI(d)
                new_uri, updated_ = c.localize(
                    dest_uri_cls=dest_uri_cls, make_md5_file=make_md5_file
                    recursive=True)
                updated |= updated_

                if updated_:
                    if d_parent is not None:
                        d_parent[d_parent_key] = new_uri.get_uri()
                    elif lst is not None:
                        lst[lst_idx] = new_uri.get_uri()
                    else:
                        raise ValueError('Recursion failed.')
                return updated

        d = json.loads(uri.read())
        dest, updated = recurse_dict(d)

        if updated:
            new_uri = '{prefix}{suffix}{ext}'.format(
                prefix=os.path.splitext(AbsPath.get_localized_uri(uri))[0],
                suffix=dest_uri_cls.get_loc_suffix(),
                ext=ext)
            new_uri.write(json.dumps(new_d, indent=4))

            local_tmp_uri = os.path.join(
                CaperURI.TMP_DIR,
                hashlib.md5(new_uri.encode('utf-8')).hexdigest(),
                os.path.basename(new_uri))
            return CaperURI(local_tmp_uri).write_str_to_file(j), True
        else:
            return self._uri, False       

    elif ext in ('.csv', '.tsv'):
        delim = ext
        contents = uri.read()

        new_contents = []
        for line in contents.split('\n'):
            new_values = []
            for v in line.split(delim):
                c = CaperURI(v)
                new_uri, updated_ = c.deepcopy(
                    dest_uri_cls=dest_uri_cls, uri_exts=uri_exts)
                updated |= updated_
                if updated_:
                    new_values.append(new_uri.get_uri())
                else:
                    new_values.append(v)

            new_contents.append(delim.join(new_values))

    if updated:
        new_uri = '{prefix}{suffix}{ext}'.format(
            prefix=fname_wo_ext, suffix=dest_uri_cls, ext=ext)
        s = '\n'.join(new_contents)
        local_tmp_uri = os.path.join(
            CaperURI.TMP_DIR,
            hashlib.md5(new_uri.encode('utf-8')).hexdigest(),
            os.path.basename(new_uri))
        return CaperURI(local_tmp_uri).write_str_to_file(s), True
    else:
        return self._uri, False

    return dest, updated
