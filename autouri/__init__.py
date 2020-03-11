#!/usr/bin/env python3
"""
Features:    
    - Wraps google-cloud-storage for gs:// URIs.
    - Wraps boto3 for s3:// URIs.
    - Wraps gsutil CLI for direct transfer between gs:// and s3:// URIs.
    - Wraps Python requests for HTTP URLs.
    - Can presign a bucket URI to get a temporary public URL (e.g. for genome browsers).
    - File locking (using .lock file for bucket URIs).
    - MD5 hash checking to prevent unnecessary re-downloading.
    - Localization on a different URI type.
        - Keeping the original directory structure.
        - Can recursively localize all files in a CSV/TSV/JSON(value only) file.
"""
import argparse
import os
import sys

from .autouri import URIBase, AutoURI, logger
from .abspath import AbsPath
from .httpurl import HTTPURL
from .s3uri import S3URI
from .gcsuri import GCSURI


__version__ = '0.1.0'


def parse_args():
    parser = argparse.ArgumentParser()

    parent_src = argparse.ArgumentParser(add_help=False)
    parent_src.add_argument('src',
        help='Source file URI')

    parent_target = argparse.ArgumentParser(add_help=False)
    parent_target.add_argument('target',
        help='Target file/directory URI (e.g. gs://here/me.txt, s3://here/my-dir/) .'
             'Directory must have a trailing directory separator '
             '(e.g. /hello/. gs://where/am/i/).')

    parent_cp = argparse.ArgumentParser(add_help=False)
    parent_cp.add_argument('--use-gsutil-for-s3', action='store_true',
        help='Use gsutil for DIRECT TRANSFER between gs:// and s3://. '
             'gsutil must be installed and configured to have AWS credentials '
             'in ~/.boto file. Run "gsutil config" do generate it.')

    subparser = parser.add_subparsers(dest='action')

    p_metadata = subparser.add_parser(
        'metadata',
        help='AutoURI(src).get_metadata(): Get metadata of source.',
        parents=[parent_src])

    p_cp = subparser.add_parser(
        'cp',
        help='AutoURI(src).cp(target): Copy source to target. '
             'target must be a full filename/directory. '
             'Target directory must have a trailing directory separator '
             '(e.g. /)',
        parents=[parent_src, parent_target, parent_cp])

    p_read = subparser.add_parser(
        'read',
        help='AutoURI(src).read(): Read from source.',
        parents=[parent_src])

    p_write = subparser.add_parser(
        'write',
        help='AutoURI(src).write(text): Write text on source.',
        parents=[parent_src])
    p_write.add_argument('text',
        help='Text to be written to source file.')

    p_rm = subparser.add_parser(
        'rm',
        help='AutoURI(src).rm(): Delete source.',
        parents=[parent_src])

    p_loc = subparser.add_parser(
        'loc',
        help='type(target_dir).localize(src): Localize source on target directory (class)',
        parents=[parent_src, parent_target, parent_cp])
    p_loc.add_argument('--recursive', action='store_true',
        help='Recursively localize source into target class.')
    p_loc.add_argument('--make-md5-file', action='store_true',
        help='Make .md5 file to store file\'s md5 hexadecimal string. '
             'This file can be used later to prevent repeated md5sum calculation. '
             'This is for local path only.')

    p_presign = subparser.add_parser(
        'presign',
        help='AutoURI(src).get_presigned_url(). For cloud-based URIs only.',
        parents=[parent_src])
    p_presign.add_argument('--gcp-private-key-file',
        help='GCP private key file (JSON format) to presign gs:// URIs.')
    p_presign.add_argument('--duration', type=int,
        help='Duration of presigned URL in seconds.')

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()

    return parser.parse_args()


def get_local_file_if_valid(s):
    path = os.path.expanduser(s)
    abspath = os.path.abspath(path)
    dirname = os.path.dirname(abspath)

    if os.path.exists(path) or os.path.exists(dirname):
        tailing_slash = os.sep if s.endswith(os.sep) else ''
        return abspath + tailing_slash
    return s


def main():
    args = parse_args()

    src = get_local_file_if_valid(args.src)

    if args.action in ('cp', 'loc'):
        if args.use_gsutil_for_s3:
            GCSURI.init_gcsuri(use_gsutil_for_s3=True)
        target = get_local_file_if_valid(args.target)

    if args.action == 'metadata':
        m = AutoURI(src).get_metadata()
        print(m)

    elif args.action == 'cp':
        u_src = AutoURI(src)
        sep = AutoURI(target).__class__.get_path_sep()
        if target.endswith(sep):
            type_ = 'dir'
            target = sep.join([target.rstrip(sep), u_src.basename])
            print(target)
        else:
            type_ = 'file'
        u_src.cp(target)
        logger.info('Copying from file {s} to {type} {t} done'.format(
            s=src, type=type_, t=target))

    elif args.action == 'read':
        s = AutoURI(src).read()
        print(s)

    elif args.action == 'write':
        AutoURI(src).write(args.text)
        logger.info('Text has been written to {s}'.format(s=src))

    elif args.action == 'rm':        
        u = AutoURI(src)
        if not u.exists:
            raise ValueError('File does not exist. {s}'.format(s=src))
        u.rm()
        logger.info('Deleted {s}'.format(s=src))

    elif args.action == 'loc':
        _, localized = AutoURI(target).__class__.localize(
            src,
            recursive=args.recursive,
            loc_prefix=target)
        if localized:
            logger.info('Localized {s} on {t}'.format(s=src, t=target))
        else:
            logger.info('No need to localize {s} on {t}'.format(s=src, t=target))

    elif args.action == 'presign':
        u = AutoURI(src)

        if isinstance(u, GCSURI):
            if not args.gcp_private_key_file:
                raise ValueError('GCP private key file (--gcp-private-key-file) not found.')
            url = u.get_presigned_url(duration=args.duration,
                                      private_key_file=args.gcp_private_key_file)
            print(url)

        elif isinstance(u, S3URI):
            url = u.get_presigned_url(duration=args.duration)
            print(url)

        else:
            raise ValueError('Presigning URL is available for cloud URIs (gs://, s3://) only.')


if __name__ == '__main__':
    main()
