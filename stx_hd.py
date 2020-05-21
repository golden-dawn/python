import argparse
import os
from stx_eod import StxEOD


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stock splits')
    parser.add_argument('-s', '--splitsfile',
                        help='name of the splits file',
                        type=str,
                        required=True)
    parser.add_argument('-d', '--datadir',
                        help='directory that contains the splits file',
                        default=os.getenv('DOWNLOAD_DIR'),
                        type=str)
    args = parser.parse_args()

    print('stx_splits: data dir = {0:s}, splits file = {1:s}'.
          format(args.datadir, args.splitsfile))
    seod = StxEOD(args.datadir)
    splits_file = os.path.join(args.datadir, args.splitsfile)
    seod.upload_splits(splits_file)
