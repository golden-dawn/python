import argparse
from configparser import ConfigParser
import logging
import os
from stx_eod import StxEOD


if __name__ == '__main__':
    config = ConfigParser()
    # parse existing configuration file
    cfg_file_path = os.path.abspath(os.path.join(os.getenv('HOME'),
                                                 'stx_cfg.ini'))
    config.read(cfg_file_path)
    parser = argparse.ArgumentParser(description='Stock splits')
    parser.add_argument('-s', '--splitsfile',
                        help='name of the splits file',
                        type=str,
                        required=True)
    parser.add_argument('-d', '--datadir',
                        help='directory that contains the splits file',
                        default=config.get('datafeed', 'data_dir'),
                        type=str)
    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    logging.info('stx_splits: data dir = {0:s}, splits file = {1:s}'.
                 format(args.datadir, args.splitsfile))
    seod = StxEOD(args.datadir)
    splits_file = os.path.join(args.datadir, args.splitsfile)
    seod.upload_splits(splits_file)
