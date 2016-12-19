import os
from stxdb import *
import sys

class StxEOD :

    upload_dir       = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    eod_name         = '{0:s}/eod_upload.txt'.format(upload_dir)
    sql_create_eod   = 'CREATE TABLE `{0:s}` ('\
                       '`stk` varchar(8) NOT NULL,'\
                       '`dt` varchar(10) NOT NULL,'\
                       '`o` decimal(9,2) DEFAULT NULL,'\
                       '`h` decimal(9,2) DEFAULT NULL,'\
                       '`l` decimal(9,2) DEFAULT NULL,'\
                       '`c` decimal(9,2) DEFAULT NULL,'\
                       '`v` int(11) DEFAULT NULL,'\
                       'PRIMARY KEY (`stk`,`dt`)'\
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_split = 'CREATE TABLE `{0:s}` ('\
                       '`stk` varchar(8) NOT NULL,'\
                       '`dt` varchar(10) NOT NULL,'\
                       '`ratio` decimal(8,4) DEFAULT NULL,'\
                       'PRIMARY KEY (`stk`,`dt`)'\
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    
    def __init__(self, in_dir, eod_tbl, split_tbl, extension = '.txt') :
        self.in_dir    = in_dir
        self.eod_tbl   = eod_tbl
        self.split_tbl = split_tbl
        self.extension = extension
        db_create_missing_table(eod_tbl, cls.sql_create_eod)
        print('EOD DB table: {0:s}'.format(eod_tbl))
        db_create_missing_table(split_tbl, cls.sql_create_split)
        print('Split DB table: {0:s}'.format(split_tbl))

        
    def load_ctin_files(self, stx = '') :
        split_fname  = '{0:s}/splits_ctin.txt'.format(cls.upload_dir)
        try :
            os.remove(split_fname)
            print('Removed {0:s}'.format(split_fname))
        except :
            pass
        if stx == '' :
            lst      = [f for f in os.listdir(self.in_dir) \
                        if f.endswith(self.extension)]
        else :
            lst      = []
            stk_list = stx.split(',')
            for stk in stk_list :
                lst.append('{0:s}{1:s}'.format(stk.strip(), self.extension))
        num_stx      = len(lst)
        print('Loading data for {0:d} stocks'.format(num_stx))
        ixx          = 0
        for fname in lst :
            self.load_ctin_stk(fname, split_fname)
            ixx     += 1
            if ixx % 500 == 0 or ixx == num_stx :
                print('Uploaded {0:5d}/{1:5d} stocks'.format(ixx, num_stx))
        try :
            db_upload_file(split_fname, self.split_tbl, 2)
            print('Successfully uploaded the splits in the DB')
        except :
            e = sys.exc_info()[1]
            print('Failed to upload the splits from file {0:s}, error {1:s}'.\
                  format(split_fname, str(e)))
            
            
    def load_ctin_stk(self, short_fname, split_fname) :
        fname = '{0:s}/{1:s}'.format(self.in_dir, short_fname)
        stk   = short_fname[:-4]
        try :
            with open(fname, 'r') as ifile :
                lines = ifile.readlines()
        except :
            e = sys.exc_info()[1]
            print('Failed to read {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        eods, splits = 0, 0
        try :
            with open(self.eod_name, 'w') as eod :
                for line in lines:
                    start = 1 if line.startswith('*') else 0
                    tokens = line[start:].strip().split(' ')
                    eod.write('{0:s}\t{1:s}\t{2:s}\t{3:s}\t{4:s}\t{5:s}\t' \
                              '{6:s}\n'.\
                              format(stk, tokens[0], tokens[1], tokens[2],
                                     tokens[3], tokens[4], tokens[5]))
                    eods += 1
                    if start == 1 :
                        splits += 1
                        with open(split_fname, 'a') as split_file :
                            split_file.write('{0:s}\t{1:s}\t{2:f}\n'.format \
                                             (stk, tokens[0], float(tokens[6])))
        except :
            e = sys.exc_info()[1]
            print('Failed to parse {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        try :
            db_upload_file(self.eod_name, self.eod_tbl, 2)
            print('{0:s}: uploaded {1:d} eods and {2:d} splits'.\
                  format(stk, eods, splits))
        except :
            e = sys.exc_info()[1]
            print('Failed to upload {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))

        
if __name__ == '__main__' :
    seod = StxEOD('c:/goldendawn/bkp', 'eod1', 'split1')
    seod.load_ctin_files('XTR')
