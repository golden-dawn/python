import os
import stxdb
import sys

class StxEOD :

    upload_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    eod_name   = '{0:s}/eod_upload.txt'.format(upload_dir)

    def __init__(self, in_dir, extension = '.txt') :
        self.in_dir    = in_dir
        self.extension = extension
        self.cnx       = stxdb.connect()
    
    def load_from_files(self, stx = '') :
        if stx == '' :
            lst        = [f for f in os.listdir(self.in_dir) \
                          if f.endswith(self.extension)]
            for fname in lst :
                self.load_stk(fname)
        else :
            stk_list   = stx.split(',')
            for stk in stk_list :
                self.load_stk('{0:s}.txt'.format(stk.strip()))

    def load_stk(self, short_fname) :
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
        q_split = 'insert into split values'
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
                        sep = ' ' if splits == 0 else ','
                        splits += 1
                        q_split = "{0:s}{1:s}('{2:s}','{3:s}',{4:f})".format \
                            (q_split, sep, stk, tokens[0], float(tokens[6]))
        except :
            e = sys.exc_info()[1]
            print('Failed to parse {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        try :
            self.cnx.query("load data infile '{0:s}' into table eod". \
                           format(self.eod_name))
        except :
            e = sys.exc_info()[1]
            print('Failed to upload {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        if splits > 0 :
            try :
                self.cnx.query(q_split)
            except :
                e = sys.exc_info()[1]
                print('Failed to upload {0:s}, error {1:s}'.\
                      format(short_fname, str(e)))
        print('{0:s}: uploaded {1:d} eods and {2:d} splits'.\
              format(stk, eods, splits))
