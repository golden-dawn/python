import os
import stxdb
import sys

class StxEOD :

    upload_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    eod_name   = '{0:s}/eod_upload.txt'.format(upload_dir)

    def __init__(self, cnx, in_dir, eod_tbl, split_tbl, extension = '.txt') :
        self.in_dir    = in_dir
        self.eod_tbl   = eod_tbl
        self.split_tbl = split_tbl
        self.extension = extension
        self.cnx       = cnx
        if cnx.query("show tables like '{0:s}'".format(eod_tbl)) == 0 :
            try :
                cnx.query('CREATE TABLE `{0:s}` ('\
                          '`stk` varchar(8) NOT NULL,'\
                          '`dt` varchar(10) NOT NULL,'\
                          '`o` decimal(9,2) DEFAULT NULL,'\
                          '`h` decimal(9,2) DEFAULT NULL,'\
                          '`l` decimal(9,2) DEFAULT NULL,'\
                          '`c` decimal(9,2) DEFAULT NULL,'\
                          '`v` int(11) DEFAULT NULL,'\
                          'PRIMARY KEY (`stk`,`dt`)'\
                          ') ENGINE=MyISAM DEFAULT CHARSET=utf8'.\
                          format(eod_tbl))
                print('Successfully created DB table {0:s}'.format(eod_tbl))
            except :
                e = sys.exc_info()[1]
                print('Failed to create DB table {0:s}, error {1:s}'.\
                      format(eod_tbl, str(e)))
                raise
        if cnx.query("show tables like '{0:s}'".format(split_tbl)) == 0 :
            try :
                cnx.query('CREATE TABLE `{0:s}` ('\
                          '`stk` varchar(8) NOT NULL,'\
                          '`dt` varchar(10) NOT NULL,'\
                          '`ratio` decimal(8,4) DEFAULT NULL,'\
                          'PRIMARY KEY (`stk`,`dt`)'\
                          ') ENGINE=MyISAM DEFAULT CHARSET=utf8'.\
                          format(split_tbl))
                print('Successfully created DB table {0:s}'.format(split_tbl))
            except :
                e = sys.exc_info()[1]
                print('Failed to create DB table {0:s}, error {1:s}'.\
                      format(split_tbl, str(e)))
                raise


    def load_from_files(self, stx = '') :
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
            self.load_stk(fname)
            ixx     += 1
            if ixx % 500 == 0 or ixx == num_stx :
                print('Uploaded {0:5d}/{1:5d} stocks'.format(ixx, num_stx))


    def reload_from_files(self, stx = '') :
        if stx == '' :
            lst        = [f for f in os.listdir(self.in_dir) \
                          if f.endswith(self.extension)]
        else :
            lst      = []
            stk_list = stx.split(',')
            for stk in stk_list :
                lst.append('{0:s}{1:s}'.format(stk.strip(), self.extension))
        num_stx    = len(lst)
        print('Reloading data for {0:d} stocks'.format(num_stx))
        ixx        = 0
        reloaded   = 0
        cursor     = self.cnx.cursor()
        for fname in lst :
            cursor.execute("select count(*) from {0:s} where stk='{1:s}'".\
                           format(self.eod_tbl, fname[:-4]))
            for n in cursor :
                if n[0] == 0 :
                    self.repair_file(fname)
                    self.load_stk(fname)
            ixx   += 1
            if ixx % 500 == 0 or ixx == num_stx :
                print('Reloaded {0:5d}/{1:5d} stocks'.format(ixx, num_stx))
        cursor.close()

                
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
        q_split = 'insert into {0:s} values'.format(self.split_tbl)
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
            self.cnx.query("load data infile '{0:s}' into table {1:s}". \
                           format(self.eod_name, self.eod_tbl))
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

    def repair_file(self, fname) :
        # This function removes duplicate entries and incorrect rows
        # VPA, WEG: duplicate entries
        # VOXXE, WEFND: invalid utf8 character
        # QQQQ: list index out of range
        # Failed to upload LDX.txt, error (1264, "Out of range value for column 'ratio' at row 1")
        pass
        
if __name__ == '__main__' :
    cnx  = stxdb.connect()
    seod = StxEOD(cnx, 'c:/goldendawn/bkp', 'eod1', 'split1')
    seod.load_from_files('XTR')
