import csv
from datetime import datetime
import os
import pandas as pd
from shutil import copyfile, rmtree
from stxcal import *
from stxdb import *
from stxts import StxTS
import sys

class StxEOD :

    sh_dir           = 'c:/goldendawn/stockhistory'
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
        db_create_missing_table(eod_tbl, self.sql_create_eod)
        print('EOD DB table: {0:s}'.format(eod_tbl))
        db_create_missing_table(split_tbl, self.sql_create_split)
        print('Split DB table: {0:s}'.format(split_tbl))


    # Load my historical data.  Load each stock and accumulate splits.
    # Upload splits at the end.
    def load_my_files(self, stx = '') :
        split_fname  = '{0:s}/my_splits.txt'.format(self.upload_dir)
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
            self.load_my_stk(fname, split_fname)
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
            

    # Upload each stock.  Split lines are prefixed with '*'.  Upload
    # stock data separately and then accumulate each stock.
    def load_my_stk(self, short_fname, split_fname) :
        fname                   = '{0:s}/{1:s}'.format(self.in_dir, short_fname)
        stk                     = short_fname[:-4]
        try :
            with open(fname, 'r') as ifile :
                lines           = ifile.readlines()
        except :
            e                   = sys.exc_info()[1]
            print('Failed to read {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        eods, splits            = 0, 0
        try :
            with open(self.eod_name, 'w') as eod :
                for line in lines:
                    start       = 1 if line.startswith('*') else 0
                    toks        = line[start:].strip().split(' ')
                    o,h,l,c,v   = float(toks[1]), float(toks[2]), \
                                  float(toks[3]), float(toks[4]), int(toks[5])
                    if o > 0.05 and h > 0.05 and l > 0.05 and c > 0.05 and \
                       v > 0 :
                        eod.write('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'\
                                  '{5:.2f}\t{6:d}\n'.\
                                  format(stk, toks[0], o, h, l, c, v))
                    eods       += 1
                    if start == 1 :
                        splits += 1
                        with open(split_fname, 'a') as split_file :
                            split_file.write('{0:s}\t{1:s}\t{2:f}\n'.format \
                                             (stk, toks[0], float(toks[6])))
        except :
            e                   = sys.exc_info()[1]
            print('Failed to parse {0:s}, error {1:s}'.\
                  format(short_fname, str(e)))
            return
        try :
            db_upload_file(self.eod_name, self.eod_tbl, 2)
            print('{0:s}: uploaded {1:d} eods and {2:d} splits'.\
                  format(stk, eods, splits))
        except :
            e                   = sys.exc_info()[1]
            print('Failed to upload {0:s}: {1:s}'.format(short_fname, str(e)))

        
    # Parse delta neutral stock history.  First, separate each yearly
    # data into stock files, and upload each stock file.  Then upload
    # all the splits into the database.  We will worry about
    # missing/wrong splits and volume adjustments later.
    def load_deltaneutral_files(self, stks = '') :
        for yr in range(2001, 2017) :
            fname    = '{0:s}/stockhistory_{1:d}.csv'.format(self.sh_dir, yr)
            stx      = {}
            stk_list = [] if stks == '' else stks.split(',')
            with open(fname) as csvfile :
                frdr = csv.reader(csvfile)
                for row in frdr :
                    stk  = row[0].strip()
                    if (stk_list and stk not in stk_list) or ('/' in stk) or \
                       ('*' in stk) or (stk in ['AUX', 'PRN']) :
                        continue
                    data = stx.get(stk, [])
                    o,h,l,c,v = float(row[2]), float(row[3]), float(row[4]), \
                                float(row[5]), int(row[7])
                    if o > 0.05 and h > 0.05 and l > 0.05 and c > 0.05 and \
                       v > 0 :
                        data.append('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'\
                                    '{5:.2f}\t{6:d}\n'.\
                                    format(stk, str(datetime.strptime\
                                                    (row[1],'%m/%d/%Y').date()),
                                           o, h, l, c, v))
                    stx[stk] = data
            print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
            for stk, recs in stx.items() :
                with open('{0:s}/{1:s}.txt'.format(self.in_dir, stk), 'a') \
                     as ofile :
                    for rec in recs :
                        ofile.write(rec)
        lst      = [f for f in os.listdir(self.in_dir) \
                    if f.endswith(self.extension)]
        for fname in lst :
            copyfile('{0:s}/{1:s}'.format(self.in_dir, fname), self.eod_name)
            try :
                db_upload_file(self.eod_name, self.eod_tbl, 2)
                print('{0:s}: uploaded eods'.format(stk))
            except :
                e = sys.exc_info()[1]
                print('Failed to upload {0:s}, error {1:s}'.format(stk, str(e)))
        self.load_deltaneutral_splits(stk_list)


    # Load the delta neutral splits into the database
    def load_deltaneutral_splits(self, stk_list) :
        # this part uploads the splits
        in_fname         = '{0:s}/stocksplits.csv'.format(self.sh_dir)
        out_fname        = '{0:s}/stocksplits.txt'.format(self.upload_dir)
        with open(in_fname, 'r') as csvfile :
            with open(out_fname, 'w') as dbfile :
                frdr         = csv.reader(csvfile)
                for row in frdr :
                    stk  = row[0].strip()
                    if stk_list and stk not in stk_list :
                        continue
                    dt   = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                    dbfile.write('{0:s}\t{1:s}\t{2:f}\n'.\
                                 format(stk,prev_busday(dt),1/float(row[2])))
        try :
            db_upload_file(out_fname, self.split_tbl, 2)
            print('Uploaded delta neutral splits')
        except :
            e = sys.exc_info()[1]
            print('Failed to upload splits, error {0:s}'.format(str(e)))


    # Perform reconciliation with the option spots.  First get all the
    # underliers for which we have spot prices within a given
    # interval.  Then, reconcile for each underlier
    def reconcile_spots(self, sd = None, ed = None, stx = '') :
        if stx == '' :
            res      = db_read_cmd('select distinct stk from opt_spots {0:s}'.\
                                   format(db_sql_timeframe(sd, ed, False)))
            stk_list = [stk[0] for stk in res]
        else :
            stk_list = stx.split(',')
        with open('{0:s}/spot_recon_{1:s}.csv'.format(self.in_dir,
                                                      self.eod_tbl),
                  'a') as ofile :
            for stk in stk_list :
                res = self.reconcile_opt_spots(stk, sd, ed)
                ofile.write(res)

    # Perform reconciliation for a single stock. If we cannot get the
    # EOD data, return N/A. Otherwise, return, for each stock, the
    # name, the start and end date between which spot data is
    # available, the start and end dates between which there is eod
    # data, and then the mse and percentage of coverage
    def reconcile_opt_spots(self, stk, sd, ed) :
        q         = "select dt, spot from opt_spots where stk='{0:s}' {1:s}".\
                    format(stk, db_sql_timeframe(sd, ed, True))
        spot_df   = pd.read_sql(q, db_get_cnx())
        spot_df.set_index('dt', inplace=True)
        s_spot    = str(spot_df.index[0])
        e_spot    = str(spot_df.index[-1])
        try :
            ts    = StxTS(stk, sd, ed, self.eod_tbl, self.split_tbl)
        except :
            print(sys.exc_info())
            return '{0:s},{1:s},{2:s},N/A,N/A,N/A,N/A,N/A\n'.\
                format(stk, s_spot, e_spot)
        df        = ts.df.join(spot_df)
        df['r']   = df['spot'] / df['c']
        for x in range(1, 4) :
            df['r{0:d}'.format(x)]  = df['r'].shift(-x)
            df['r_{0:d}'.format(x)] = df['r'].shift(x)            
        df['c1']  = df['c'].shift(-1)
        df['s1']  = df['spot'].shift(-1)
        df[['r', 'r1', 'r2', 'r3', 'r_1', 'r_2', 'r_3', 'c1', 's1']].\
            fillna(method='bfill', inplace=True)
        df['rr']  = df['r1']/df['r']
        df_f1     = df[(abs(df['rr'] - 1) > 0.05) & \
                       (round(df['r_1'] - df['r'], 2) == 0) & \
                       (round(df['r_2'] - df['r'], 2) == 0) & \
                       (round(df['r_3'] - df['r'], 2) == 0) & \
                       (round(df['r2']  - df['r1'], 2) == 0) & \
                       (round(df['r3']  - df['r1'], 2) == 0) & \
                       (df['c'] > 1.0)]
        s_df      = str(df.index[0].date())
        e_df      = str(df.index[-1].date())        
        if len(df_f1) > 0 :
            with open('{0:s}/split_recon_{1:s}.csv'.\
                      format(self.in_dir, self.eod_tbl), 'a') as ofile :
                splits   = {}
                for r in df_f1.iterrows():
                    splits[str(r[0].date())] = '{0:.4f},{1:.2f},{2:.2f},'\
                                               '{3:.2f},{4:.2f}'.\
                                               format(r[1]['rr'], r[1]['c'],
                                                      r[1]['spot'], r[1]['c1'],
                                                      r[1]['s1'])
                print('Found {0:d} splits for {1:s}'.format(len(splits), stk))
                dates    = list(splits.keys())
                dates.sort()
                for dt in dates :
                    ofile.write('{0:s},{1:s},{2:s}\n'.format(stk, dt,
                                                             splits[dt]))
        cov, acc  = self.quality(df, df_f1, ts, spot_df)
        return '{0:s},{1:s},{2:s},{3:s},{4:s},{5:d},{6:.2f},{7:.4f}\n'.\
            format(stk, s_spot, e_spot, s_df, e_df, len(df_f1), cov, acc)


    # Function that calculates the coverage and MSE between the spot
    # and eod prices
    def quality(self, df, df_f1, ts, spot_df) :
        s_spot     = str(spot_df.index[0])
        e_spot     = str(spot_df.index[-1])
        spot_days  = num_busdays(s_spot, e_spot)
        s_ts       = str(ts.df.index[0].date())
        e_ts       = str(ts.df.index[-1].date())
        if s_ts < s_spot :
            s_ts   = s_spot
        if e_ts > e_spot :
            e_ts   = e_spot
        ts_days    = num_busdays(s_ts, e_ts)
        coverage   = round(100.0 * ts_days / spot_days, 2)
        # apply the split adjustments
        ts.splits.clear()
        for r in df_f1.iterrows():
            ts.splits[r[0]] = r[1]['rr']
        ts.adjust_splits_date_range(0, len(ts.df) - 1, inv = 1)
        df.drop(['c'], inplace = True, axis = 1)
        df         = df.join(ts.df[['c']])
        # calculate statistics: coverage and mean square error
        df['sqrt'] = pow(1 - df['spot']/df['c'], 2)
        accuracy   = pow(df['sqrt'].sum() / min(len(df['sqrt']), len(spot_df)),
                         0.5)
        return coverage, accuracy

    def cleanup(self) :
        db_write_cmd('drop table `{0:s}`'.format(self.eod_tbl))
        db_write_cmd('drop table `{0:s}`'.format(self.split_tbl))
        
    def cleanup_data_folder(self) :
        if os.path.exists(self.in_dir) :
            rmtree('{0:s}'.format(self.in_dir))

        
if __name__ == '__main__' :
    seod = StxEOD('c:/goldendawn/bkp', 'eod1', 'split1')
    seod.load_my_files('XTR')
