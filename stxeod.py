import csv
from datetime import datetime
from math import trunc
import os
import pandas as pd
from shutil import copyfile, rmtree
import stxcal
import stxdb
from stxts import StxTS
import sys


class StxEOD:

    sh_dir = 'C:/goldendawn/stockhistory'
    upload_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    ed_dir = 'C:/goldendawn/EODData'
    eod_name = '{0:s}/eod_upload.txt'.format(upload_dir)
    sql_create_eod = 'CREATE TABLE `{0:s}` ('\
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
                       '`implied` tinyint DEFAULT 0,'\
                       'PRIMARY KEY (`stk`,`dt`)'\
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_recon = 'CREATE TABLE `{0:s}` ('\
                       '`stk` varchar(8) NOT NULL,'\
                       '`recon_name` varchar(16) NOT NULL,'\
                       '`recon_interval` char(18) NOT NULL,'\
                       '`s_spot` char(10) DEFAULT NULL,'\
                       '`e_spot` char(10) DEFAULT NULL,'\
                       '`sdf` char(10) DEFAULT NULL,'\
                       '`edf` char(10) DEFAULT NULL,'\
                       '`splits` smallint DEFAULT NULL,'\
                       '`coverage` float DEFAULT NULL,'\
                       '`mse` float DEFAULT NULL,'\
                       '`status` tinyint DEFAULT 0,'\
                       'PRIMARY KEY (`stk`,`recon_name`,`recon_interval`)'\
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    status_none = 0
    status_ok = 1
    status_ko = 2

    def __init__(self, in_dir, eod_tbl, split_tbl, recon_tbl,
                 extension='.txt'):
        self.in_dir = in_dir
        self.eod_tbl = eod_tbl
        self.rec_name = eod_tbl
        self.split_tbl = split_tbl
        self.recon_tbl = recon_tbl
        self.extension = extension
        stxdb.db_create_missing_table(eod_tbl, self.sql_create_eod)
        # print('EOD DB table: {0:s}'.format(eod_tbl))
        stxdb.db_create_missing_table(split_tbl, self.sql_create_split)
        # print('Split DB table: {0:s}'.format(split_tbl))
        stxdb.db_create_missing_table(recon_tbl, self.sql_create_recon)

    # Load my historical data.  Load each stock and accumulate splits.
    # Upload splits at the end.
    def load_my_files(self, stx=''):
        split_fname = '{0:s}/my_splits.txt'.format(self.upload_dir)
        try:
            os.remove(split_fname)
            print('Removed {0:s}'.format(split_fname))
        except:
            pass
        if stx == '':
            lst = [f for f in os.listdir(self.in_dir)
                   if f.endswith(self.extension)]
        else:
            lst = []
            stk_list = stx.split(',')
            for stk in stk_list:
                lst.append('{0:s}{1:s}'.format(stk.strip(), self.extension))
        num_stx = len(lst)
        print('Loading data for {0:d} stocks'.format(num_stx))
        ixx = 0
        for fname in lst:
            self.load_my_stk(fname, split_fname)
            ixx += 1
            if ixx % 500 == 0 or ixx == num_stx:
                print('Uploaded {0:5d}/{1:5d} stocks'.format(ixx, num_stx))
        try:
            stxdb.db_upload_file(split_fname, self.split_tbl, 2)
            print('Successfully uploaded the splits in the DB')
        except:
            e = sys.exc_info()[1]
            print('Failed to upload the splits from file {0:s}, error {1:s}'.
                  format(split_fname, str(e)))

    # Upload each stock.  Split lines are prefixed with '*'.  Upload
    # stock data separately and then accumulate each stock.
    def load_my_stk(self, short_fname, split_fname):
        fname = '{0:s}/{1:s}'.format(self.in_dir, short_fname)
        stk = short_fname[:-4].upper()
        try:
            with open(fname, 'r') as ifile:
                lines = ifile.readlines()
        except:
            e = sys.exc_info()[1]
            print('Failed to read {0:s}, error {1:s}'.
                  format(short_fname, str(e)))
            return
        eods, splits = 0, 0
        try:
            with open(self.eod_name, 'w') as eod:
                for line in lines:
                    start = 1 if line.startswith('*') else 0
                    toks = line[start:].strip().split(' ')
                    o, h, l, c, v = float(toks[1]), float(toks[2]), \
                        float(toks[3]), float(toks[4]), int(toks[5])
                    if o > 0.05 and h > 0.05 and l > 0.05 and c > 0.05 and \
                       v > 0:
                        eod.write('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'
                                  '{5:.2f}\t{6:d}\n'.
                                  format(stk, toks[0], o, h, l, c, v))
                    eods += 1
                    if start == 1:
                        splits += 1
                        with open(split_fname, 'a') as split_file:
                            split_file.write('{0:s}\t{1:s}\t{2:f}\t0\n'.format
                                             (stk, toks[0], float(toks[6])))
        except:
            e = sys.exc_info()[1]
            print('Failed to parse {0:s}, error {1:s}'.
                  format(short_fname, str(e)))
            return
        try:
            stxdb.db_upload_file(self.eod_name, self.eod_tbl, 2)
            # print('{0:s}: uploaded {1:d} eods and {2:d} splits'.\
            #       format(stk, eods, splits))
        except:
            e = sys.exc_info()[1]
            print('Failed to upload {0:s}: {1:s}'.format(short_fname, str(e)))

    # Parse delta neutral stock history.  First, separate each yearly
    # data into stock files, and upload each stock file.  Then upload
    # all the splits into the database.  We will worry about
    # missing/wrong splits and volume adjustments later.
    def load_deltaneutral_files(self, stks=''):
        if not os.path.exists(self.in_dir):
            os.makedirs(self.in_dir)
        for yr in range(2001, 2017):
            fname = '{0:s}/stockhistory_{1:d}.csv'.format(self.sh_dir, yr)
            stx = {}
            stk_list = [] if stks == '' else stks.split(',')
            with open(fname) as csvfile:
                frdr = csv.reader(csvfile)
                for row in frdr:
                    stk = row[0].strip()
                    if (stk_list and stk not in stk_list) or ('/' in stk) or \
                       ('*' in stk) or (stk in ['AUX', 'PRN']):
                        continue
                    data = stx.get(stk, [])
                    o, h, l, c, v = float(row[2]), float(row[3]), \
                        float(row[4]), float(row[5]), int(row[7])
                    if o > 0.05 and h > 0.05 and l > 0.05 and c > 0.05 and \
                       v > 0:
                        data.append('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'
                                    '{5:.2f}\t{6:d}\n'.
                                    format(stk, str(datetime.strptime
                                                    (row[1], '%m/%d/%Y').
                                                    date()),
                                           o, h, l, c, v))
                    stx[stk] = data
            print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
            for stk, recs in stx.items():
                with open('{0:s}/{1:s}.txt'.format(self.in_dir, stk), 'a') \
                     as ofile:
                    for rec in recs:
                        ofile.write(rec)
        lst = [f for f in os.listdir(self.in_dir)
               if f.endswith(self.extension)]
        for fname in lst:
            copyfile('{0:s}/{1:s}'.format(self.in_dir, fname), self.eod_name)
            try:
                stxdb.db_upload_file(self.eod_name, self.eod_tbl, 2)
                print('{0:s}: uploaded eods'.format(stk))
            except:
                e = sys.exc_info()[1]
                print('Upload failed {0:s}, error {1:s}'.format(stk, str(e)))
        self.load_deltaneutral_splits(stk_list)

    # Load the delta neutral splits into the database
    def load_deltaneutral_splits(self, stk_list):
        # this part uploads the splits
        in_fname = '{0:s}/stocksplits.csv'.format(self.sh_dir)
        out_fname = '{0:s}/stocksplits.txt'.format(self.upload_dir)
        with open(in_fname, 'r') as csvfile:
            with open(out_fname, 'w') as dbfile:
                frdr = csv.reader(csvfile)
                for row in frdr:
                    stk = row[0].strip()
                    if stk_list and stk not in stk_list:
                        continue
                    dt = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                    dbfile.write('{0:s}\t{1:s}\t{2:f}\t0\n'.
                                 format(stk, stxcal.prev_busday(dt),
                                        1 / float(row[2])))
        try:
            stxdb.db_upload_file(out_fname, self.split_tbl, 2)
            print('Uploaded delta neutral splits')
        except:
            e = sys.exc_info()[1]
            print('Failed to upload splits, error {0:s}'.format(str(e)))

    # Load data from EODData data source in the database. There is a
    # daily file for each exchange (Amex, Nasdaq, NYSE). Use an
    # overlap of 5 days with the previous reconciliation interval
    # (covering up to 2012-12-31)
    def load_eoddata_files(self, sd='2013-01-01', ed='2012-12-21'):
        dt = stxcal.move_busdays(sd, -25)
        fnames = [self.in_dir + '/AMEX_{0:s}.txt',
                  self.in_dir + '/NASDAQ_{0:s}.txt',
                  self.in_dir + '/NYSE_{0:s}.txt']
        while dt <= ed:
            dtc = dt.replace('-', '')
            with open(self.eod_name, 'w') as ofile:
                for fname in fnames:
                    self.load_eoddata_file(ofile, fname.format(dtc), dt, dtc)
            try:
                stxdb.db_upload_file(self.eod_name, self.eod_tbl, 2)
                print('{0:s}: uploaded eods'.format(dt))
            except:
                e = sys.exc_info()[1]
                print('Failed to upload {0:s}, error {1:s}'.format(dt, str(e)))
            dt = stxcal.next_busday(dt)

    # Load data from a single EODData file in the database Perform
    # some quality checks on the data: do not upload days where volume
    # is 0, or where the open/close are outside the [low, high] range.
    def load_eoddata_file(self, ofile, ifname, dt, dtc):
        with open(ifname, 'r') as ifile:
            lines = ifile.readlines()
            for line in lines[1:]:
                tokens = line.replace(dtc, dt).strip().split(',')
                o = float(tokens[2])
                h = float(tokens[3])
                l = float(tokens[4])
                c = float(tokens[5])
                v = int(tokens[6])
                if v == 0 or o < l or o > h or c < l or c > h or \
                   len(tokens[0]) > 6:
                    continue
                ofile.write('{0:s}\n'.format('\t'.join(tokens)))

    # Perform reconciliation with the option spots.  First get all the
    # underliers for which we have spot prices within a given
    # interval.  Then, reconcile for each underlier
    def reconcile_spots(self, sd=None, ed=None, stx='', dbg=False):
        if stx == '':
            res = stxdb.db_read_cmd('select distinct stk from opt_spots {0:s}'.
                                    format(stxdb.db_sql_timeframe(sd, ed,
                                                                  False)))
            stk_list = [stk[0] for stk in res]
        else:
            stk_list = stx.split(',')
        if sd is None:
            sd = '2001-01-01'
        if ed is None:
            ed = datetime.now().strftime('%Y-%m-%d')
        rec_interval = '{0:s}_{1:s}'.format(
            datetime.strptime(sd, '%Y-%m-%d').strftime('%Y%m%d'),
            datetime.strptime(ed, '%Y-%m-%d').strftime('%Y%m%d'))
        num_stx = len(stk_list)
        print('Reconciling {0:d} stocks in interval {1:s}'.
              format(num_stx, rec_interval))
        num = 0
        for stk in stk_list:
            try:
                rec_res = stxdb.db_read_cmd("select * from {0:s} where "
                                            "stk='{1:s}' and "
                                            "recon_name='{2:s}' and "
                                            "recon_interval='{3:s}'".
                                            format(self.recon_tbl, stk,
                                                   self.rec_name,
                                                   rec_interval))
                if not rec_res:
                    res = self.reconcile_opt_spots(stk, sd, ed, dbg)
                    if not dbg:
                        stxdb.db_write_cmd("insert into {0:s} values "
                                           "('{1:s}','{2:s}','{3:s}',{4:s},0)".
                                           format(self.recon_tbl, stk,
                                                  self.rec_name,
                                                  rec_interval, res))
            except:
                e = sys.exc_info()[1]
                print('Failed to reconcile {0:s}, error {1:s}'.
                      format(stk, str(e)))
            finally:
                num += 1
                if num % 500 == 0 or num == num_stx:
                    print('Reconciled {0:4d} out of {1:4d} stocks'.
                          format(num, num_stx))

    # Perform reconciliation for a single stock. If we cannot get the
    # EOD data, return N/A. Otherwise, return, for each stock, the
    # name, the start and end date between which spot data is
    # available, the start and end dates between which there is eod
    # data, and then the mse and percentage of coverage
    def reconcile_opt_spots(self, stk, sd, ed, dbg=False):
        q = "select dt, spot from opt_spots where stk='{0:s}' {1:s}".\
            format(stk, stxdb.db_sql_timeframe(sd, ed, True))
        spot_df = pd.read_sql(q, stxdb.db_get_cnx())
        spot_df.set_index('dt', inplace=True)
        s_spot, e_spot = str(spot_df.index[0]), str(spot_df.index[-1])
        df, ts = self.build_df_ts(spot_df, stk, sd, ed)
        if df is None:
            return "'{0:s}','{1:s}'{2:s}".format(s_spot, e_spot,
                                                 ",'','',0,0,0")
        self.calc_implied_splits(df, stk, sd, ed)
        df, cov, mse = self.quality(df, ts, spot_df, dbg)
        if mse > 0.02:
            num, df, ts = self.autocorrect(df, spot_df, ts.stk, sd, ed)
            if df is None:
                return "'{0:s}','{1:s}'{2:s}".format(s_spot, e_spot,
                                                     ",'','',0,0,0")
            if num > 0:
                df, cov, mse = self.quality(df, ts, spot_df, dbg)
                num, df, ts = self.autocorrect(df, spot_df, ts.stk, sd, ed)
            df, cov, mse = self.quality(df, ts, spot_df, dbg)
        s_df, e_df = str(df.index[0].date()), str(df.index[-1].date())
        res = stxdb.db_read_cmd("select * from {0:s} where stk = '{1:s}'"
                                " and dt between '{2:s}' and '{3:s}' and "
                                "implied = 1".format(self.split_tbl, stk, sd,
                                                     ed))
        if dbg:
            print('{0:s}: {1:s} {2:s} {3:s} {4:s} {5:s} {6:d} {7:.2f} {8:.4f}'.
                  format(self.eod_tbl, stk, s_spot, e_spot, s_df, e_df,
                         len(res), cov, mse))
            return df
        return "'{0:s}','{1:s}','{2:s}','{3:s}',{4:d},{5:.2f},{6:.4f}".\
            format(s_spot, e_spot, s_df, e_df, len(res), cov, mse)

    # This function detects differences in the ratio between the spot
    # prices (from opt_spots table) and the closing prices from the
    # EOD table.  When the ratio changes, we assume there is a split.
    # To avoid defining a split for every spike ina ratio (which can
    # be due, for instancem to one day with wrong data), we are
    # looking at the ratio to stay the same over three days before the
    # day where the ratio change and 2 days after.
    def calc_implied_splits(self, df, stk, sd, ed):
        stxdb.db_write_cmd("delete from {0:s} where stk = '{1:s}' and "
                           "dt between '{2:s}' and '{3:s}' and implied = 1".
                           format(self.split_tbl, stk, sd, ed))
        df['r'] = df['spot'] / df['c']
        for i in [x for x in range(-3, 4) if x != 0]:
            df['r{0:d}'.format(i)] = df['r'].shift(-i)
        df['rr'] = df['r1'] / df['r']
        df_f1 = df[(abs(df['rr'] - 1) > 0.05) & (df['c'] > 1.0) &
                   (round(df['r-1'] - df['r'], 2) == 0) &
                   (round(df['r-2'] - df['r'], 2) == 0) &
                   (round(df['r-3'] - df['r'], 2) == 0) &
                   (round(df['r2'] - df['r1'], 2) == 0) &
                   (round(df['r3'] - df['r1'], 2) == 0) &
                   (df['v'] > 0)]
        for r in df_f1.iterrows():
            stxdb.db_write_cmd("insert into {0:s} values ('{1:s}', '{2:s}', "
                               "{3:.4f}, 1)".format(self.split_tbl, stk,
                                                    str(r[0].date()),
                                                    r[1]['rr']))

    # Function that calculates the coverage and MSE between the spot
    # and eod prices
    def quality(self, df, ts, spot_df, dbg=False):
        sfx = '' if 'rr' in df.columns else '_adj'
        s_spot, e_spot = str(spot_df.index[0]), str(spot_df.index[-1])
        spot_days = stxcal.num_busdays(s_spot, e_spot)
        s_ts, e_ts = str(ts.df.index[0].date()), str(ts.df.index[-1].date())
        if s_ts < s_spot:
            s_ts = s_spot
        if e_ts > e_spot:
            e_ts = e_spot
        ts_days = stxcal.num_busdays(s_ts, e_ts) if e_ts > s_ts else 0
        coverage = round(100.0 * ts_days / spot_days, 2)
        # apply the split adjustments
        ts.splits.clear()
        splits = stxdb.db_read_cmd("select dt, ratio from {0:s} where stk = "
                                   "'{1:s}' and implied = 1".
                                   format(self.split_tbl, ts.stk))
        for s in splits:
            ts.splits[pd.to_datetime(stxcal.next_busday(s[0]))] = float(s[1])
        ts.adjust_splits_date_range(0, len(ts.df) - 1, inv=1)
        df.drop(['c'], inplace=True, axis=1)
        df = df.join(ts.df[['c']])

        # calculate statistics: coverage and mean square error
        def msefun(x):
            return 0 if x['spot'] == trunc(x['c']) or x['v'] == 0 \
                else pow(1 - x['c']/x['spot'], 2)
        df['mse'] = df.apply(msefun, axis=1)
        mse = pow(df['mse'].sum()/min(len(df['mse']), len(spot_df)), 0.5)
        if dbg:
            df.to_csv('c:/goldendawn/dbg/{0:s}_{1:s}{2:s}_recon.csv'.
                      format(ts.stk, self.eod_tbl, sfx))
        return df, coverage, mse

    def cleanup(self):
        # drop the EOD and splits tables
        stxdb.db_write_cmd('drop table `{0:s}`'.format(self.eod_tbl))
        stxdb.db_write_cmd('drop table `{0:s}`'.format(self.split_tbl))
        # if reconciliation table exists, delete all the records that
        # correspond to the self.recon_name variable
        res = stxdb.db_read_cmd("show tables like '{0:s}'".
                                format(self.recon_tbl))
        if res:
            stxdb.db_write_cmd("delete from {0:s} where recon_name='{1:s}'".
                               format(self.recon_tbl, self.rec_name))

    def cleanup_data_folder(self):
        if os.path.exists(self.in_dir):
            rmtree('{0:s}'.format(self.in_dir))

    # This function detects any errors in the reconciled data
    # (obtained after applying the calc_implied_splits function).
    # Errors can be due either to a spike in the spot or the closing
    # price, to mismatches over a large period of time (mostly due I
    # think, to symbol changes), and errors that have the same value
    # over a large time interval, and point to a missed split/large
    # dividend.  The errors are reduced in two ways: by applying a
    # split for large sequences of errors that have the same error
    # value, and by deleting data in an interval where the values of
    # the errors are different each day.  The removal of records, and
    # the application of new implied splits (implied database table
    # field is set to 1) is done directly in the database.
    def autocorrect(self, df, spot_df, stk, sd, ed):
        df_err = df.query('mse>0.01')
        start = len(df_err) - 1
        adj_factor = 1
        wrong_recs = []
        split_adjs = []
        while start >= 0:
            mse0 = df_err.ix[start].mse
            strikes = 0
            end, ixx = start, start - 1
            while ixx >= 0 and strikes < 3:
                rec = df_err.ix[ixx]
                if rec.spot == trunc(rec.spot):
                    pass
                elif abs(1 - rec.mse/mse0) < 0.01:
                    strikes = 0
                    end = ixx
                else:
                    strikes += 1
                ixx -= 1
            if start - end < 15:
                wrong_recs.append(df_err.index[start])
                start -= 1
            else:
                rec = df_err.ix[start]
                ratio = rec.c / rec.spot
                if abs(1 - ratio * adj_factor) > 0.01:
                    split_adjs.append(list([df_err.index[start],
                                            ratio * adj_factor]))
                    adj_factor = 1 / ratio
                start = end - 1
        if len(wrong_recs) > 0:
            sql = "delete from {0:s} where stk='{1:s}' and dt in (".\
                format(self.eod_tbl, stk)
            for wrong_rec in wrong_recs:
                sql += "'{0:s}',".format(str(wrong_rec.date()))
            sql = sql[:-1] + ')'
            # print('sql = {0:s}'.format(sql))
            stxdb.db_write_cmd(sql)
            df, ts = self.build_df_ts(spot_df, stk, sd, ed)
            self.calc_implied_splits(df, stk, sd, ed)
        else:
            # print('split_adjs = {0:s}'.format(str(split_adjs)))
            for s in split_adjs:
                stxdb.db_write_cmd("insert into {0:s} values ('{1:s}', '{2:s}'"
                                   ", {3:.4f}, 1)".format(self.split_tbl, stk,
                                                          str(s[0].date()),
                                                          s[1]))
        df, ts = self.build_df_ts(spot_df, stk, sd, ed)
        return len(wrong_recs), df, ts

    # Utility function that builds a stock time series and a data
    # frame that will include the reconciliation calculations.
    def build_df_ts(self, spot_df, stk, sd, ed):
        try:
            ts = StxTS(stk, sd, ed, self.eod_tbl, self.split_tbl)
        except:
            return None, None
        df = ts.df.join(spot_df)
        for i in [x for x in range(-2, 3) if x != 0]:
            df['s{0:d}'.format(i)] = df['spot'].shift(-i)
        return df, ts

    # Once the reconciliation is performed, select all the
    # reconciilations performed on a specific set of data for which
    # the coverage is above a pre-defined threshold and the mean
    # square error is below a pre-defined threshold.  For each stock
    # that satisfies these criteria, retrieve all the implied splits,
    # apply reverse adjustments for each implied split, and upload the
    # data in the final EOD table.
    def upload_eod(self, eod_table, split_table, stx='', sd=None, ed=None,
                   max_mse=0.02, min_coverage=80):
        stxdb.db_create_missing_table(eod_table, self.sql_create_eod)
        stxdb.db_create_missing_table(split_table, self.sql_create_split)
        if sd is None:
            sd = '2001-01-01'
        if ed is None:
            ed = datetime.now().strftime('%Y-%m-%d')
        rec_interval = '{0:s}_{1:s}'.format(sd.replace('-', ''),
                                            ed.replace('-', ''))
        sql = "select stk from {0:s} where recon_name='{1:s}' and "\
              "recon_interval='{2:s}' and mse<={3:f} and coverage>={4:f}".\
              format(self.recon_tbl, self.rec_name, rec_interval, max_mse,
                     min_coverage)
        if stx != '':
            sql = "{0:s} and stk in ('{1:s}')".\
                  format(sql, stx.replace(',', "','"))
        res = stxdb.db_read_cmd(sql)
        stk_list = [x[0] for x in res]
        num_stx = len(stk_list)
        print('Found {0:d} reconciled stocks from {1:s} EOD table'.
              format(num_stx, self.eod_tbl))
        num = 0
        if sd == '2001-01-01':
            sd = '1962-01-01'
        for stk in stk_list:
            res = stxdb.db_read_cmd("select * from {0:s} where "
                                    "stk='{1:s}' and recon_interval='{2:s}' "
                                    "and status={3:d}".
                                    format(self.recon_tbl, stk, rec_interval,
                                           self.status_ok))
            if not res:
                try:
                    self.upload_stk(eod_table, split_table, stk, sd, ed,
                                    rec_interval)
                    num += 1
                    if num % 500 == 0:
                        print('Uploaded {0:4d} stocks from {1:s}'.
                              format(num, self.eod_tbl))
                except:
                    e = sys.exc_info()[1]
                    print('Failed to EOD upload stock {0:s}, error {1:s}'.
                          format(stk, str(e)))
        print('{0:s} - uploaded {1:d} stocks'.format(self.rec_name, num))

    def upload_stk(self, eod_table, split_table, stk, sd, ed, rec_interval):
        ts = StxTS(stk, sd, ed, self.eod_tbl, self.split_tbl)
        ts.splits.clear()
        impl_splits = stxdb.db_read_cmd("select dt, ratio from {0:s} where "
                                        "stk='{1:s}' and implied = 1".
                                        format(self.split_tbl, stk))
        # for implied splits, need to perform a reverse adjustment
        # for the adjustment to work, move the split date to next business day
        for s in impl_splits:
            ts.splits[pd.to_datetime(stxcal.next_busday(s[0]))] = float(s[1])
        ts.adjust_splits_date_range(0, len(ts.df) - 1, inv=1)
        with open(self.eod_name, 'w') as ofile:
            for idx, row in ts.df.iterrows():
                if row['v'] > 0:
                    ofile.write('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t'
                                '{4:.2f}\t{5:.2f}\t{6:.0f}\n'.
                                format(stk, str(idx.date()), row['o'],
                                       row['h'], row['l'], row['c'],
                                       row['v']))
        # upload the eod data in the database
        stxdb.db_upload_file(self.eod_name, eod_table, 2)
        # finally, upload all the splits into the final split table
        stxdb.db_write_cmd('insert into {0:s} (stk,dt,ratio,implied) '
                           'select stk, dt, ratio, implied from {1:s} '
                           "where stk='{2:s}'".
                           format(split_table, self.split_tbl, stk))
        # update the reconciilation table
        stxdb.db_write_cmd("update {0:s} set status={1:d} where "
                           "stk='{2:s}' and recon_interval='{3:s}' and "
                           "recon_name='{4:s}'".
                           format(self.recon_tbl, self.status_ok, stk,
                                  rec_interval, self.rec_name))

    # Capture all the large price changes without a volume that is
    # larger than the average, and check that these are not missed
    # splits.  Also, check split data coming from several sources
    # (mypivots, deltaneutral, my DB, EODData), make sure that these
    # splits are reflected in the EOD data, and, if they are, make
    # sure that they are added to the splits database.
    def split_reconciliation(self, stx, sd, ed, split_tbls):
        if stx == '':
            sql = "select distinct stk from {0:s} where dt between '{1:s}'"\
                  " and '{2:s}'".format(self.eod_tbl, sd, ed)
            res = stxdb.db_read_cmd(sql)
            stk_list = [x[0] for x in res]
        else:
            stk_list = stx.split(',')
        num_stx = len(stk_list)
        print('Reconciling big changes for {0:d} stocks from {1:s}'.
              format(num_stx, self.eod_tbl))
        num = 0
        for stk in stk_list:
            try:
                self.reconcile_big_changes(stk, sd, ed, split_tbls)
            except:
                e = sys.exc_info()[1]
                print('Failed to reconcile {0:s}, error {1:s}'.
                      format(stk, str(e)))
            finally:
                num += 1
                if num % 500 == 0 or num == num_stx:
                    print('Reconciled big changes for {0:4d} out of {1:4d}'
                          ' stocks'.format(num, num_stx))

    def reconcile_big_changes(self, stk, sd, ed, split_tbls):
        ts = StxTS(stk, sd, ed, self.eod_tbl, self.split_tbl)
        ts.df['chg'] = ts.df['c'].pct_change().abs()
        ts.df['id_chg'] = (ts.df['h'] - ts.df['l']) / ts.df['c']
        ts.df['avg_chg20'] = ts.df['chg'].rolling(20).mean()
        ts.df['avg_v20'] = ts.df['v'].shift().rolling(20).mean()
        ts.df['avg_v50'] = ts.df['v'].shift().rolling(50).mean()
        df_review = ts.df.query('chg>=0.15 and chg>2*avg_chg20 and '
                                'v<(0.5+10*chg)*avg_v50 and avg_v20>1000 and '
                                'id_chg<0.6*chg and c>3 and '
                                '(chg-id_chg)>2*avg_chg20 and (c>8 or chg>1)')
        all_splits = {}
        for split_table in split_tbls:
            res = stxdb.db_read_cmd('select dt, ratio from {0:s} where '
                                    "stk='{1:s}' and implied=0".
                                    format(split_table, stk))
            all_splits[split_table] = {stxcal.next_busday(x[0]): x[1]
                                       for x in res}
        for idx, row in df_review.iterrows():
            if idx in ts.splits:
                continue
            db_splits = ''
            big_chg_dt = str(idx.date())
            for tbl_name, splits in all_splits.items():
                if big_chg_dt not in splits:
                    continue
                db_splits = '{0:s}{1:9s} {2:7.4f} '.\
                            format(db_splits, tbl_name, splits[big_chg_dt])
                sql = "insert into {0:s} values ('{1:s}','{2:s}',"\
                      "{3:.4f},0)".format(self.split_tbl, stk,
                                          stxcal.prev_busday(big_chg_dt),
                                          splits[big_chg_dt])
                try:
                    stxdb.db_write_cmd(sql)
                except:
                    pass
            ofname = 'c:/goldendawn/big_change_recon_{0:s}_{1:s}.txt'.\
                     format(sd.replace('-', ''), ed.replace('-', ''))
            with open(ofname, 'a') as ofile:
                ofile.write('{0:5s} {1:s} {2:6.2f} {3:6.2f} {4:6.2f} {5:6.2f} '
                            '{6:9.0f} {7:5.3f} {8:5.3f} {9:9.0f} {10:s}\n'.
                            format(stk, big_chg_dt, row['o'], row['h'],
                                   row['l'], row['c'], row['v'], row['chg'],
                                   row['avg_chg20'], row['avg_v50'],
                                   db_splits))

    def correct_split_adjustment_error(self, fname):
        with open(fname, 'r') as ifile:
            lines = ifile.readlines()
            for line in lines:
                tokens = line.split()
                if len(tokens) > 10:
                    stk, dt = tokens[0], tokens[1]
                    sql = "update {0:s} set dt='{1:s}' where "\
                        "stk='{2:s}' and dt='{3:s}'".\
                        format(self.split_tbl, stxcal.prev_busday(dt), stk, dt)
                    stxdb.db_write_cmd(sql)

    def volume_check(self):
        # ts.set_day('2006-12-18')
        # ts.next_day()
        # df1 = ts.df[['v']].ix[ts.pos-20:ts.pos]
        # av1 = round(df1[['v']].mean()['v'], 0)
        # df2 = ts.df[['v']].ix[ts.pos+1:ts.pos+21]
        # av2 = round(df2[['v']].mean()['v'], 0)
        # av2/av1
        pass


if __name__ == '__main__':
    # ed_eod = StxEOD('c:/goldendawn/EODData', 'ed_eod', 'ed_split')
    # ed_eod.load_eoddata_files()
    s_date = '2001-01-01'
    e_date = '2012-12-31'
    my_eod = StxEOD('c:/goldendawn/bkp', 'my_eod', 'my_split',
                    'reconciliation')
    # my_eod.load_my_files()
    dn_eod = StxEOD('c:/goldendawn/dn_data', 'dn_eod', 'dn_split',
                    'reconciliation')
    # dn_eod.load_deltaneutral_files()
    # my_eod.reconcile_spots(s_date, e_date)
    # dn_eod.reconcile_spots(s_date, e_date)
    # To debug a reconciliation:
    # my_eod.reconcile_opt_spots('AEOS', '2002-02-01', '2012-12-31', True)
    # my_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date)
    my_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    dn_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    eod = StxEOD('', 'eod', 'split', 'reconciliation')
    eod.split_reconciliation('', s_date, e_date, ['splits', 'my_split',
                                                  'dn_split'])
