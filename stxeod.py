import csv
from datetime import datetime
from io import BytesIO
# import json
import logging
from math import trunc
import numpy as np
import os
import pandas as pd
# import requests
from shutil import rmtree
import stxcal
import stxdb
from stxts import StxTS


class StxEOD:
    data_dir = os.getenv('DATA_DIR')
    sh_dir = '{0:s}/stockhistory_2017'.format(data_dir)
    upload_dir = '/tmp'
    ed_dir = '{0:s}/EODData'.format(data_dir)
    dload_dir = '{0:s}/Downloads'.format(data_dir)
    eod_name = '{0:s}/eod_upload.txt'.format(upload_dir)
    yhoo_url = 'http://chart.finance.yahoo.com/table.csv?s={0:s}&a={1:d}&'\
        'b={2:d}&c={3:d}&d={4:d}&e={5:d}&f={6:d}&g={7:s}&ignore=.csv'
    sql_create_recon = 'CREATE TABLE {0:s} ('\
                       'stk varchar(8) NOT NULL,'\
                       'recon_name varchar(16) NOT NULL,'\
                       'recon_interval varchar(18) NOT NULL,'\
                       's_spot char(10) DEFAULT NULL,'\
                       'e_spot char(10) DEFAULT NULL,'\
                       'sdf char(10) DEFAULT NULL,'\
                       'edf char(10) DEFAULT NULL,'\
                       'splits smallint DEFAULT NULL,'\
                       'coverage float DEFAULT NULL,'\
                       'mse float DEFAULT NULL,'\
                       'status smallint DEFAULT 0,'\
                       'PRIMARY KEY (stk,recon_name,recon_interval)'\
                       ')'
    sql_show_tables = "SELECT table_name FROM information_schema.tables "\
                      "WHERE table_schema='public' AND table_name='{0:s}'"
    status_none = 0
    status_ok = 1
    status_ko = 2

    def __init__(self, in_dir, prefix, recon_tbl, extension='.txt'):
        self.in_dir = in_dir
        self.eod_tbl = 'eods' if prefix == '' else '{0:s}_eods'.format(prefix)
        self.rec_name = self.eod_tbl
        self.divi_tbl = 'dividends' if prefix == '' else \
                        '{0:s}_dividends'.format(prefix)
        self.recon_tbl = recon_tbl
        self.extension = extension
        stxdb.db_create_table_like('eods', self.eod_tbl)
        stxdb.db_create_table_like('dividends', self.divi_tbl)
        stxdb.db_create_missing_table(recon_tbl, self.sql_create_recon)
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir)

    def create_exchange(self):
        xchgs = stxdb.db_read_cmd("select * from exchanges where name='US'")
        if not xchgs:
            stxdb.db_write_cmd("insert into exchanges values('US')")
        db_stx = {x[0]: 0 for x in stxdb.db_read_cmd("select * from equities")}
        stx = {}
        return db_stx, stx

    # Load my historical data.  Load each stock and accumulate splits.
    # Upload splits at the end.
    def load_my_files(self, stx=''):
        split_fname = '{0:s}/my_splits.txt'.format(self.upload_dir)
        try:
            os.remove(split_fname)
            print('Removed {0:s}'.format(split_fname))
        except Exception as ex:
            print('Problem removing {0:s}: {1:s}'.format(split_fname, str(ex)))
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
        db_stx, stx_dct = self.create_exchange()
        ixx = 0
        for fname in lst:
            self.load_my_stk(fname, split_fname, db_stx, stx_dct)
            ixx += 1
            if ixx % 500 == 0 or ixx == num_stx:
                print('Uploaded {0:5d}/{1:5d} stocks'.format(ixx, num_stx))
        try:
            stxdb.db_upload_file(split_fname, self.divi_tbl, '\t')
            print('Successfully uploaded the splits in the DB')
        except Exception as ex:
            print('Failed to upload the splits from file {0:s}, error {1:s}'.
                  format(split_fname, str(ex)))

    # Upload each stock.  Split lines are prefixed with '*'.  Upload
    # stock data separately and then accumulate each stock.
    def load_my_stk(self, short_fname, split_fname, db_stx, stx_dct):
        fname = '{0:s}/{1:s}'.format(self.in_dir, short_fname)
        stk = short_fname[:-4].upper()
        if (stk not in db_stx) and (stk not in stx_dct):
            insert_stx = "INSERT INTO equities VALUES "\
                         "('{0:s}', '', 'US Stocks', 'US')".format(stk)
            stxdb.db_write_cmd(insert_stx)
            stx_dct[stk] = stk
        try:
            with open(fname, 'r') as ifile:
                lines = ifile.readlines()
        except Exception as ex:
            # e = sys.exc_info()[1]
            print('Failed to read {0:s}, error {1:s}'.
                  format(short_fname, str(ex)))
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
                                  '{5:.2f}\t{6:d}\t0\n'.
                                  format(stk, toks[0], o, h, l, c, v))
                    eods += 1
                    if start == 1:
                        splits += 1
                        with open(split_fname, 'a') as split_file:
                            split_file.write('{0:s}\t{1:s}\t{2:f}\t0\n'.format
                                             (stk, toks[0], float(toks[6])))
        except Exception as ex:
            print('Failed to parse {0:s}, error {1:s}'.
                  format(short_fname, str(ex)))
            return
        try:
            stxdb.db_upload_file(self.eod_name, self.eod_tbl, '\t')
            # print('{0:s}: uploaded {1:d} eods and {2:d} splits'.\
            #       format(stk, eods, splits))
        except Exception as ex:
            print('Failed to upload {0:s}: {1:s}'.format(short_fname, str(ex)))

    # Parse delta neutral stock history.  First, separate each yearly
    # data into stock files, and upload each stock file.  Then upload
    # all the splits into the database.  We will worry about
    # missing/wrong splits and volume adjustments later.
    def load_deltaneutral_files(self, stks=''):
        if os.path.exists(self.in_dir):
            rmtree(self.in_dir)
        os.makedirs(self.in_dir)
        for yr in range(2001, 2017):
            fname = '{0:s}/stockhistory_{1:d}.csv'.format(self.sh_dir, yr)
            stx = {}
            stk_list = [] if stks == '' else stks.split(',')
            with open(fname) as csvfile:
                db_stx, stx_dct = self.create_exchange()
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
                                    '{5:.2f}\t{6:d}\t0\n'.
                                    format(stk, str(datetime.strptime
                                                    (row[1], '%m/%d/%Y').
                                                    date()),
                                           o, h, l, c, v))
                    stx[stk] = data
            print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
            for stk, recs in stx.items():
                if stk not in db_stx:
                    insert_stx = "INSERT INTO equities VALUES "\
                                 "('{0:s}', '', 'US Stocks', 'US')".format(stk)
                    stxdb.db_write_cmd(insert_stx)
                with open('{0:s}/{1:s}.txt'.format(self.in_dir, stk), 'a') \
                        as ofile:
                    for rec in recs:
                        ofile.write(rec)
        lst = [f for f in os.listdir(self.in_dir)
               if f.endswith(self.extension)]
        for fname in lst:
            try:
                stxdb.db_upload_file(os.path.join(self.in_dir, fname),
                                     self.eod_tbl, '\t')
                print('{0:s}: uploaded eods'.format(stk))
            except Exception as ex:
                print('Upload failed {0:s}, error {1:s}'.format(stk, str(ex)))
        self.load_deltaneutral_splits(stk_list)

    # Load the delta neutral splits into the database
    def load_deltaneutral_splits(self, stk_list):
        # this part uploads the splits
        in_fname = '{0:s}/stocksplits_20170102.csv'.format(self.sh_dir)
        out_fname = '{0:s}/stocksplits.txt'.format(self.upload_dir)
        with open(in_fname, 'r') as csvfile:
            with open(out_fname, 'w') as dbfile:
                frdr = csv.reader(csvfile)
                for row in frdr:
                    stk = row[0].strip()
                    if stk_list and stk not in stk_list:
                        continue
                    dt = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                    sep = ' for ' if row[3].find(' for ') > 0 else ' to '
                    denom, num = row[3].split(sep)
                    num = float(num)
                    denom = float(denom)
                    dbfile.write('{0:s}\t{1:s}\t{2:8.4f}\t0\n'.
                                 format(stk, stxcal.prev_busday(dt),
                                        num / denom))
        try:
            stxdb.db_upload_file(out_fname, self.divi_tbl, '\t')
            print('Uploaded delta neutral splits')
        except Exception as ex:
            print('Failed to upload splits, error {0:s}'.format(str(ex)))

    # Load the delta neutral divis into the database
    def load_deltaneutral_divis(self, stk_list):
        # this part uploads the splits
        in_fname = '{0:s}/dividends.csv'.format(self.sh_dir)
        out_fname = '{0:s}/stockdivis.txt'.format(self.upload_dir)
        with open(in_fname, 'r') as csvfile:
            with open(out_fname, 'w') as dbfile:
                frdr = csv.reader(csvfile)
                for row in frdr:
                    stk, divi = row[0].strip(), float(row[2])
                    if (stk_list and stk not in stk_list) or divi == 0:
                        continue
                    dt = stxcal.prev_busday(
                        str(datetime.strptime(row[1], '%m/%d/%Y').date()))
                    if dt < '2001-01-01':
                        continue
                    try:
                        res = stxdb.db_read_cmd("select c from {0:s} where "
                                                "stk='{1:s}' and date='{2:s}'".
                                                format(self.eod_tbl, stk, dt))
                        cc = float(res[0][0])
                        # Cannot handle cases when there is a split
                        # and a dividend payment on the same date
                        res = stxdb.db_read_cmd("select * from {0:s} where "
                                                "stk='{1:s}' and date='{2:s}'".
                                                format(self.divi_tbl, stk,
                                                       dt))
                        if res:
                            print('Cannot upload {0:s}, because there is '
                                  'already a split on that date: {1:s}'.
                                  format(str(row), str(res)))
                        else:
                            dbfile.write('{0:s}\t{1:s}\t{2:8.4f}\t2\n'.
                                         format(stk, dt, (1 - divi / cc)))
                    except Exception as ex:
                        print('Failed to process {0:s}, error: {1:s}'.
                              format(str(row), str(ex)))
        try:
            stxdb.db_upload_file(out_fname, self.divi_tbl, '\t')
            print('Uploaded delta neutral splits')
        except Exception as ex:
            print('Failed to upload splits, error {0:s}'.format(str(ex)))

    # Load data from EODData data source in the database. There is a
    # daily file for each exchange (Amex, Nasdaq, NYSE). Use an
    # overlap of 5 days with the previous reconciliation interval
    # (covering up to 2012-12-31)
    def load_eoddata_files(self, sd='2013-01-02', ed='2013-11-15', stks=''):
        dt = sd
        # dt = stxcal.move_busdays(sd, -25)
        fnames = [os.path.join(self.in_dir, 'AMEX_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NASDAQ_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NYSE_{0:s}.txt')]
        while dt <= ed:
            dtc = dt.replace('-', '')
            with open(self.eod_name, 'w') as ofile:
                for fname in fnames:
                    self.load_eoddata_file(ofile, fname.format(dtc), dt, dtc,
                                           stks)
            try:
                stxdb.db_upload_file(self.eod_name, self.eod_tbl, '\t')
                print('{0:s}: uploaded eods'.format(dt))
            except Exception as ex:
                print('Failed to upload {0:s}, error {1:s}'.format(dt,
                                                                   str(ex)))
            dt = stxcal.next_busday(dt)

    # Load data from a single EODData file in the database Perform
    # some quality checks on the data: do not upload days where volume
    # is 0, or where the open/close are outside the [low, high] range.
    def load_eoddata_file(self, ofile, ifname, dt, dtc, stks=''):
        stk_list = [] if stks == '' else stks.split(',')
        db_stx, _ = self.create_exchange()
        with open(ifname, 'r') as ifile:
            lines = ifile.readlines()
            for line in lines[1:]:
                tokens = line.replace(dtc, dt).strip().split(',')
                stk = tokens[0].strip()
                if (stk_list and stk not in stk_list) or ('/' in stk) or \
                   ('*' in stk) or (stk in ['AUX', 'PRN']):
                    continue
                if stk not in db_stx:
                    insert_stx = "INSERT INTO equities VALUES "\
                                 "('{0:s}', '', 'US Stocks', 'US')".format(stk)
                    stxdb.db_write_cmd(insert_stx)
                o = float(tokens[2])
                hi = float(tokens[3])
                lo = float(tokens[4])
                c = float(tokens[5])
                v = int(tokens[6])
                if v == 0 or o < lo or o > hi or c < lo or c > hi or \
                   len(tokens[0]) > 6:
                    continue
                ofile.write('{0:s}\t0\n'.format('\t'.join(tokens)))

    # Load data from the market archive.  Data is located in three
    # different directories (AMEX, NASDAQ and NYSE)
    def load_marketdata_files(self, sd='1962-01-02', ed='2016-12-31'):
        log_fname = 'splits_divis_{0:s}.csv'.format(datetime.now().
                                                    strftime('%Y%m%d%H%M%S'))
        exchanges = ['AMEX', 'NASDAQ', 'NYSE']
        db_stx, stx_dct = self.create_exchange()
        with open(log_fname, 'w') as logfile:
            for exchange in exchanges:
                exch_dir = os.path.join(self.in_dir, exchange)
                num = 0
                files = os.listdir(exch_dir)
                total = len(files)
                print('{0:s} uploading {1:d} {2:s} stocks'.
                      format(stxcal.print_current_time(), total, exchange))
                for fname in files:
                    try:
                        self.load_marketdata_file(
                            os.path.join(exch_dir, fname), logfile, db_stx)
                    except Exception as ex:
                        print('Failed to load {0:s}: {1:s}'.
                              format(fname, str(ex)))
                    num += 1
                    if num % 250 == 0 or num == total:
                        print('{0:s} uploaded {1:4d}/{2:4d} {3:s} stocks'.
                              format(stxcal.print_current_time(), num, total,
                                     exchange))

    # For each marketdata file, back out splits and dividends; adjust
    # volume for splits (but not dividends).  Update the database with
    # the splits / divis, and the eod data
    def load_marketdata_file(self, ifname, logfile, db_stx):
        df = pd.read_csv(ifname)
        dt_dict = {}
        for row in df.iterrows():
            dt_dict[row[1][0]] = row[0]
        stk = ifname[ifname.rfind('/') + 1:ifname.rfind('.')]
        if stk not in db_stx:
            insert_stx = "INSERT INTO equities VALUES "\
                         "('{0:s}', '', 'US Stocks', 'US')".format(stk)
            stxdb.db_write_cmd(insert_stx)
        df['R'] = df['Close'] / df['Adj Close']
        df['R_1'] = df['R'].shift()
        df.R_1.fillna(df.R, inplace=True)
        df['RR'] = np.round(df['R'] / df['R_1'], 4)
        df['RR_1'] = df['RR'].shift(-1)
        df.RR_1.fillna(df.RR, inplace=True)
        df['IRR'] = np.round(df['R_1'] / df['R'], 4)
        df['IRR_1'] = df['IRR'].shift(-1)
        df.IRR_1.fillna(df.IRR, inplace=True)
        splits_divis = df.query('RR_1<0.999 | RR_1>1.001')
        splits_dict = {}
        split_tables = []
        for tbl in ['dn_dividends', 'dividends', 'split']:
            res = stxdb.db_read_cmd(self.sql_show_tables.format(tbl))
            if len(res) == 1:
                split_tables.append(tbl)
        for row in splits_divis.iterrows():
            dt = row[1]['Date']
            ratio, iratio = row[1]['RR_1'], row[1]['IRR_1']
            sd_type = 0 if ratio <= 0.95 or ratio >= 1.05 else 2
            validation = ''
            for tbl in split_tables:
                sql = "select * from {0:s} where stk='{1:s}' and "\
                      "date='{2:s}'".format(tbl, stk, dt)
                res = stxdb.db_read_cmd(sql)
                if len(res) > 0:
                    tbl_sd_type = res[0][3]
                    if tbl_sd_type == 1:
                        tbl_sd_type = 0
                    validation = tbl
                    if tbl_sd_type != sd_type:
                        sd_type = tbl_sd_type + 3
                    break
            if validation == '':
                sd_type += 6
            if sd_type in [0, 3, 6]:
                splits_dict[dt] = iratio
            logfile.write('{0:s},{1:s},{2:f},{3:f},{4:d},{5:s}\n'.
                          format(stk, dt, ratio, iratio, sd_type, validation))
            stxdb.db_write_cmd("insert into {0:s} values ('{1:s}', '{2:s}', "
                               "{3:f}, {4:d})".
                               format(self.divi_tbl, stk, dt, ratio, sd_type))
        for dt in reversed(sorted(splits_dict.keys())):
            s_ix, e_ix = 0, dt_dict[dt]
            df.loc[s_ix:e_ix, 'Volume'] = df['Volume'] / splits_dict[dt]
        upload_fname = '{0:s}/eod.txt'.format(self.upload_dir)
        with open(upload_fname, 'w') as ofile:
            for r in df.iterrows():
                if pd.isnull(r[1]['Date']) or pd.isnull(r[1]['Open']) or \
                   pd.isnull(r[1]['High']) or pd.isnull(r[1]['Low']) or \
                   pd.isnull(r[1]['Close']) or pd.isnull(r[1]['Volume']):
                    continue
                ofile.write('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t'
                            '{5:.2f}\t{6:.0f}\t0\n'.
                            format(stk, r[1]['Date'], r[1]['Open'],
                                   r[1]['High'], r[1]['Low'], r[1]['Close'],
                                   r[1]['Volume']))
        stxdb.db_upload_file(upload_fname, self.eod_tbl, '\t')

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
            except Exception as ex:
                print('Failed to reconcile {0:s}, error {1:s}'.
                      format(stk, str(ex)))
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
        q = "select date, spot from opt_spots where stk='{0:s}' {1:s}".\
            format(stk, stxdb.db_sql_timeframe(sd, ed, True))
        spot_df = pd.read_sql(q, stxdb.db_get_cnx())
        spot_df.set_index('date', inplace=True)
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
                                " and date between '{2:s}' and '{3:s}' and "
                                "divi_type = 1".format(self.divi_tbl, stk, sd,
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
    # To avoid defining a spli t for every spike ina ratio (which can
    # be due, for instancem to one day with wrong data), we are
    # looking at the ratio to stay the same over three days before the
    # day where the ratio change and 2 days after.
    def calc_implied_splits(self, df, stk, sd, ed):
        stxdb.db_write_cmd("delete from {0:s} where stk = '{1:s}' and date "
                           "between '{2:s}' and '{3:s}' and divi_type = 1".
                           format(self.divi_tbl, stk, sd, ed))
        df['r'] = df['spot'] / df['c']
        for i in [x for x in range(-3, 4) if x != 0]:
            df['r{0:d}'.format(i)] = df['r'].shift(-i)
        df['rr'] = df['r1'] / df['r']
        df_f1 = df[(abs(df['rr'] - 1) > 0.05) & (df['c'] > 1.0) &
                   (np.round(df['r-1'] - df['r'], 2) == 0) &
                   (np.round(df['r-2'] - df['r'], 2) == 0) &
                   (np.round(df['r-3'] - df['r'], 2) == 0) &
                   (np.round(df['r2'] - df['r1'], 2) == 0) &
                   (np.round(df['r3'] - df['r1'], 2) == 0) &
                   (df['volume'] > 0)]
        for r in df_f1.iterrows():
            stxdb.db_write_cmd("insert into {0:s} values ('{1:s}', '{2:s}', "
                               "{3:.4f}, 1)".format(self.divi_tbl, stk,
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
        coverage = np.round(100.0 * ts_days / spot_days, 2)
        # apply the split adjustments
        ts.splits.clear()
        splits = stxdb.db_read_cmd("select date, ratio, divi_type from {0:s} "
                                   "where stk='{1:s}' and divi_type = 1".
                                   format(self.divi_tbl, ts.stk))
        for s in splits:
            ts.splits[pd.to_datetime(stxcal.next_busday(s[0]))] = \
                [float(s[1]), int(s[2])]
        ts.adjust_splits_date_range(0, len(ts.df) - 1, inv=1)
        df.drop(['c'], inplace=True, axis=1)
        df = df.join(ts.df[['c']])

        # calculate statistics: coverage and mean square error
        def msefun(x):
            return 0 if x['spot'] == trunc(x['c']) or x['volume'] == 0 \
                else pow(1 - x['c'] / x['spot'], 2)
        df['mse'] = df.apply(msefun, axis=1)
        mse = pow(df['mse'].sum() / min(len(df['mse']), len(spot_df)), 0.5)
        if dbg:
            df.to_csv('{0:s}/dbg/{1:s}_{2:s}{3:s}_recon.csv'.
                      format(self.data_dir, ts.stk, self.eod_tbl, sfx))
        return df, coverage, mse

    def cleanup(self):
        # drop the EOD and splits tables
        stxdb.db_write_cmd('drop table {0:s}'.format(self.eod_tbl))
        stxdb.db_write_cmd('drop table {0:s}'.format(self.divi_tbl))
        # if reconciliation table exists, delete all the records that
        # correspond to the self.recon_name variable
        res = stxdb.db_read_cmd(self.sql_show_tables.format(self.recon_tbl))
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
                elif abs(1 - rec.mse / mse0) < 0.01:
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
            sql = "delete from {0:s} where stk='{1:s}' and date in (".\
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
                                   ", {3:.4f}, 1)".format(self.divi_tbl, stk,
                                                          str(s[0].date()),
                                                          s[1]))
        df, ts = self.build_df_ts(spot_df, stk, sd, ed)
        return len(wrong_recs), df, ts

    # Utility function that builds a stock time series and a data
    # frame that will include the reconciliation calculations.
    def build_df_ts(self, spot_df, stk, sd, ed):
        try:
            ts = StxTS(stk, sd, ed, self.eod_tbl, self.divi_tbl)
        except Exception as ex:
            print('Failed to build StxTS: error {0:s}'.format(str(ex)))
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
                except Exception as ex:
                    print('Failed to EOD upload stock {0:s}, error {1:s}'.
                          format(stk, str(ex)))
        print('{0:s} - uploaded {1:d} stocks'.format(self.rec_name, num))

    def upload_stk(self, eod_table, split_table, stk, sd, ed, rec_interval):
        ts = StxTS(stk, sd, ed, self.eod_tbl, self.divi_tbl)
        ts.splits.clear()
        impl_splits = stxdb.db_read_cmd(
            "select date, ratio, divi_type from {0:s} where stk='{1:s}' "
            "and divi_type = 1".format(self.divi_tbl, stk))
        # for implied splits, need to perform a reverse adjustment
        # for the adjustment to work, move the split date to next business day
        for s in impl_splits:
            ts.splits[pd.to_datetime(stxcal.next_busday(s[0]))] = \
                [float(s[1]), int(s[2])]
        ts.adjust_splits_date_range(0, len(ts.df) - 1, inv=1)
        with open(self.eod_name, 'w') as ofile:
            for idx, row in ts.df.iterrows():
                if row['volume'] > 0:
                    ofile.write('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t'
                                '{4:.2f}\t{5:.2f}\t{6:.0f}\t0\n'.
                                format(stk, str(idx.date()), row['o'],
                                       row['hi'], row['lo'], row['c'],
                                       row['volume']))
        # upload the eod data in the database
        stxdb.db_upload_file(self.eod_name, eod_table, '\t')
        # finally, upload all the splits into the final split table
        stxdb.db_write_cmd('insert into {0:s} (stk,date,ratio,divi_type) '
                           'select stk, date, ratio, divi_type from {1:s} '
                           "where stk='{2:s}'".
                           format(split_table, self.divi_tbl, stk))
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
            sql = "select distinct stk from {0:s} where date between '{1:s}'"\
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
            except Exception as ex:
                print('Failed to reconcile {0:s}, error {1:s}'.
                      format(stk, str(ex)))
            finally:
                num += 1
                if num % 500 == 0 or num == num_stx:
                    print('Reconciled big changes for {0:4d} out of {1:4d}'
                          ' stocks'.format(num, num_stx))

    def reconcile_big_changes(self, stk, sd, ed, split_tbls):
        ts = StxTS(stk, sd, ed, self.eod_tbl, self.divi_tbl)
        ts.df['chg'] = ts.df['c'].pct_change().abs()
        ts.df['id_chg'] = (ts.df['hi'] - ts.df['lo']) / ts.df['c']
        ts.df['avg_chg20'] = ts.df['chg'].rolling(20).mean()
        ts.df['avg_v20'] = ts.df['volume'].shift().rolling(20).mean()
        ts.df['avg_v50'] = ts.df['volume'].shift().rolling(50).mean()
        df_review = ts.df.query('chg>=0.15 and chg>2*avg_chg20 and '
                                'volume<(0.5+10*chg)*avg_v50 and avg_v20>1000 '
                                'and id_chg<0.6*chg and c>3 and '
                                '(chg-id_chg)>2*avg_chg20 and (c>8 or chg>1)')
        all_splits = {}
        for split_table in split_tbls:
            res = stxdb.db_read_cmd('select date, ratio from {0:s} where '
                                    "stk='{1:s}' and divi_type=0".
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
                      "{3:.4f},0)".format(self.divi_tbl, stk,
                                          stxcal.prev_busday(big_chg_dt),
                                          splits[big_chg_dt])
                try:
                    stxdb.db_write_cmd(sql)
                except Exception as ex:
                    print('Failed to write values to DB, error {0:s}'.
                          format(str(ex)))
            ofname = '{0:s}/big_change_recon_{1:s}_{2:s}.txt'.\
                     format(self.data_dir, sd.replace('-', ''),
                            ed.replace('-', ''))
            with open(ofname, 'a') as ofile:
                ofile.write('{0:5s} {1:s} {2:6.2f} {3:6.2f} {4:6.2f} {5:6.2f} '
                            '{6:9.0f} {7:5.3f} {8:5.3f} {9:9.0f} {10:s}\n'.
                            format(stk, big_chg_dt, row['o'], row['hi'],
                                   row['lo'], row['c'], row['volume'],
                                   row['chg'], row['avg_chg20'],
                                   row['avg_v50'], db_splits))

    def correct_split_adjustment_error(self, fname):
        with open(fname, 'r') as ifile:
            lines = ifile.readlines()
            for line in lines:
                tokens = line.split()
                if len(tokens) > 10:
                    stk, dt = tokens[0], tokens[1]
                    sql = "update {0:s} set date='{1:s}' where "\
                        "stk='{2:s}' and date='{3:s}'".\
                        format(self.divi_tbl, stxcal.prev_busday(dt), stk, dt)
                    stxdb.db_write_cmd(sql)

    # Capture all the large price changes without a volume that is
    # larger than the average, and check that these are not missed
    # splits.  Also, check split and dividend data from yahoo, EODData
    # and mypivots, deltaneutral, make sure that these splits are
    # reflected in the EOD data, and, if they are, make sure that they
    # are added to the splits database.
    # def eod_reconciliation(self, sd, ed):
    #     sql = "select distinct stk from {0:s} where date between '{1:s}'"\
    #           " and '{2:s}'".format(self.eod_tbl, sd, ed)
    #     # c = pycurl.Curl()
    #     # c.setopt(pycurl.SSL_VERIFYPEER, 0)
    #     # c.setopt(pycurl.SSL_VERIFYHOST, 0)
    #     res = stxdb.db_read_cmd(sql)
    #     stk_list = [x[0] for x in res]
    #     num_stx = len(stk_list)
    #     print('EOD reconciliation for {0:d} stocks from {1:s}'.
    #           format(num_stx, self.eod_tbl))
    #     num = 0
    #     for stk in stk_list:
    #         try:
    #             self.reconcile_stk_eod(c, stk, sd, ed)
    #         except Exception as ex:
    #             print('{0:s} EOD reconciliation failed: {1:s}'.
    #                   format(stk, str(ex)))
    #         finally:
    #             num += 1
    #             if num % 500 == 0 or num == num_stx:
    #                 print('Reconciled EOD for {0:4d} out of {1:4d}'
    #                       ' stocks'.format(num, num_stx))

    def reconcile_stk_eod(self, c, stk, sd, ed):
        ts = StxTS(stk, sd, ed, self.eod_tbl, self.divi_tbl)
        ts.df['chg'] = ts.df['c'].pct_change().abs()
        ts.df['id_chg'] = (ts.df['hi'] - ts.df['lo']) / ts.df['c']
        ts.df['avg_chg20'] = ts.df['chg'].rolling(20).mean()
        ts.df['avg_v20'] = ts.df['volume'].shift().rolling(20).mean()
        ts.df['avg_v50'] = ts.df['volume'].shift().rolling(50).mean()
        df_review = ts.df.query('chg>=0.15 and chg>2*avg_chg20 and '
                                'v<(0.5+10*chg)*avg_v50 and avg_v20>1000 and '
                                'id_chg<0.6*chg and c>3 and '
                                '(chg-id_chg)>2*avg_chg20 and (c>8 or chg>1)')
        if len(df_review) > 0:
            sdy, sdm, sdd = sd.split('-')
            edy, edm, edd = ed.split('-')
            c.setopt(c.URL, self.yhoo_url.format(stk, int(sdm) - 1, int(sdd),
                                                 int(sdy), int(edm) - 1,
                                                 int(edd), int(edy), 'volume'))
            res_buffer = BytesIO()
            c.setopt(c.WRITEDATA, res_buffer)
            c.perform()
            divis = res_buffer.getvalue().decode('iso-8859-1').strip().\
                split('\n')
            for divi in divis[1:]:
                print(divi)
            c.setopt(c.URL, self.yhoo_url.format(stk, int(sdm) - 1, int(sdd),
                                                 int(sdy), int(edm) - 1,
                                                 int(edd), int(edy), 'd'))
            c.setopt(c.WRITEDATA, res_buffer)
            c.perform()
            prices = res_buffer.getvalue().decode('iso-8859-1').strip().\
                split('\n')
            for rec1, rec2 in zip(prices[1:-2], prices[2:]):
                px1 = rec1.split(',')
                px2 = rec2.split(',')
                ratio_1 = float(px1[4]) / float(px1[6])
                ratio_2 = float(px2[4]) / float(px2[6])
                split = ratio_1 / ratio_2
                if np.round(split, 2) != 1:
                    print('{0:s}: {1:s} - {2:4f}'.format(stk, px1[0],
                                                         np.round(split, 4)))
                    db_cmd = "insert into {0:s} values('{1:s}','{2:s}',"\
                             "{3:4f},0) on conflict (stk, date) do update "\
                             "set ratio={4:4f}, divi_type=0".\
                             format(self.divi_tbl, stk, px1[0],
                                    np.round(split, 4), np.round(split, 4))
                    stxdb.db_write_cmd(db_cmd)
            ofname = '{0:s}/big_change_recon_{1:s}_{2:s}.txt'.\
                     format(self.data_dir, sd.replace('-', ''),
                            ed.replace('-', ''))
            with open(ofname, 'a') as ofile:
                for idx, row in df_review.iterrows():
                    big_chg_dt = str(idx.date())
                    ofile.write('{0:5s} {1:s} {2:6.2f} {3:6.2f} {4:6.2f} '
                                '{5:6.2f} {6:9.0f} {7:5.3f} {8:5.3f} {9:9.0f}'
                                ' {10:s}\n'.
                                format(stk, big_chg_dt, row['o'], row['hi'],
                                       row['lo'], row['c'], row['volume'],
                                       row['chg'], row['avg_chg20'],
                                       row['avg_v50']))

    # 1. Upload EODData starting from 2010.
    # 2. Compare the EODData volume with the volume in the EOD table,
    #    and also with the volume in the marketdata table
    # 3. Reconcile the eod and marketdata volumes against EODData.
    # 4. Subsequently, generate report with volume ratios that are different
    def volume_check(self):
        # ts.set_day('2006-12-18')
        # ts.next_day()
        # df1 = ts.df[['volume']].ix[ts.pos-20:ts.pos]
        # av1 = round(df1[['volume']].mean()['volume'], 0)
        # df2 = ts.df[['volume']].ix[ts.pos+1:ts.pos+21]
        # av2 = round(df2[['volume']].mean()['volume'], 0)
        # av2/av1
        pass

    def load_stooq_files(self, sd, ed):
        dirs = ['{0:s}/bonds'.format(self.in_dir),
                '{0:s}/commodities'.format(self.in_dir),
                '{0:s}/currencies/major'.format(self.in_dir),
                '{0:s}/indices'.format(self.in_dir),
                '{0:s}/lme'.format(self.in_dir),
                '{0:s}/money_market'.format(self.in_dir)]
        for instr_dir in dirs:
            files = os.listdir(instr_dir)
            instr_name = instr_dir[instr_dir.rfind('/') + 1:]
            print('{0:s} uploading {1:s}'.format(stxcal.print_current_time(),
                                                 instr_dir))
            for fname in files:
                symbol = fname[:-4].upper()
                with open('{0:s}/{1:s}'.format(instr_dir, fname),
                          'r') as ifile:
                    lines = ifile.readlines()
                ofname = '{0:s}/eod.txt'.format(self.upload_dir)
                self.load_stooq_history(symbol, lines[1:], instr_name, sd, ed,
                                        ofname)

    def load_stooq_history(self, symbol, lines, instr_name, sd, ed, ofname):
        with open(ofname, 'w') as ofile:
            for line in lines:
                try:
                    dt, o, hi, lo, c, v, oi = line.split(',')
                    dt = '{0:s}-{1:s}-{2:s}'.format(dt[0:4], dt[4:6], dt[6:8])
                    if dt < sd or dt > ed or not stxcal.is_busday(dt):
                        continue
                    o = float(o)
                    hi = float(hi)
                    lo = float(lo)
                    c = float(c)
                    v = int(v)
                    oi = int(oi)
                    if symbol.startswith('^'):
                        v //= 1000
                    if v == 0:
                        v = 1
                    if((instr_name in ['bonds', 'money_market', 'major'] or
                        symbol.endswith('.B') or symbol.endswith('6.F')) and
                       'XAG' not in symbol and 'XAU' not in symbol):
                        o *= 10000
                        hi *= 10000
                        lo *= 10000
                        c *= 10000
                    if symbol in ['HO.F', 'NG.F', 'RB.F']:
                        o *= 100
                        hi *= 100
                        lo *= 100
                        c *= 100
                    oline = '{0:s}\t{1:s}\t{2:f}\t{3:f}\t{4:f}\t{5:f}\t{6:d}'.\
                            format(symbol, dt, o, hi, lo, c, v)
                    oline = '{0:s}\t{1:d}\n'.format(oline, oi) \
                            if instr_name == 'commodities' else \
                            '{0:s}\n'.format(oline)
                    ofile.write(oline)
                except Exception as ex:
                    print('{0:s}: Failed to parse line {1:s}, error {2:s}'.
                          format(symbol, line, str(ex)))
        tbl_name = self.ftr_tbl if instr_name == 'commodities' \
            else self.eod_tbl
        stxdb.db_upload_file(ofname, tbl_name, '\t')

    def multiply_prices(self, o, h, l, c, factor):
        return o * factor, h * factor, l * factor, c * factor

    def parseeodline(self, line, db_stx):
        stk, _, dt, o, h, l, c, v, oi = line.split(',')
        # look only at the US stocks, for the time being
        if not stk.endswith('.US'):
            return
        dt = '{0:s}-{1:s}-{2:s}'.format(dt[0:4], dt[4:6], dt[6:8])
        if not stxcal.is_busday(dt):
            raise Exception('{0:s} is not a business day'.format(dt))
        o, h, l, c = float(o), float(h), float(l), float(c)
        # Make sure o and c are in the interval [l, h]
        o = o if o <= h and o >= l else (h if o > h else l)
        c = c if c <= h and c >= l else (h if c > h else l)
        v, oi = int(v), int(oi)
        if stk.endswith('.US'):  # proces stock tickers, volume must be > 0
            stk = stk[:-3].replace("-.", ".P.").replace("_", ".").replace(
                '-', '.')
            if v == 0:
                raise Exception('Zero volume for stock')
            if len(stk) > 8:
                raise Exception('Ticker {0:s} too long'.format(stk))
        elif stk.endswith('.B'):  # multiply bond prices by 10000
            o, h, l, c = self.multiply_prices(o, h, l, c, 10000)
        elif stk.endswith('6.F'):  # multiply currency future prices by 10000
            o, h, l, c = self.multiply_prices(o, h, l, c, 10000)
        elif stk in ['HO.F', 'NG.F', 'RB.F']:  # express prices in cents
            o, h, l, c = self.multiply_prices(o, h, l, c, 100)
        elif stk.startswith('^'):  # divide index volumes by 1000
            v = 1 if v == 0 else v // 1000
        elif '.' not in stk and 'XAG' not in stk and 'XAU' not in stk:
            # multiply FX/Money Market prices by 10000
            o, h, l, c = self.multiply_prices(o, h, l, c, 10000)
        # all tickers ending in .F are futures, except the LME tickers
        v = 1 if v == 0 else v
        if stk not in db_stx:
            insert_stx = "INSERT INTO equities VALUES "\
                         "('{0:s}', '', 'US Stocks', 'US')".format(stk)
            stxdb.db_write_cmd(insert_stx)
        db_cmd = "insert into {0:s} values('{1:s}','{2:s}',{3:2f},{4:2f},"\
            "{5:2f},{6:2f},{7:d},{8:d}) on conflict (stk, date) do update "\
            "set volume={9:d}, open_interest={10:d}".format(
                self.eod_tbl, stk, dt, o, h, l, c, v, oi, v, oi)
        stxdb.db_write_cmd(db_cmd)

    def parseeodfiles(self, s_date, e_date):
        dt = s_date
        num_days = stxcal.num_busdays(s_date, e_date)
        print('Uploading EOD data for {0:d} days'.format(num_days))
        day_num = 0
        while dt <= e_date:
            db_stx, _ = self.create_exchange()
            try:
                with open('{0:s}/{1:s}_d.prn'.format
                          (self.in_dir, dt.replace('-', ''))) as ifile:
                    lines = ifile.readlines()
            except IOError as ioe:
                print(str(ioe))
                continue
            for line in lines:
                try:
                    self.parseeodline(line, db_stx)
                except Exception as ex:
                    logging.info('Error with line {0:s}: {1:s}'.format
                                 (line.strip(), str(ex)))
            dt = stxcal.next_busday(dt)
            day_num += 1
            if day_num % 20 == 0 or day_num == num_days:
                print(' Uploaded EOD data for {0:d} days'.format(day_num))

    def parse_ed_splits(self, split_file):
        # curl
        # 'http://ichart.finance.yahoo.com/x?s=IBM&a=00&b=2&c=2011&d=04&e=25&f=2017&g=v&y=0&z=30000'
        num_splits = 0
        with open('{0:s}/{1:s}'.format(self.in_dir, split_file), 'r') as ifile:
            lines = ifile.readlines()
        for line in lines[1:]:
            exch, stk, dt, ratio = line.split()
            if exch not in ['AMEX', 'NASDAQ', 'NYSE']:
                continue
            dt = str(datetime.strptime(dt, '%m/%d/%Y').date())
            denom, num = ratio.split('-')
            ratio = float(num) / float(denom)
            if ratio > 9999:
                continue
            db_cmd = "insert into {0:s} values('{1:s}','{2:s}',{3:8.4f},0) "\
                     "on conflict (stk, date) do update set ratio={4:8.4f}".\
                     format(self.divi_tbl, stk, dt, ratio, ratio)
            stxdb.db_write_cmd(db_cmd)
            num_splits += 1
        print('{0:s}: uploaded/updated {1:d} splits'.format(split_file,
                                                            num_splits))


if __name__ == '__main__':
    logging.basicConfig(filename='stxeod.log', level=logging.INFO)
    s_date = '2001-01-01'
    e_date = '2012-12-31'
    data_dir = os.getenv('DATA_DIR')
    my_dir = '{0:s}/bkp'.format(data_dir)
    dn_dir = '{0:s}/stockhistory_2017'.format(data_dir)
    ed_dir = '{0:s}/EODData'.format(data_dir)
    md_dir = '{0:s}/md'.format(data_dir)
    sq_dir = '{0:s}/ALL'.format(data_dir)
    my_eod = StxEOD(my_dir, 'my', 'reconciliation')
    dn_eod = StxEOD(dn_dir, 'dn', 'reconciliation')
    ed_eod = StxEOD(ed_dir, 'ed', 'reconciliation')
    md_eod = StxEOD(md_dir, 'md', 'reconciliation')
    sq_eod = StxEOD(sq_dir, 'sq', 'reconciliation')
    my_eod.load_my_files()
    dn_eod.load_deltaneutral_files()
    ed_eod.load_eoddata_files()
    log_fname = 'splits_divis_{0:s}.csv'.format(datetime.now().
                                                strftime('%Y%m%d%H%M%S'))
    with open(log_fname, 'w') as logfile:
        md_eod.load_marketdata_files()
    sq_eod.parseeodfiles(s_date, e_date)
    # my_eod.reconcile_spots(s_date, e_date)
    # dn_eod.reconcile_spots(s_date, e_date)
    # To debug a reconciliation:
    # my_eod.reconcile_opt_spots('EXPE', '2002-02-01', '2012-12-31', True)
    # my_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # my_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # eod = StxEOD('', 'eod', 'split', 'reconciliation')
    # eod.split_reconciliation('', s_date, e_date, ['splits', 'my_split',
    #                                               'dn_split'])
    #
    # s_date = '2013-01-02'
    # e_date = '2013-11-15'
    # ed_eod.reconcile_spots(s_date, e_date)
    # md_eod.reconcile_spots(s_date, e_date)
    # dn_eod.reconcile_spots(s_date, e_date)
    # # To debug a reconciliation:
    # # my_eod.reconcile_opt_spots('AEOS', '2002-02-01', '2012-12-31', True)
    # ed_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # md_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # ed_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # md_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # eod = StxEOD('', 'eod', 'split', 'reconciliation')
    # eod.split_reconciliation('', s_date, e_date, ['splits', 'my_split',
    #                                               'dn_split', 'md_split'])

    # s_date = '2013-11-18'
    # e_date = '2016-08-23'
    # md_eod.reconcile_spots(s_date, e_date)
    # dn_eod.reconcile_spots(s_date, e_date)
    # # To debug a reconciliation:
    # # my_eod.reconcile_opt_spots('AEOS', '2002-02-01', '2012-12-31', True)
    # md_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date)
    # md_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # dn_eod.upload_eod('eod', 'split', '', s_date, e_date, 0.02, 15)
    # eod = StxEOD('', 'eod', 'split', 'reconciliation')
    # eod.split_reconciliation('', s_date, e_date, ['splits', 'my_split',
    #                                               'dn_split', 'md_split'])
    # sq_eod.load_stooq_files('1901-01-01', '2016-08-23')
    #
    # This reconciliation is done to check that data from stooq is
    # consistent with opt_spots.  All the stocks during this time
    # period passed the reconciliation, as it should be the case. So,
    # there is no reason to invoke upload_eod. Instead, we will just
    # upload the stooq data into eod.
    #
    # s_date = '2016-08-24'
    # e_date = '2016-12-31'
    # for sdt in ['20160928', '20161223', '20170102', '20170317']:
    #     sq_eod.parse_ed_splits('splits_{0:s}.txt'.format(sdt))
    # sq_eod.reconcile_spots(s_date, e_date)
    # s_date = '2016-08-24'
    # e_date = '2017-03-17'
    # eod = StxEOD(StxEOD.dload_dir, 'eod', 'split', 'reconciliation', 'ftr')
    # eod.parseeodfiles(s_date, e_date)
    # dn_eod = StxEOD('c:/goldendawn/dn_data', 'eod', 'dn_split',
    #                 'reconciliation')
    # dn_eod.load_deltaneutral_splits([])
    # dn_eod.load_deltaneutral_divis([])
    # s_date = '2016-08-01'
    # e_date = '2017-03-17'
    # eod = StxEOD(StxEOD.dload_dir, 'eod', 'split', 'reconciliation', 'ftr')
    # eod.split_reconciliation('', s_date, e_date,
    #                          ['splits', 'split_sq', 'dn_split', 'md_split',
    #                           'split_dn'])
