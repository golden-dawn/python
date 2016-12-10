import csv
from datetime import datetime
import os
import pandas as pd
import stxdb
from stxcal import StxCal
from stxts import StxTS
import sys

class StockHistory :

    def __init__(self, cnx, sh_dir = 'c:/goldendawn/stockhistory',
                 d_dir = 'c:/goldendawn/data',
                 db_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads') :
        self.cnx    = cnx
        self.sc     = StxCal()
        self.sh_dir = sh_dir
        self.d_dir  = d_dir
        self.db_dir = db_dir
        
    def parse_eod_data(self) :
        for yr in range(2001, 2017) :
            fname    = '{0:s}/stockhistory_{1:d}.csv'.format(self.sh_dir, yr)
            stx      = {}
            with open(fname) as csvfile :
                frdr = csv.reader(csvfile)
                for row in frdr :
                    stk  = row[0].strip()
                    data = stx.get(stk, [])
                    data.append('{0:s},{1:s},{2:.2f},{3:.2f},{4:.2f},{5:.2f},' \
                                '{6:d},{7:.2f}\n'.\
                                format(stk, str(datetime.strptime(row[1],
                                                                  '%m/%d/%Y').\
                                                date()),
                                       float(row[2]), float(row[3]),
                                       float(row[4]), float(row[5]),
                                       int(row[7]), float(row[6])))
                    stx[stk] = data
            print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
            for stk, recs in stx.items() :
                if stk in ['AUX', 'PRN'] or stk.find('/') != -1 or \
                   stk.find('*') != -1 :
                    continue
                with open('{0:s}/{1:s}.csv'.format(self.d_dir, stk), 'a') \
                     as ofile :
                    for rec in recs :
                        ofile.write(rec)

    def load_splits(self) :
        fname            = '{0:s}/stocksplits.csv'.format(self.sh_dir)
        with open(fname) as csvfile :
            frdr         = csv.reader(csvfile)
            stx          = {}
            for row in frdr :
                stk      = row[0].strip()
                data     = stx.get(stk, {})
                dt       = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                dt       = str(self.sc.prev_busday(dt).date())
                if dt in data :
                    print('Duplicate entry for {0:s} on {1:s}'.format(stk, dt))
                    continue
                data[dt] = 1 / float(row[2])
                stx[stk] = data
            print('Found splits for {0:d} stocks'.format(len(stx)))
        fname            = 'c:/goldendawn/stocksplits.csv'
        with open(fname, 'w') as ofile :
            for stk, splits in stx.items() :
                for dt, ratio in splits.items() :
                    ofile.write('{0:s},{1:s},{2:f}\n'.format(stk, dt, ratio))
        return stx

    def load_stk(self, stk) :
        stk_fname                = '{0:s}/{1:s}.csv'.format(self.d_dir, stk)
        stk_data                 = []
        stk_dict                 = {}
        ixx                      = 0
        with open(stk_fname, 'r') as ifile :
            frdr                 = csv.reader(ifile)
            for row in frdr :
                stk_data.append(row)
                stk_dict[row[1]] = ixx
                ixx              = ixx + 1
        return stk_data, stk_dict
        
    def gen_split_db_upload(self, stx) :
        split_list       = []
        for stk, splits in stx.items() :
            split_dates  = list(splits.keys())
            split_dates.sort()
            six          = 0
            split_dt     = split_dates[six]
            stk_data, dd = self.load_stk(stk)
            sdate, edate = stk_data[0][1], stk_data[-1][1]
            while six < len(split_dates) and split_dt < sdate :
                print('{0:s}:{1:s}: split before start'.format(stk, split_dt))
                six      = six + 1
                if six < len(split_dates) :
                    split_dt = split_dates[six]
            if six >= len(split_dates) :
                continue
            for r1, r2 in zip(stk_data, stk_data[1:]) :
                if r1[1] <= split_dt and r2[1] > split_dt :
                    try :
                        r_1  = float(r1[5]) / float(r1[7])
                        r_2  = float(r2[5]) / float(r2[7])
                        rr   = round(r_2 / r_1, 4)
                        if round(rr, 2) == 1.0 :
                            print('{0:s}:{1:s}: no split'.format(stk, split_dt))
                        else :
                            ratio  = round(splits.get(split_dt), 4)
                            if round(rr, 2) != round(ratio, 2) :
                                print('{0:s}: {1:s}: diff split ratio: {2:f} ' \
                                      'instead of {3:f}'.\
                                      format(stk, split_dt, rr, ratio))
                                ratio = rr
                            split_list.append('{0:s}\t{1:s}\t{2:.4f}\n'.\
                                              format(stk, split_dt, ratio))
                    except ZeroDivisionError :
                        print('{0:s}:{1:s}:ZeroDivError'.format(stk, split_dt))
                    six           = six + 1
                    if six >= len(split_dates) :
                        break
                    else :
                        split_dt  = split_dates[six]
            while six < len(split_dates) :
                print('{0:s}:{1:s}: split after end'.format(stk, split_dt))
                six = six + 1
                if six < len(split_dates) :
                    split_dt  = split_dates[six]
        self.gen_upload_file(split_list)

    def gen_upload_file(self, split_list) :
        fname            = '{0:s}/stocksplits.txt'.format(self.db_dir)
        with open(fname, 'w') as ofile :
            for split in split_list :
                ofile.write(split)

    def adjust_volume_and_upload(self) :
        splits    = pd.read_sql('select * from split', self.cnx)
        stk_files = os.listdir(self.d_dir)
        for stk_file in stk_files :
            stk   = stk_file[:-4]
            self.adjust_stock_volume_and_upload(stk, splits)

    def adjust_stock_volume_and_upload(self, stk, splits) :
        data, dct      = self.load_stk(stk)
        stk_splits     = splits[splits['stk'] == stk]
        for ixx in stk_splits.index :
            split      = stk_splits.ix[ixx]
            ixxx       = dct.get(split['dt'])
            if ixxx is None :
                # TODO: try with the next business day
                print('{0:s}: no split date {1:s}'.format(stk, split['dt']))
                continue
            for row in data[: ixxx + 1] :
                row[6] = '{0:.0f}'.format(int(row[6]) * split['ratio'])
        ofname         = '{0:s}/eod_upload.txt'.format(self.db_dir)
        with open(ofname, 'w') as ofile :
            for row in data :
                if int(row[6]) > 0 and float(row[5]) > 0.09 :
                    ofile.write('{0:s}\n'.format('\t'.join(row[:7])))
        try :
            self.cnx.query("load data infile '{0:s}' into table eod". \
                           format(ofname))
            print('Loaded eod for {0:s}'.format(stk))
        except :
            e = sys.exc_info()[1]
            print('Failed to upload {0:s}, error {1:s}'.format(stk, str(e)))


    def reconcile_splits(self, split_fname) :
        with open(split_fname, 'r') as ifile :
            frdr       = csv.reader(ifile)
            for row in frdr :
                if row[0] == 'stk' :
                    continue                
                stk    = row[0]
                dt     = row[1]
                ratio  = float(row[2])
                print('\n{0:s} {1:s} {2:f}'.format(stk, dt, ratio))
                sd     = str(self.sc.move_busdays(dt, -3))
                ed     = str(self.sc.move_busdays(dt, 3))
                try :
                    ts = StxTS(stk, sd, ed)
                    print(ts.df)
                except :
                    ex = sys.exc_info()[1]
                    print('Load failed {0:s} between {1:s} and {2:s}: {3:s}'.\
                          format(stk, sd, ed, str(ex)))


    def reconcile_with_spots(self) :
        print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
        for stk, recs in stx.items() :
            if stk in ['AUX', 'PRN'] or stk.find('/') != -1 or \
               stk.find('*') != -1 :
                continue
            with open('{0:s}/{1:s}.csv'.format(self.d_dir, stk), 'a') \
                 as ofile :
                for rec in recs :
                    ofile.write(rec)

    def load_splits(self) :
        fname            = '{0:s}/stocksplits.csv'.format(self.sh_dir)
        with open(fname) as csvfile :
            frdr         = csv.reader(csvfile)
            stx          = {}
            for row in frdr :
                stk      = row[0].strip()
                data     = stx.get(stk, {})
                dt       = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                dt       = str(self.sc.prev_busday(dt).date())
                if dt in data :
                    print('Duplicate entry for {0:s} on {1:s}'.format(stk, dt))
                    continue
                data[dt] = 1 / float(row[2])
                stx[stk] = data
            print('Found splits for {0:d} stocks'.format(len(stx)))
        fname            = 'c:/goldendawn/stocksplits.csv'
        with open(fname, 'w') as ofile :
            for stk, splits in stx.items() :
                for dt, ratio in splits.items() :
                    ofile.write('{0:s},{1:s},{2:f}\n'.format(stk, dt, ratio))
        return stx

    def load_stk(self, stk) :
        stk_fname                = '{0:s}/{1:s}.csv'.format(self.d_dir, stk)
        stk_data                 = []
        stk_dict                 = {}
        ixx                      = 0
        with open(stk_fname, 'r') as ifile :
            frdr                 = csv.reader(ifile)
            for row in frdr :
                stk_data.append(row)
                stk_dict[row[1]] = ixx
                ixx              = ixx + 1
        return stk_data, stk_dict
        
    def gen_split_db_upload(self, stx) :
        split_list       = []
        for stk, splits in stx.items() :
            split_dates  = list(splits.keys())
            split_dates.sort()
            six          = 0
            split_dt     = split_dates[six]
            stk_data, dd = self.load_stk(stk)
            sdate, edate = stk_data[0][1], stk_data[-1][1]
            while six < len(split_dates) and split_dt < sdate :
                print('{0:s}:{1:s}: split before start'.format(stk, split_dt))
                six      = six + 1
                if six < len(split_dates) :
                    split_dt = split_dates[six]
            if six >= len(split_dates) :
                continue
            for r1, r2 in zip(stk_data, stk_data[1:]) :
                if r1[1] <= split_dt and r2[1] > split_dt :
                    try :
                        r_1  = float(r1[5]) / float(r1[7])
                        r_2  = float(r2[5]) / float(r2[7])
                        rr   = round(r_2 / r_1, 4)
                        if round(rr, 2) == 1.0 :
                            print('{0:s}:{1:s}: no split'.format(stk, split_dt))
                        else :
                            ratio  = round(splits.get(split_dt), 4)
                            if round(rr, 2) != round(ratio, 2) :
                                print('{0:s}: {1:s}: diff split ratio: {2:f} ' \
                                      'instead of {3:f}'.\
                                      format(stk, split_dt, rr, ratio))
                                ratio = rr
                            split_list.append('{0:s}\t{1:s}\t{2:.4f}\n'.\
                                              format(stk, split_dt, ratio))
                    except ZeroDivisionError :
                        print('{0:s}:{1:s}:ZeroDivError'.format(stk, split_dt))
                    six           = six + 1
                    if six >= len(split_dates) :
                        break
                    else :
                        split_dt  = split_dates[six]
            while six < len(split_dates) :
                print('{0:s}:{1:s}: split after end'.format(stk, split_dt))
                six = six + 1
                if six < len(split_dates) :
                    split_dt  = split_dates[six]
        self.gen_upload_file(split_list)

    def gen_upload_file(self, split_list) :
        fname            = '{0:s}/stocksplits.txt'.format(self.db_dir)
        with open(fname, 'w') as ofile :
            for split in split_list :
                ofile.write(split)

    def adjust_volume_and_upload(self) :
        splits    = pd.read_sql('select * from split', self.cnx)
        stk_files = os.listdir(self.d_dir)
        for stk_file in stk_files :
            stk   = stk_file[:-4]
            self.adjust_stock_volume_and_upload(stk, splits)

    def adjust_stock_volume_and_upload(self, stk, splits) :
        data, dct      = self.load_stk(stk)
        stk_splits     = splits[splits['stk'] == stk]
        for ixx in stk_splits.index :
            split      = stk_splits.ix[ixx]
            ixxx       = dct.get(split['dt'])
            if ixxx is None :
                # TODO: try with the next business day
                print('{0:s}: no split date {1:s}'.format(stk, split['dt']))
                continue
            for row in data[: ixxx + 1] :
                row[6] = '{0:.0f}'.format(int(row[6]) * split['ratio'])
        ofname         = '{0:s}/eod_upload.txt'.format(self.db_dir)
        with open(ofname, 'w') as ofile :
            for row in data :
                if int(row[6]) > 0 and float(row[5]) > 0.09 :
                    ofile.write('{0:s}\n'.format('\t'.join(row[:7])))
        try :
            self.cnx.query("load data infile '{0:s}' into table eod". \
                           format(ofname))
            print('Loaded eod for {0:s}'.format(stk))
        except :
            e = sys.exc_info()[1]
            print('Failed to upload {0:s}, error {1:s}'.format(stk, str(e)))


    def reconcile_splits(self, split_fname) :
        with open(split_fname, 'r') as ifile :
            frdr       = csv.reader(ifile)
            for row in frdr :
                if row[0] == 'stk' :
                    continue                
                stk    = row[0]
                dt     = row[1]
                ratio  = float(row[2])
                print('\n{0:s} {1:s} {2:f}'.format(stk, dt, ratio))
                sd     = str(self.sc.move_busdays(dt, -3))
                ed     = str(self.sc.move_busdays(dt, 3))
                try :
                    ts = StxTS(stk, sd, ed)
                    print(ts.df)
                except :
                    ex = sys.exc_info()[1]
                    print('Load failed {0:s} between {1:s} and {2:s}: {3:s}'.\
                          format(stk, sd, ed, str(ex)))


    def reconcile_opt_spots(self, stk) :
        q        = "select dt, spot from opt_spots where stk='{0:s}'".\
                                                              format(stk)
        spot_df  = pd.read_sql(q, self.cnx)
        spot_df.set_index('dt', inplace=True)
        ts       = StxTS(stk, '2001-01-01', '2016-12-31')
        df       = ts.df.join(spot_df)
        df['r']  = df['spot'] / df['c']
        df['r1'] = df['r'].shift(-1)
        df['r1'].fillna(method='bfill', inplace=True)
        df['r'].fillna(method='bfill', inplace=True)
        df['rr'] = df['r1']/df['r']
        df_f1    = df[((df['rr'] < 0.95) | (df['rr'] > 1.05)) & (df['c'] > 1.0)]
        splits   = {}
        for row in df_f1['rr'].iteritems():
            splits[str(row[0].date())] = round(row[1], 4)
        print('Found splits for {0:s}'.format(stk))
        dates    = list(splits.keys())
        dates.sort()
        for dt in dates :
            print('{0:s} : {1:.4f}'.format(dt, splits[dt]))
            
                    
if __name__ == '__main__' :
    cnx = stxdb.connect()
    sh  = StockHistory(cnx)
    # sh.parse_eod_data()
    # stx = sh.load_splits()
    # sh.gen_split_db_upload(stx)
    # splits    = pd.read_sql('select * from split', cnx)
    # sh.adjust_stock_volume_and_upload('AAPL', splits)
    # sh.adjust_stock_volume_and_upload('BKX', splits)
    # sh.adjust_volume_and_upload()
    sh.reconcile_splits('C:/goldendawn/splits_to_reconcile.csv')
    stxdb.disconnect(cnx)
