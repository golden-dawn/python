import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from shutil import rmtree
import stxcal
import stxdb
import zipfile


class OptEOD:
    upload_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April',
                   5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September',
                   10: 'October', 11: 'November', 12: 'December'}
    sql_create_opts = 'CREATE TABLE `{0:s}` ('\
                      '`exp` date NOT NULL,'\
                      '`und` char(6) NOT NULL,'\
                      '`cp` char(1) NOT NULL,'\
                      '`strike` decimal(9, 2) NOT NULL,'\
                      '`dt` date NOT NULL,'\
                      '`bid` decimal(9, 2) NOT NULL,'\
                      '`ask` decimal(9, 2) NOT NULL,'\
                      '`volume` int(11) NOT NULL,'\
                      'PRIMARY KEY (`exp`,`und`,`cp`,`strike`,`dt`)'\
                      ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_spots = 'CREATE TABLE `{0:s}` ('\
                       '`stk` char(6) NOT NULL,'\
                       '`dt` date NOT NULL,'\
                       '`spot` decimal(9, 2) NOT NULL,'\
                       'PRIMARY KEY (`stk`,`dt`)'\
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8'

    def __init__(self, in_dir='c:/goldendawn/options', opt_tbl='options',
                 spot_tbl='opt_spots'):
        self.in_dir = in_dir
        self.opt_tbl = opt_tbl
        self.spot_tbl = spot_tbl
        stxdb.db_create_missing_table(opt_tbl, self.sql_create_opts)
        stxdb.db_create_missing_table(spot_tbl, self.sql_create_spots)

    def load_opts(self, start_year, end_year):
        for year in range(start_year, end_year + 1):
            y_spots, y_opts = 0, 0
            for month in range(1, 13):
                zip_fname = '{0:s}/bb_{1:d}_{2:s}.zip'.\
                        format(self.in_dir, year, self.month_names[month])
                if os.path.exists(zip_fname):
                    m_spots, m_opts = self.load_opts_archive(zip_fname, year,
                                                             month)
                    y_spots += m_spots
                    y_opts += m_opts
            print('{0:s}\t{1:10d}\t{2:5d}\t{3:6d}'.
                  format(stxcal.print_current_time(), year, y_spots, y_opts))

    def load_opts_archive(self, zip_fname, year, month):
        # remove the tmp directory (if it already exists, and recreate it
        tmp_dir = '{0:s}/tmp'.format(self.in_dir)
        if os.path.exists(tmp_dir):
            rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        m_spots, m_opts = 0, 0
        with zipfile.ZipFile(zip_fname, 'r') as zip_file:
            zip_file.extractall(tmp_dir)
        daily_opt_files = os.listdir(tmp_dir)
        for opt_file in daily_opt_files:
            d_spots, d_opts = self.load_opts_daily(os.path.join(tmp_dir,
                                                                opt_file))
            m_spots += d_spots
            m_opts += d_opts
        # Remove the tmp directory after we are done with the current
        # archive
        rmtree(tmp_dir)
        print('{0:s}\t{1:7d}-{2:02d}\t{3:5d}\t{4:6d}'.format
              (stxcal.print_current_time(), year, month, m_spots, m_opts))
        return m_spots, m_opts

    def load_opts_daily(self, opt_fname):
        # print('{0:s} -load_opts_daily'.format(stxcal.print_current_time()))
        dt = '{0:s}-{1:s}-{2:s}'.format(opt_fname[-12:-8], opt_fname[-8:-6],
                                        opt_fname[-6:-4])
        sdt = datetime.strptime(dt, '%Y-%m-%d')
        edt = sdt + relativedelta(months=+7)
        sdt = str(sdt.date())[:-3]
        edt = str(edt.date())[:-3]
        exps = {exp: '' for exp in stxcal.expiries(sdt, edt)}
        spot_dct, opt_dct, spots, opts = {}, {}, [], []
        with open(opt_fname) as csvfile:
            frdr = csv.reader(csvfile)
            for row in frdr:
                stk = row[0]
                try:
                    spot = round(float(row[1]), 2)
                    cp = row[5][:1]
                    exp = str(datetime.strptime(row[6], '%m/%d/%Y').date())
                    strike = round(float(row[8]), 2)
                    bid = round(float(row[10]), 2)
                    ask = round(float(row[11]), 2)
                    volume = int(row[12])
                except:
                    continue
                if exp not in exps or ask == 0:
                    continue
                if stk not in spot_dct:
                    spot_dct[stk] = spot
                    spots.append([stk, dt, spot])
                opt_key = ':'.join([exp, stk, cp, str(strike)])
                if opt_key not in opt_dct:
                    opt_dct[opt_key] = [bid, ask, volume]
                    opts.append([exp, stk, cp, strike, dt, bid, ask, volume])
        spots_upload_file = '{0:s}/spots.txt'.format(self.upload_dir)
        opts_upload_file = '{0:s}/opts.txt'.format(self.upload_dir)
        with open(spots_upload_file, 'w') as spots_file:
            for s in spots:
                spots_file.write('{0:s}\t{1:s}\t{2:2f}\n'.
                                 format(s[0], s[1], s[2]))
        with open(opts_upload_file, 'w') as opts_file:
            for o in opts:
                opts_file.write('{0:s}\t{1:s}\t{2:s}\t{3:2f}\t{4:s}\t{5:2f}\t'
                                '{6:2f}\t{7:d}\n'.format(o[0], o[1], o[2],
                                                         o[3], o[4], o[5],
                                                         o[6], o[7]))
        stxdb.db_upload_file(spots_upload_file, self.spot_tbl, 2)
        stxdb.db_upload_file(opts_upload_file, self.opt_tbl, 5)
        d_spots, d_opts = len(spots), len(opts)
        print('{0:s}\t{1:s}\t{2:5d}\t{3:6d}'.format
              (stxcal.print_current_time(), dt, d_spots, d_opts))
        return d_spots, d_opts


if __name__ == '__main__':
    opt_eod = OptEOD(opt_tbl='opts', spot_tbl='opt_spots')
    opt_eod.load_opts(2002, 2003)
    # opt_eod.load_opts_archive('{0:s}/bb_2016_April.zip'.
    #                           format(opt_eod.in_dir), 2016, 4)
