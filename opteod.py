import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from shutil import rmtree
import stxcal
import stxdb
import sys
import zipfile


class OptEOD:
    data_dir = os.getenv('DATA_DIR')
    upload_dir = '/tmp'
    options_dir = '{0:s}/options'.format(data_dir)
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April',
                   5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September',
                   10: 'October', 11: 'November', 12: 'December'}
    sql_create_opts = 'CREATE TABLE {0:s} ('\
                      'exp date NOT NULL,'\
                      'und varchar(8) NOT NULL,'\
                      'cp varchar(1) NOT NULL,'\
                      'strike decimal(10, 2) NOT NULL,'\
                      'dt date NOT NULL,'\
                      'bid decimal(10, 2) NOT NULL,'\
                      'ask decimal(10, 2) NOT NULL,'\
                      'volume integer NOT NULL,'\
                      'PRIMARY KEY (exp,und,cp,strike,dt)'\
                      ')'
    sql_create_spots = 'CREATE TABLE {0:s} ('\
                       'stk varchar(8) NOT NULL,'\
                       'dt date NOT NULL,'\
                       'spot decimal(10, 2) NOT NULL,'\
                       'PRIMARY KEY (stk,dt)'\
                       ')'

    def __init__(self, in_dir=None, opt_tbl='options', spot_tbl='opt_spots',
                 upload_options=True, upload_spots=True):
        self.in_dir = self.options_dir if in_dir is None else in_dir
        self.opt_tbl = opt_tbl
        self.spot_tbl = spot_tbl
        self.upload_spots = upload_spots
        self.upload_options = upload_options
        stxdb.db_create_missing_table(opt_tbl, self.sql_create_opts)
        stxdb.db_create_missing_table(spot_tbl, self.sql_create_spots)

    def load_opts(self, start_date, end_date):
        start_year, start_month = [int(x) for x in start_date.split('-')]
        end_year, end_month = [int(x) for x in end_date.split('-')]
        for year in range(start_year, end_year + 1):
            y_spots, y_opts = 0, 0
            for month in range(1, 13):
                if (year == start_year and month < start_month) or \
                   (year == end_year and month > end_month):
                    continue
                zip_fname = os.path.join(
                    self.in_dir,
                    'bb_{0:d}_{1:s}.zip'.format(year, self.month_names[month]))
                if os.path.exists(zip_fname):
                    m_spots, m_opts = self.load_opts_archive(zip_fname, year,
                                                             month)
                    y_spots += m_spots
                    y_opts += m_opts
            print('{0:s}\t{1:10d}\t{2:5d}\t{3:6d}'.
                  format(stxcal.print_current_time(), year, y_spots, y_opts))

    def load_opts_archive(self, zip_fname, year, month):
        # remove the tmp directory (if it already exists, and recreate it
        tmp_dir = '{0:s}/tmp'.format(self.data_dir)
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
        invalid_days = ['2002-02-01', '2002-02-04', '2002-02-05', '2002-02-06',
                        '2002-02-07', '2002-05-30', '2002-05-31', '2002-06-14',
                        '2002-06-17', '2002-12-02', '2002-12-03', '2002-12-04',
                        '2002-12-05', '2002-12-06', '2002-12-09', '2002-12-10']
        dt = '{0:s}-{1:s}-{2:s}'.format(opt_fname[-12:-8], opt_fname[-8:-6],
                                        opt_fname[-6:-4])
        if dt in invalid_days:
            return 0, 0
        sdt = datetime.strptime(dt, '%Y-%m-%d')
        edt = sdt + relativedelta(months=+7)
        sdt = str(sdt.date())[:-3]
        edt = str(edt.date())[:-3]
        exps = {exp: '' for exp in stxcal.expiries(sdt, edt)}
        spot_dct, opt_dct, spots, opts = {}, {}, [], []
        xchgs = stxdb.db_read_cmd("select * from exchanges where name='US'")
        if not xchgs:
            stxdb.db_write_cmd("insert into exchanges values('US')")
        insert_stx = 'INSERT INTO equities VALUES '
        db_stx = {x[0]: '' for x in stxdb.db_read_cmd(
            "select * from equities where exchange='US'")}
        stx = {}
        sep = ' '
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
                except Exception:
                    continue
                if exp not in exps or ask == 0:
                    continue
                if (stk not in db_stx) and (stk not in stx):
                    insert_stx = "{0:s}{1:s}('{2:s}', '', 'US Stocks', 'US')".\
                        format(insert_stx, sep, stk)
                    stx[stk] = ''
                    sep = ','
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
        if insert_stx != 'INSERT INTO equities VALUES ':
            stxdb.db_write_cmd(insert_stx)
        if self.upload_spots:
            stxdb.db_upload_file(spots_upload_file, self.spot_tbl, '\t')
        if self.upload_options:
            stxdb.db_upload_file(opts_upload_file, self.opt_tbl, '\t')
        d_spots, d_opts = len(spots), len(opts)
        print('{0:s}\t{1:s}\t{2:5d}\t{3:6d}'.format
              (stxcal.print_current_time(), dt, d_spots, d_opts))
        return d_spots, d_opts

    # These deltaneutral days contain garbage that sets off the reconciliation
    # process and trading simulations.  We are better off removing those days
    # from the opts and opt_spots tables
    def remove_invalid_days(self):
        invalid_days = "('2002-02-01', '2002-02-04', '2002-02-05', "\
                       "'2002-02-06', '2002-02-07', '2002-05-30', "\
                       "'2002-05-31', '2002-06-14', '2002-06-17', "\
                       "'2002-12-02', '2002-12-03', '2002-12-04', "\
                       "'2002-12-05', '2002-12-06', '2002-12-09', "\
                       "'2002-12-10')"
        print('{0:s}: removing invalid days from opt_spots and opts tables.'.
              format(stxcal.print_current_time()))
        stxdb.db_write_cmd('delete from {0:s} where dt in {1:s}'.
                           format(self.spot_tbl, invalid_days))
        print('{0:s}: removed invalid days from {1:s} table.'.
              format(stxcal.print_current_time(), self.spot_tbl))
        stxdb.db_write_cmd('delete from {0:s} where dt in {1:s}'.
                           format(self.opt_tbl, invalid_days))
        print('{0:s}: removed invalid days from {1:s} table.'.
              format(stxcal.print_current_time(), self.opt_tbl))


if __name__ == '__main__':
    upload_options = True
    upload_spots = True
    if '-no_spots' in sys.argv:
        upload_spots = False
    if '-no_options' in sys.argv:
        upload_options = False
    opt_eod = OptEOD(opt_tbl='options', spot_tbl='opt_spots',
                     upload_options=upload_options, upload_spots=upload_spots)
    opt_eod.load_opts('2002-02', '2009-03')
    # opt_eod.load_opts_archive('{0:s}/bb_2016_April.zip'.
    #                           format(opt_eod.in_dir), 2016, 4)
