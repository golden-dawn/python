import logging
import os
import stxcal
import stxdb


class StxEOD:
    data_dir = os.getenv('DATA_DIR')
    upload_dir = '/tmp'
    dload_dir = '{0:s}/Downloads'.format(data_dir)
    eod_name = '{0:s}/eod_upload.txt'.format(upload_dir)
    status_none = 0
    status_ok = 1
    status_ko = 2

    def __init__(self, in_dir, extension='.txt'):
        self.in_dir = in_dir
        self.eod_tbl = 'eods'
        self.rec_name = self.eod_tbl
        self.divi_tbl = 'dividends'

    def get_name(self, x):
        return {
            'ed': 'eoddata',
            'sq': 'stooq'
        }.get(x, 'final')

    def create_exchange(self):
        xchgs = stxdb.db_read_cmd("select * from exchanges where name='US'")
        if not xchgs:
            stxdb.db_write_cmd("insert into exchanges values('US')")
        db_stx = {x[0]: 0 for x in stxdb.db_read_cmd("select * from equities")}
        stx = {}
        return db_stx, stx

    # Load data from EODData data source in the database. There is a
    # daily file for each exchange (Amex, Nasdaq, NYSE). Use an
    # overlap of 5 days with the previous reconciliation interval
    # (covering up to 2012-12-31)
    def load_eoddata_files(self, sd, ed, stks=''):
        print('Loading eoddata files...')
        dt = sd
        # dt = stxcal.move_busdays(sd, -25)
        fnames = [os.path.join(self.in_dir, 'AMEX_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NASDAQ_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NYSE_{0:s}.txt')]
        while dt <= ed:
            print('eoddata: {0:s}'.format(dt))
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
        print('Loaded eoddata files')

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
                v = v // 1000
                if v == 0:
                    v = 1
                ofile.write('{0:s}\t0\n'.format('\t'.join(tokens)))

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
            v = v // 1000
            if v == 0:
                v = 1
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
            print('stooq: {0:s}'.format(dt))
            db_stx, _ = self.create_exchange()
            try:
                with open('{0:s}/{1:s}_d.prn'.format
                          (self.in_dir, dt.replace('-', ''))) as ifile:
                    lines = ifile.readlines()
            except IOError as ioe:
                print('{0:s} failed to read EOD file: {1:s}'.
                      format(dt, str(ioe)))
                dt = stxcal.next_busday(dt)
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


if __name__ == '__main__':
    logging.basicConfig(filename='stxeod.log', level=logging.INFO)
    s_date_ed = '2018-04-02'
    e_date_ed = '2018-11-23'
    s_date_sq = '2018-03-12'
    e_date_sq = '2018-03-29'
    seod = StxEOD('/home/cma/Downloads')
    seod.parseeodfiles(s_date_sq, e_date_sq)
    seod.load_eoddata_files(s_date_ed, e_date_ed)
