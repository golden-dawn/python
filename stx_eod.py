import argparse
import datetime
import json
import logging
import os
import requests
import stxcal
import stxdb
import sys


class StxEOD:
    upload_dir = '/tmp'
    eod_name = os.path.join(upload_dir, 'eod_upload.txt')
    status_none = 0
    status_ok = 1
    status_ko = 2
    yhoo_url = 'https://query1.finance.yahoo.com/v7/finance/options/{0:s}?' \
               'formatted=true&crumb=BfPVqc7QhCQ&lang=en-US&region=US&' \
               'date={1:d}&corsDomain=finance.yahoo.com'

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

    # def create_exchange(self):
    #     xchgs = stxdb.db_read_cmd("select * from exchanges where name='US'")
    #     if not xchgs:
    #         stxdb.db_write_cmd("insert into exchanges values('US')")
    #  db_stx = {x[0]: 0 for x in stxdb.db_read_cmd("select * from equities")}
    #     stx = {}
    #     return db_stx, stx

    # Load data from EODData data source in the database. There is a
    # daily file for each exchange (Amex, Nasdaq, NYSE). Use an
    # overlap of 5 days with the previous reconciliation interval
    # (covering up to 2012-12-31)
    def load_eoddata_files(self, sd, ed, stks='', batch=False):
        logging.info('Loading eoddata files...')
        dt = sd
        # dt = stxcal.move_busdays(sd, -25)
        fnames = [os.path.join(self.in_dir, 'AMEX_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NASDAQ_{0:s}.txt'),
                  os.path.join(self.in_dir, 'NYSE_{0:s}.txt')]
        while dt <= ed:
            logging.info('eoddata: {0:s}'.format(dt))
            data_available = True
            dtc = dt.replace('-', '')
            for fname in fnames:
                fname_dt = fname.format(dtc)
                if not os.path.isfile(fname_dt):
                    logging.error('Could not find file {0:s}'.format(fname_dt))
                    data_available = False
            if not data_available:
                logging.error('Data is missing for date {0:s}. Exiting.'.
                              format(dt))
                return
            for fname in fnames:
                fname_dt = fname.format(dtc)
                self.load_eoddata_file(fname_dt, dt, dtc, stks, batch=batch)
            stxdb.db_write_cmd("update analyses set dt='{0:s}' where "
                               "analysis='eod_datafeed'".format(dt))
            dt = stxcal.next_busday(dt)
        logging.info('Loaded eoddata files')

    # Load data from a single EODData file in the database Perform
    # some quality checks on the data: do not upload days where volume
    # is 0, or where the open/close are outside the [low, high] range.
    def load_eoddata_file(self, ifname, dt, dtc, stks='', batch=False):
        upload_lines = []
        stk_list = [] if stks == '' else stks.split(',')
        # db_stx, _ = self.create_exchange()
        with open(ifname, 'r') as ifile:
            lines = ifile.readlines()
        for line in lines[1:]:
            tokens = line.replace(dtc, dt).strip().split(',')
            stk = tokens[0].strip()
            if (stk_list and stk not in stk_list) or ('/' in stk) or \
               ('*' in stk) or (stk in ['AUX', 'PRN']):
                continue
            # if stk not in db_stx:
            #     insert_stx = "INSERT INTO equities VALUES "\
            #                  "('{0:s}', '', 'US Stocks', 'US')".format(stk)
            #     stxdb.db_write_cmd(insert_stx)
            o = int(100 * float(tokens[2]))
            hi = int(100 * float(tokens[3]))
            lo = int(100 * float(tokens[4]))
            c = int(100 * float(tokens[5]))
            v = int(tokens[6])
            if v == 0 or o < lo or o > hi or c < lo or c > hi or \
               len(tokens[0]) > 6 or o >= 2147483647 or hi >= 2147483647 \
               or lo >= 2147483647 or c >= 2147483647:
                continue
            v = v // 1000
            if v == 0:
                v = 1
            if batch:
                upload_lines.append(
                    '{0:s}\t{1:s}\t{2:d}\t{3:d}\t{4:d}\t{5:d}\t{6:d}\t0\n'.
                    format(stk, dt, o, hi, lo, c, v))
            else:
                upload_lines.append([stk, dt, o, hi, lo, c, v, 0])
        if batch:
            with open(self.eod_name, 'w') as f:
                for line in upload_lines:
                    f.write(line)
            stxdb.db_upload_file(self.eod_name, self.eod_tbl, '\t')
        else:
            stxdb.db_insert_eods(upload_lines)

    def parseeodline(self, line):
        stk, _, dt, o, h, l, c, v, oi = line.split(',')
        # look only at the US stocks, for the time being
        if not stk.endswith('.US'):
            return
        dt = '{0:s}-{1:s}-{2:s}'.format(dt[0:4], dt[4:6], dt[6:8])
        if not stxcal.is_busday(dt):
            raise Exception('{0:s} is not a business day'.format(dt))
        o = int(100 * float(o))
        h = int(100 * float(h))
        l = int(100 * float(l))
        c = int(100 * float(c))
        # Make sure o and c are in the interval [l, h]
        o = o if o <= h and o >= l else (h if o > h else l)
        c = c if c <= h and c >= l else (h if c > h else l)
        if o >= 2147483647 or h >= 2147483647 or l >= 2147483647 or \
           c >= 2147483647:
            return
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
        # if stk not in db_stx:
        #     insert_stx = "INSERT INTO equities VALUES "\
        #                  "('{0:s}', '', 'US Stocks', 'US')".format(stk)
        #     stxdb.db_write_cmd(insert_stx)
        db_cmd = "insert into {0:s} values('{1:s}','{2:s}',{3:d},{4:d},"\
            "{5:d},{6:d},{7:d},{8:d}) on conflict (stk, dt) do update "\
            "set v={9:d}, oi={10:d}".format(
                self.eod_tbl, stk, dt, o, h, l, c, v, oi, v, oi)
        stxdb.db_write_cmd(db_cmd)

    def parseeodfiles(self, s_date, e_date):
        dt = s_date
        num_days = stxcal.num_busdays(s_date, e_date)
        logging.info('Stooq: uploading EOD data for {0:d} days'.
                     format(num_days))
        day_num = 0
        while dt <= e_date:
            logging.info('stooq: {0:s}'.format(dt))
            # db_stx, _ = self.create_exchange()
            try:
                with open('{0:s}/{1:s}_d.prn'.format
                          (self.in_dir, dt.replace('-', ''))) as ifile:
                    lines = ifile.readlines()
            except IOError as ioe:
                logging.error('{0:s} failed to read EOD file: {1:s}'.
                              format(dt, str(ioe)))
                dt = stxcal.next_busday(dt)
                continue
            for line in lines:
                try:
                    self.parseeodline(line)
                except Exception as ex:
                    logging.info('Error with line {0:s}: {1:s}'.format
                                 (line.strip(), str(ex)))
            dt = stxcal.next_busday(dt)
            day_num += 1
            if day_num % 20 == 0 or day_num == num_days:
                logging.info(' Uploaded EOD data for {0:d} days'.
                             format(day_num))

    def handle_splits(self, start_date):
        split_files = [x for x in os.listdir(self.in_dir)
                       if x.startswith('splits_')]
        split_files.sort()
        valid_prefixes = ['NYSE', 'NASDAQ', 'AMEX']
        split_dct = {}
        for split_file in split_files:
            with open(os.path.join(self.in_dir, split_file), 'r') as f:
                lines = reversed(f.readlines())
            for line in lines:
                tokens = line.strip().split()
                if len(tokens) < 4:
                    continue
                if tokens[0] not in valid_prefixes:
                    continue
                stk = tokens[1].strip()
                stk_splits = split_dct.get(stk, {})
                dt = str(datetime.datetime.strptime(tokens[2].strip(),
                                                    '%m/%d/%Y').date())
                if dt < start_date:
                    continue
                denominator, nominator = tokens[3].strip().split('-')
                split_ratio = float(nominator) / float(denominator)
                for split_date in stk_splits.keys():
                    if abs(stxcal.num_busdays(split_date, dt)) < 5:
                        del stk_splits[split_date]
                stk_splits[dt] = split_ratio
                split_dct[stk] = stk_splits
        for stk, stk_splits in split_dct.items():
            for dt, ratio in stk_splits.items():
                print('{0:s} {1:s} {2:f}'.format(stk, dt, ratio))

    def upload_splits(self, splits_file):
        logging.info('Uploading stocks from file {0:s}'.format(splits_file))
        with open(splits_file, 'r') as f:
            lines = f.readlines()
        num = 0
        for line in lines:
            tokens = line.split()
            if len(tokens) < 3:
                logging.warn('Skipping line {0:s}'.format(line))
                continue
            stk = tokens[0].strip()
            dt = stxcal.prev_busday(tokens[1].strip())
            ratio = float(tokens[2].strip())
            db_cmd = "insert into {0:s} values('{1:s}','{2:s}',{3:f},0) "\
                "on conflict (stk, dt) do update set ratio={4:f}".format(
                self.divi_tbl, stk, dt, ratio, ratio)
            try:
                stxdb.db_write_cmd(db_cmd)
                num += 1
            except Exception as ex:
                logging.error('Failed to upload split {0:s}, {1:s}, '
                              'error {2:s}'.format(stk, dt, str(ex)))
        logging.info('Successfully uploaded {0:d} out of {1:d} stock splits'.
                     format(num, len(lines)))

    def get_indices(self):
        indices = {'^DJI': '^DJI', '^SPX': '^SPX', '^NDQ': '^COMP'}
        end_date = stxcal.move_busdays(str(datetime.datetime.now().date()), 0)
        for stooq_name, db_name in indices.items():
            db_cmd = "select max(date) from eods where stk='{0:s}' and " \
                     "volume > 1".format(db_name)
            res_db = stxdb.db_read_cmd(db_cmd)
            start_date = stxcal.next_busday(str(res_db[0][0]))
            res = requests.get(
                'https://stooq.com/q/d/l?s={0:s}&d1={1:s}&d2={2:s}'.
                format(stooq_name,
                       start_date.replace('-', ''),
                       end_date.replace('-', '')))
            lines = res.text.split('\n')
            for line in lines[1:]:
                tokens = line.split(',')
            with open('{0:s}.csv'.format(db_name), 'w') as f:
                f.write(res.text)
            logging.info('{0:s}: {1:d}'.format(stooq_name, res.status_code))

    def get_data(self, stk):
        expiries = stxcal.long_expiries()
        res = requests.get(self.yhoo_url.format(stk, expiries[0]))
        logging.info('Got data for {0:s}, status code: {1:d}'.
                     format(stk, res.status_code))
        res_json = json.loads(res.text)
        res_0 = res_json['optionChain']['result'][0]
        quote = res_0['quote']
        v = quote['regularMarketVolume']
        o = quote['regularMarketOpen']
        c = quote['regularMarketPrice']
        hi = quote['regularMarketDayHigh']
        lo = quote['regularMarketDayLow']
        dt = datetime.datetime.fromtimestamp(quote['regularMarketTime'])
        print('{0:s} {1:s} {2:.2f} {3:.2f} {4:.2f} {5:.2f} {6:d}'.
              format(stk, str(dt.date()), o, hi, lo, c, v))
        calls = res_0['options'][0]['calls']
        puts = res_0['options'][0]['puts']
        for call in calls:
            ask = call['ask']['raw']
            bid = call['bid']['raw']
            strike = call['strike']['raw']
            cp = 'C'
            exp = call['expiration']['fmt']
            print('  {0:s} {1:s} {2:.2f} {3:.2f} {4:.2f}'
                  .format(cp, exp, strike, bid, ask)) 
        for put in puts:
            ask = put['ask']['raw']
            bid = put['bid']['raw']
            strike = put['strike']['raw']
            cp = 'P'
            exp = put['expiration']['fmt']
            print('  {0:s} {1:s} {2:.2f} {3:.2f} {4:.2f}'
                  .format(cp, exp, strike, bid, ask)) 


# Wake up every day at 10:00PM
# If this is an end-of-month option expiry date, generate a new cache
#  - Check that the latest EOD files have been downloaded for the day.
#  - If the EOD files are not there yet, print an error message,
#    sleep for an hour, check for the files again
# If the day is not an expiry day, then download EOD data and options from www
# Create several database tables:
# - leaders
# - eod_cache
# - options_cache
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--stooq', action='store_true',
                        help='Use data from stooq')
    parser.add_argument('-b', '--batch', action='store_true',
                        help='Batch upload of data using file copy')
    args = parser.parse_args()
    data_dir = os.getenv('DOWNLOAD_DIR')
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    seod = StxEOD(data_dir)
    if args.stooq:
        s_date_sq = '2018-03-12'
        e_date_sq = '2018-03-29'
        seod.parseeodfiles(s_date_sq, e_date_sq)
        sys.exit(0)

    # Handle default EODData stream
    # 1. Get the last date for which eod data is available in the database
    # res = stxdb.db_read_cmd("select max(dt) from eods where oi=0")
    res = stxdb.db_read_cmd("select dt from analyses where "
                            "analysis='eod_datafeed'")
    start_date = stxcal.next_busday(str(res[0][0]))
    # 2. get the last trading date
    end_date = stxcal.move_busdays(str(datetime.datetime.now().date()), 0)
    # 3. Find out if files are available for the dates
    # 4. if the files are available, upload them
    batch_load = True if args.batch else False
    seod.load_eoddata_files(start_date, end_date, batch=batch_load)
    res = stxdb.db_read_cmd("select max(dt) from dividends")
    start_date = stxcal.next_busday(str(res[0][0]))
    seod.handle_splits(start_date)
