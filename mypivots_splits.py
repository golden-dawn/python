from datetime import datetime
import requests
import stxdb


class MypivotsSplits:

    fname = '/home/cma/mypivots_splits.csv'
    prefix = '<td><a style="padding-right:10px" href="/stock-split-calendar/' \
        'ticker/'
    url_base = 'http://www.mypivots.com'
    start_row = '<tr style="text-align:center">'
    end_row = '</tr>'

    def get_all(self):
        r1 = requests.get('{0:s}/stock-split-calendar/ticker'.
                          format(self.url_base))
        lines = r1.text.split('\n')
        split_urls = [l[40:-9] for l in lines if l.startswith(self.prefix)]
        split_urls = [x[:x.find('"')] for x in split_urls]
        with open(self.fname, 'w') as f:
            f.write('Ticker,Date,Ratio\n')
        for split_url in split_urls:
            r2 = requests.get('{0:s}{1:s}'.format(self.url_base, split_url))
            if r2.status_code != 200:
                print('Failed to get {0:s}, error_code: {1:d}'.
                      format(split_url, r2.status_code))
                continue
            tokens = split_url.split('/')
            ticker = tokens[3].strip().upper()
            lines = r2.text.split('\n')
            self.store_splits(ticker, lines)

    def store_splits(self, ticker, lines):
        with open(self.fname, 'a') as ofile:
            started = False
            str_buffer = ''
            for line in lines:
                line = line.strip()
                if not (started or line == self.start_row):
                    continue
                if line == self.start_row:
                    str_buffer = ''
                    started = True
                elif line == self.end_row:
                    tokens = str_buffer[4:-5].split('</td><td>')
                    split_date = str(datetime.strptime(tokens[1],
                                                       '%m/%d/%Y').date())
                    denom, num = tokens[3].split('-')
                    ratio = float(num) / float(denom)
                    ofile.write('{0:s}\t{1:s}\t{2:f}\n'.
                                format(ticker, split_date, ratio))
                    started = False
                else:
                    str_buffer += line.strip()

    def upload_splits(self):
        with open(self.fname, 'r') as f:
            lines = f.readlines()
        lines = list(set(lines))
        dct = {}
        for l in lines:
            tokens = l.split('\t')
            ticker = tokens[0].strip()
            dct[ticker] = ''
        db_stx = {x[0]: 0 for x in stxdb.db_read_cmd("select * from equities")}
        print('len(dct) = {0:d}, len(db_stx) = {1:d}'.
              format(len(dct), len(db_stx)))
        for stk in dct:
            if stk not in db_stx:
                insert_stx = "INSERT INTO equities VALUES "\
                    "('{0:s}', '', 'US Stocks', 'US')".format(stk)
                print('insert_stx = {0:s}'.format(insert_stx))
                stxdb.db_write_cmd(insert_stx)
        try:
            stxdb.db_upload_file(self.fname, 'dividends', '\t')
        except Exception as ex:
            print('Failed to upload {0:s}: {1:s}'.format(self.fname, str(ex)))


if __name__ == '__main__':
    mps = MypivotsSplits()
    mps.get_all()
