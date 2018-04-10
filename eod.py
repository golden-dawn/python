from datetime import datetime
import stxcal
import stxdb
import unittest
from stxeod import StxEOD


class Test1EOD(unittest.TestCase):
    def setUp(self):
        self.eod_tbl = 'eod'
        self.split_tbl = 'split'
        self.ftr_tbl = 'ftr'
        eod = StxEOD(StxEOD.dload_dir, self.eod_tbl, self.split_tbl,
                     'reconciliation', self.ftr_tbl)
        sd = stxdb.db_read_cmd('select max(dt) from {0:s}'.
                               format(self.eod_tbl))
        self.sd = stxcal.next_busday(sd)
        self.ed = datetime.now().strftime('%Y-%m-%d')
        if not stxcal.is_busday(self.ed):
            self.ed = stxcal.prev_busday(self.ed)
        eod.parseeodfiles(self.sd, self.ed)
        eod.eod_reconciliation('', self.sd, self.ed)
        self.assertTrue(1 == 1)

import requests
r = requests.get('https://finance.yahoo.com/quote/GBTC/history?p=GBTC')
start_idx = r.text.find('HistoricalPriceStore') + 33
end_idx = r.text.find('isPending') - 2
with open('../../gbtc_test.txt', 'w') as f:
    f.write(r.text[start_idx: end_idx])
r = requests.get('https://stooq.com/q/d/l?s=C.F&d1=20180320&d2=20180402')
