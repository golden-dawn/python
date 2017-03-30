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
        eod.parseeodfiles(self.sd, self.ed)
        eod.split_reconciliation('', self.sd, self.ed, ['split'])
        self.assertTrue(1 == 1)
