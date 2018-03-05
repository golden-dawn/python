import datetime
from decimal import Decimal
import os
import unittest
import stxdb
from stxeod import StxEOD


class Test1StxEod(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.recon_tbl = 'reconciliation'
        cls.my_eod = StxEOD('/tmp', 'my', cls.recon_tbl)
        cls.dn_eod = StxEOD('/tmp', 'dn', cls.recon_tbl)
        cls.md_eod = StxEOD('/tmp', 'md', cls.recon_tbl)
        cls.ed_eod = StxEOD('/tmp', 'ed', cls.recon_tbl)
        cls.sq_eod = StxEOD('/tmp', 'sq', cls.recon_tbl)
        stx_test = os.getenv('TEST_STX')
        cls.test_stx = stx_test.replace('(', '').replace(')', '').replace(
            ' ', '').replace("'", '')
        cls.s_date = '2001-01-01'
        cls.s_date_spot = '2002-02-01'
        cls.e_date = '2012-12-31'
        cls.e_date_dn = '2016-12-30'
        cls.s_date_ed = '2013-01-02'
        cls.e_date_ed = '2013-11-15'
        cls.s_date_ae = '2013-11-18'
        cls.e_date_md = '2016-08-23'
        cls.s_date_sq = '2016-08-24'
        cls.e_date_sq = '2017-12-29'
        cls.eod_test = 'eods'
        cls.split_test = 'dividends'
        cls.stx = 'AEOS,EXPE,NFLX,TIE'
        cls.dn_stx = 'AEOS,EXPE,NFLX,TIE,VXX'
        cls.ed_stx = 'AA,EXPE,NFLX,VXX'
        cls.load_mypivots_splits()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_00_eod_name(self):
        eod = StxEOD('/tmp', '', self.recon_tbl)
        self.assertEqual(self.my_eod.name, 'my')
        self.assertEqual(self.dn_eod.name, 'deltaneutral')
        self.assertEqual(self.md_eod.name, 'marketdata')
        self.assertEqual(self.ed_eod.name, 'eoddata')
        self.assertEqual(self.sq_eod.name, 'stooq')
        self.assertEqual(eod.name, 'final')

    def test_01_loaded_data(self):
        res00 = stxdb.db_read_cmd('select count(*) from my_eods')
        res01 = stxdb.db_read_cmd('select count(*) from my_dividends')
        res10 = stxdb.db_read_cmd('select count(*) from dn_eods')
        res11 = stxdb.db_read_cmd('select count(*) from dn_dividends')
        res20 = stxdb.db_read_cmd('select count(*) from ed_eods')
        res21 = stxdb.db_read_cmd('select count(*) from ed_dividends')
        res30 = stxdb.db_read_cmd('select count(*) from md_eods')
        res31 = stxdb.db_read_cmd('select count(*) from md_dividends')
        res40 = stxdb.db_read_cmd('select count(*) from sq_eods')
        res41 = stxdb.db_read_cmd('select count(*) from sq_dividends')
        print('res10\n{0:s}'.format(res00))
        print('res11\n{0:s}'.format(res01))
        print('res10\n{0:s}'.format(res10))
        print('res11\n{0:s}'.format(res11))
        print('res10\n{0:s}'.format(res20))
        print('res11\n{0:s}'.format(res21))
        print('res10\n{0:s}'.format(res30))
        print('res11\n{0:s}'.format(res31))
        print('res10\n{0:s}'.format(res40))
        print('res11\n{0:s}'.format(res41))
        self.assertEqual(res00[0][0], 39794)
        self.assertEqual(res01[0][0], 32)
        self.assertEqual(res10[0][0], 21960)
        self.assertEqual(res11[0][0], 21)
        self.assertEqual(res20[0][0], 1113)
        self.assertEqual(res21[0][0], 0)
        self.assertEqual(res30[0][0], 35934)
        self.assertEqual(res31[0][0], 486)
        self.assertEqual(res40[0][0], 0)
        self.assertEqual(res41[0][0], 0)

    def test_02_reconcile_my_data(self):
        self.my_eod.reconcile_spots(self.s_date_spot, self.e_date,
                                    self.test_stx)
        res1 = stxdb.db_read_cmd("select * from {0:s} where "
                                 "recon_name='{1:s}' order by stk".
                                 format(self.recon_tbl, self.my_eod.rec_name))
        res2 = stxdb.db_read_cmd("select * from {0:s} where divi_type=1 order "
                                 "by stk, date".format(self.my_eod.divi_tbl))
        print('test_02_reconcile_my_data')
        print('res1')
        print(res1)
        print('res2')
        print(res2)
        self.assertEqual(res1[0][0], 'AEOS')
        self.assertEqual(res1[0][1], self.my_eod.rec_name)
        self.assertEqual(res1[0][2], '20020201_20121231')
        self.assertEqual(res1[0][3], '2002-02-08')
        self.assertEqual(res1[0][4], '2007-03-09')
        self.assertEqual(res1[0][5], '2002-02-04')
        self.assertEqual(res1[0][6], '2007-01-26')
        self.assertEqual(res1[0][7], 0)
        self.assertEqual(res1[0][8], 97.73)
        self.assertEqual(res1[0][9], 0.0016)
        self.assertEqual(res1[0][10], 0)
        self.assertTrue(res1[0] == ('AEOS', self.my_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2007-03-09', '2002-02-04',
                                    '2007-01-26', 0, 97.73, 0.0016, 0))
        self.assertTrue(res1[1] == ('EXPE', self.my_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2012-12-31', '2002-02-01',
                                    '2012-12-31', 1, 100.0, 0.0012, 0))
        self.assertTrue(res1[2] == ('NFLX', self.my_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-12-11', '2012-12-31', '2002-05-29',
                                    '2012-12-31', 0, 100.0, 0.0012, 0))
        self.assertTrue(res1[3] == ('TIE', self.my_eod.rec_name,
                                    '20020201_20121231',
                                    '2005-10-03', '2012-12-31', '2002-02-04',
                                    '2012-12-31', 2, 100.0, 0.0011, 0))
        self.assertTrue(res2[0] == ('EXPE', datetime.date(2003, 3, 10),
                                    Decimal('0.5000'), 1))
        self.assertTrue(res2[1] == ('TIE', datetime.date(2006, 2, 16),
                                    Decimal('0.5000'), 1))
        self.assertTrue(res2[2] == ('TIE', datetime.date(2006, 5, 15),
                                    Decimal('0.5000'), 1))

    def test_03_reconcile_dn_data(self):
        self.dn_eod.reconcile_spots(self.s_date_spot, self.e_date,
                                    self.test_stx)
        res1 = stxdb.db_read_cmd("select * from {0:s} where "
                                 "recon_name='{1:s}' order by stk".
                                 format(self.recon_tbl, self.dn_eod.rec_name))
        res2 = stxdb.db_read_cmd("select * from {0:s} where divi_type=1 order"
                                 " by stk, date".format(self.dn_eod.divi_tbl))
        print('test_03_reconcile_dn_data')
        print('res1')
        print(res1)
        print('res2')
        print(res2)
        self.assertTrue(res1[0] == ('AEOS', self.dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2007-03-09', '2002-02-04',
                                    '2007-03-09', 1, 100.0, 0.0017, 0) and
                        res1[1] == ('EXPE', self.dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2012-12-31', '2005-07-21',
                                    '2012-12-31', 0, 68.34, 0.0001, 0) and
                        res1[2] == ('NFLX', self.dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-12-11', '2012-12-31', '2002-05-23',
                                    '2012-12-31', 0, 100.0, 0.0012, 0) and
                        res1[3] == ('TIE', self.dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2005-10-03', '2012-12-31', '2002-02-01',
                                    '2012-12-31', 0, 100.0, 0.0041, 0) and
                        res2[0] == ('AEOS', datetime.date(2005, 3, 7),
                                    Decimal('0.4999'), 1))

    def test_04_split_recon(self):
        self.dn_eod.reconcile_big_changes('AEOS', self.s_date, self.e_date,
                                          [self.my_eod.divi_tbl])
        res = stxdb.db_read_cmd("select * from {0:s} where stk='{1:s}'"
                                .format(self.dn_eod.divi_tbl, 'AEOS'))
        print('res')
        print(res)
        self.assertTrue(
            res[0] == ('AEOS', datetime.date(2005, 3, 7), Decimal('0.4999'),
                       1) and
            res[1] == ('AEOS', datetime.date(2006, 12, 18), Decimal('0.6700'),
                       0))

    def test_05_merge_eod_tbls(self):
        self.my_eod.upload_eod(self.eod_test, self.split_test, self.test_stx,
                               self.s_date_spot, self.e_date)
        self.dn_eod.upload_eod(self.eod_test, self.split_test, self.test_stx,
                               self.s_date_spot, self.e_date)
        res1 = stxdb.db_read_cmd("select * from {0:s} where stk='EXPE' and "
                                 "date between '2003-03-10' and '2003-03-11'".
                                 format(self.eod_test))
        res2 = stxdb.db_read_cmd("select * from {0:s} where stk='TIE' and "
                                 "date between '2006-02-16' and '2006-02-17'".
                                 format(self.eod_test))
        res3 = stxdb.db_read_cmd("select * from {0:s} where stk='TIE' and "
                                 "date between '2006-05-15' and '2006-05-16'".
                                 format(self.eod_test))
        res4 = stxdb.db_read_cmd("select * from {0:s} where stk='EXPE' and "
                                 "date between '2003-08-08' and '2005-07-21'".
                                 format(self.eod_test))
        res5 = stxdb.db_read_cmd('select stk, count(*) from {0:s} group by '
                                 'stk order by stk'.format(self.split_test))
        print('test_05_merge_eod_tbls')
        print('res1')
        print(res1)
        print('res2')
        print(res2)
        print('res3')
        print(res3)
        print('res4')
        print(res4)
        print('res5')
        print(res5)
        self.assertTrue(res1[0][2] == Decimal('69.06') and
                        res1[0][3] == Decimal('69.38') and
                        res1[0][4] == Decimal('68.40') and
                        res1[0][5] == Decimal('68.66') and
                        res1[0][6] == 922950 and
                        res1[1][2] == Decimal('33.76') and
                        res1[1][3] == Decimal('34.20') and
                        res1[1][4] == Decimal('33.32') and
                        res1[1][5] == Decimal('33.78') and
                        res1[1][6] == 4107600 and
                        res2[0][2] == Decimal('72.36') and
                        res2[0][3] == Decimal('74.16') and
                        res2[0][4] == Decimal('71.28') and
                        res2[0][5] == Decimal('73.80') and
                        res2[0][6] == 1886800 and
                        res2[1][2] == Decimal('37.12') and
                        res2[1][3] == Decimal('38.00') and
                        res2[1][4] == Decimal('37.02') and
                        res2[1][5] == Decimal('37.56') and
                        res2[1][6] == 1439500 and
                        res3[0][2] == Decimal('77.12') and
                        res3[0][3] == Decimal('77.48') and
                        res3[0][4] == Decimal('69.60') and
                        res3[0][5] == Decimal('72.12') and
                        res3[0][6] == 22097800 and
                        res3[1][2] == Decimal('36.50') and
                        res3[1][3] == Decimal('38.50') and
                        res3[1][4] == Decimal('34.80') and
                        res3[1][5] == Decimal('37.96') and
                        res3[1][6] == 11771300 and
                        res4[0][1] == datetime.date(2003, 8, 8) and
                        res4[1][1] == datetime.date(2005, 7, 21) and
                        res5[0] == ('AA', 1) and
                        res5[1] == ('AEOS', 6) and
                        res5[2] == ('EXPE', 1) and
                        res5[3] == ('KO', 1) and
                        res5[4] == ('NFLX', 2) and
                        res5[5] == ('TIE', 4) and
                        res5[6] == ('VXX', 2))

    def test_06_reconcile_ed_data(self):
        self.ed_eod.reconcile_spots(self.s_date_ed, self.e_date_ed,
                                    self.test_stx)
        res1 = stxdb.db_read_cmd("select * from {0:s} where "
                                 "recon_name='{1:s}' order by stk".
                                 format(self.recon_tbl, self.ed_eod.rec_name))
        res2 = stxdb.db_read_cmd("select * from {0:s} where divi_type=1 order"
                                 " by stk, date".format(self.ed_eod.divi_tbl))
        self.assertTrue(res1[0] == ('AA', self.ed_eod.rec_name,
                                    '20130102_20131115',
                                    '2013-01-02', '2013-11-15', '2013-01-02',
                                    '2013-11-15', 0, 100.0, 0.0011, 0) and
                        res1[1] == ('EXPE', self.ed_eod.rec_name,
                                    '20130102_20131115',
                                    '2013-01-02', '2013-11-15', '2013-01-02',
                                    '2013-11-15', 0, 100.0, 0.0005, 0) and
                        res1[2] == ('NFLX', self.ed_eod.rec_name,
                                    '20130102_20131115',
                                    '2013-01-02', '2013-11-15', '2013-01-02',
                                    '2013-11-15', 0, 100.0, 0.001, 0) and
                        res1[3] == ('VXX', self.ed_eod.rec_name,
                                    '20130102_20131115',
                                    '2013-01-02', '2013-11-15', '2013-01-02',
                                    '2013-11-15', 0, 100.0, 0.0017, 0) and
                        len(res2) == 0)

    def test_07_split_recon(self):
        stk_list = self.test_stx.split(',')
        for stk in stk_list:
            self.ed_eod.reconcile_big_changes(
                stk, self.s_date_ed, self.e_date_ed,
                ['dividends', 'dn_dividends'])
        res = stxdb.db_read_cmd("select * from {0:s}".
                                format(self.ed_eod.divi_tbl))
        print('res')
        print(res)
        self.assertTrue(res[0] == ('VXX', datetime.date(2013, 11, 7),
                                   Decimal('4.0000'), 0))

    def test_08_merge_eod_tbls(self):
        self.my_eod.upload_eod(self.eod_test, self.split_test, self.stx,
                               self.s_date_ed, self.e_date_ed)
        self.dn_eod.upload_eod(self.eod_test, self.split_test, self.stx,
                               self.s_date_ed, self.e_date_ed)
        self.md_eod.upload_eod(self.eod_test, self.split_test, 'VXX',
                               self.s_date_ed, self.e_date_ed)
        self.ed_eod.upload_eod(self.eod_test, self.split_test, self.ed_stx,
                               self.s_date_ed, self.e_date_ed)
        res1 = stxdb.db_read_cmd("select * from {0:s} where stk='EXPE' and "
                                 "date between '2003-03-10' and '2003-03-11'".
                                 format(self.eod_test))
        res2 = stxdb.db_read_cmd("select * from {0:s} where stk='TIE' and "
                                 "date between '2006-02-16' and '2006-02-17'".
                                 format(self.eod_test))
        res3 = stxdb.db_read_cmd("select * from {0:s} where stk='TIE' and "
                                 "date between '2006-05-15' and '2006-05-16'".
                                 format(self.eod_test))
        res4 = stxdb.db_read_cmd("select * from {0:s} where stk='EXPE' and "
                                 "date between '2003-08-08' and '2005-07-21'".
                                 format(self.eod_test))
        res5 = stxdb.db_read_cmd('select stk, count(*) from {0:s} group by '
                                 'stk order by stk'.format(self.split_test))
        print('test_08_merge_eod_tbls')
        print('res1')
        print(res1)
        print('res2')
        print(res2)
        print('res3')
        print(res3)
        print('res4')
        print(res4)
        print('res5')
        print(res5)
        self.assertTrue(res1[0][2] == Decimal('69.06') and
                        res1[0][3] == Decimal('69.38') and
                        res1[0][4] == Decimal('68.40') and
                        res1[0][5] == Decimal('68.66') and
                        res1[0][6] == 922950 and
                        res1[1][2] == Decimal('33.76') and
                        res1[1][3] == Decimal('34.20') and
                        res1[1][4] == Decimal('33.32') and
                        res1[1][5] == Decimal('33.78') and
                        res1[1][6] == 4107600 and
                        res2[0][2] == Decimal('72.36') and
                        res2[0][3] == Decimal('74.16') and
                        res2[0][4] == Decimal('71.28') and
                        res2[0][5] == Decimal('73.80') and
                        res2[0][6] == 1886800 and
                        res2[1][2] == Decimal('37.12') and
                        res2[1][3] == Decimal('38.00') and
                        res2[1][4] == Decimal('37.02') and
                        res2[1][5] == Decimal('37.56') and
                        res2[1][6] == 1439500 and
                        res3[0][2] == Decimal('77.12') and
                        res3[0][3] == Decimal('77.48') and
                        res3[0][4] == Decimal('69.60') and
                        res3[0][5] == Decimal('72.12') and
                        res3[0][6] == 22097800 and
                        res3[1][2] == Decimal('36.50') and
                        res3[1][3] == Decimal('38.50') and
                        res3[1][4] == Decimal('34.80') and
                        res3[1][5] == Decimal('37.96') and
                        res3[1][6] == 11771300 and
                        res4[0][1] == datetime.date(2003, 8, 8) and
                        res4[1][1] == datetime.date(2005, 7, 21) and
                        res5[0] == ('AA', 1) and
                        res5[1] == ('AEOS', 6) and
                        res5[2] == ('EXPE', 1) and
                        res5[3] == ('KO', 1) and
                        res5[4] == ('NFLX', 2) and
                        res5[5] == ('TIE', 4) and
                        res5[6] == ('VXX', 2))

    @classmethod
    def load_mypivots_splits(cls, fname='/home/cma/mypivots_splits.csv'):
        with open(fname, 'r') as f:
            lines = f.readlines()
        print('self.test_stx={0:s}'.format(cls.test_stx))
        stx_lst = cls.test_stx.replace('(', '').replace(')', '').replace(
            ' ', '').replace("'", '')
        stx_lst = stx_lst.split(',')
        for line in lines:
            tokens = line.split()
            if tokens[0] in stx_lst:
                stxdb.db_write_cmd("insert into dividends values "
                                   "('{0:s}', '{1:s}', {2:.4f}, 0)".format(
                                    tokens[0], tokens[1], float(tokens[2])))


if __name__ == '__main__':
    unittest.main()
