import datetime
from datetime import date
from decimal import Decimal
# from opteod import OptEOD
import os
import unittest
from shutil import copyfile
import stxcal
import stxdb
from stxeod import StxEOD
# from stxts import StxTS


class Test1StxDb(unittest.TestCase):
    def setUp(self):
        self.tbl_name = 'eod_test'
        self.sql_create_tbl = "CREATE TABLE {0:s} ("\
                              "stk varchar(8) NOT NULL,"\
                              "dt date NOT NULL,"\
                              "o decimal(9,2) DEFAULT NULL,"\
                              "hi decimal(9,2) DEFAULT NULL,"\
                              "lo decimal(9,2) DEFAULT NULL,"\
                              "c decimal(9,2) DEFAULT NULL,"\
                              "v integer DEFAULT NULL,"\
                              "PRIMARY KEY (stk,dt)"\
                              ")".format(self.tbl_name)
        self.sql_tbls = "SELECT table_name FROM information_schema.tables "\
                        "WHERE table_schema='public' AND table_name='{0:s}'".\
                        format(self.tbl_name)
        self.sql_select = 'select * from {0:s}'.format(self.tbl_name)
        self.sql_drop_tbl = 'drop table {0:s}'.format(self.tbl_name)
        self.bulk_data = '''A,2000-01-01,33.00,34.00,33.00,34.00,1000
B,2000-01-01,33.00,34.00,33.00,34.00,1000
C,2000-01-01,33.00,34.00,33.00,34.00,1000
A,2000-01-01,33.00,34.00,33.00,34.00,1000
A,2000-01-02,33.00,34.00,33.00,34.00,1000
'''
        self.file_name = '/tmp/test.txt'

    def tearDown(self):
        pass

    def test_1_create_missing_table(self):
        res0 = stxdb.db_read_cmd(self.sql_tbls)
        stxdb.db_create_missing_table(self.tbl_name, self.sql_create_tbl)
        res1 = stxdb.db_read_cmd(self.sql_tbls)
        print('res1 = {0:s}'.format(res1))
        print('res1[0][0] = {0:s}'.format(res1[0][0]))
        self.assertEqual(len(res0), 0)
        self.assertEqual(len(res1), 1)
        self.assertEqual(res1[0][0], self.tbl_name)

    def test_2_get_table_columns(self):
        res = stxdb.db_get_table_columns(self.tbl_name)
        print('res = {0:s}'.format(res))
        self.assertEqual(len(res), 7)
        self.assertEqual(res[0][0], 'stk')
        self.assertEqual(res[0][1], 'varchar')
        self.assertEqual(res[0][2], 8)
        self.assertEqual(res[1][0], 'dt')
        self.assertEqual(res[1][1], 'date')
        self.assertEqual(res[2][0], 'o')
        self.assertEqual(res[2][1], 'numeric')
        self.assertEqual(res[2][3], 9)
        self.assertEqual(res[2][4], 2)
        self.assertEqual(res[3][0], 'hi')
        self.assertEqual(res[3][1], 'numeric')
        self.assertEqual(res[3][3], 9)
        self.assertEqual(res[3][4], 2)
        self.assertEqual(res[4][0], 'lo')
        self.assertEqual(res[4][1], 'numeric')
        self.assertEqual(res[4][3], 9)
        self.assertEqual(res[4][4], 2)
        self.assertEqual(res[5][0], 'c')
        self.assertEqual(res[5][1], 'numeric')
        self.assertEqual(res[5][3], 9)
        self.assertEqual(res[5][4], 2)
        self.assertEqual(res[6][0], 'v')
        self.assertEqual(res[6][1], 'int4')

    def test_3_get_key_len(self):
        res = stxdb.db_get_key_len(self.tbl_name)
        self.assertEqual(res, 2)

    def test_4_bulk_upload(self):
        with open(self.file_name, 'w') as f:
            f.write(self.bulk_data)
        stxdb.db_upload_file(self.file_name, self.tbl_name, ',')
        res = stxdb.db_read_cmd(self.sql_select)
        self.assertEqual(len(res), 4)

    def test_5_drop_table(self):
        stxdb.db_write_cmd(self.sql_drop_tbl)
        res = stxdb.db_read_cmd(self.sql_tbls)
        self.assertEqual(len(res), 0)

    def test_6_create_table_like(self):
        tbl_name = 'eods'
        new_tbl_name = 'eods2'
        sql_new_tbl = "SELECT table_name FROM information_schema.tables "\
                      "WHERE table_schema='public' AND table_name='{0:s}'".\
                      format(new_tbl_name)
        res0 = stxdb.db_read_cmd(sql_new_tbl)
        stxdb.db_create_table_like(tbl_name, new_tbl_name)
        res1 = stxdb.db_read_cmd(sql_new_tbl)
        print('res1 = {0:s}'.format(res1))
        print('res1[0][0] = {0:s}'.format(res1[0][0]))
        self.assertEqual(len(res0), 0)
        self.assertEqual(len(res1), 1)
        self.assertEqual(res1[0][0], new_tbl_name)
        res = stxdb.db_get_table_columns(new_tbl_name)
        print('res = {0:s}'.format(res))
        self.assertEqual(len(res), 8)
        self.assertEqual(res[0][0], 'stk')
        self.assertEqual(res[0][1], 'varchar')
        self.assertEqual(res[0][2], 8)
        self.assertEqual(res[1][0], 'date')
        self.assertEqual(res[1][1], 'date')
        self.assertEqual(res[2][0], 'o')
        self.assertEqual(res[2][1], 'numeric')
        self.assertEqual(res[2][3], 10)
        self.assertEqual(res[2][4], 2)
        self.assertEqual(res[3][0], 'hi')
        self.assertEqual(res[3][1], 'numeric')
        self.assertEqual(res[3][3], 10)
        self.assertEqual(res[3][4], 2)
        self.assertEqual(res[4][0], 'lo')
        self.assertEqual(res[4][1], 'numeric')
        self.assertEqual(res[4][3], 10)
        self.assertEqual(res[4][4], 2)
        self.assertEqual(res[5][0], 'c')
        self.assertEqual(res[5][1], 'numeric')
        self.assertEqual(res[5][3], 10)
        self.assertEqual(res[5][4], 2)
        self.assertEqual(res[6][0], 'volume')
        self.assertEqual(res[6][1], 'int4')
        self.assertEqual(res[7][0], 'open_interest')
        self.assertEqual(res[7][1], 'int4')


class Test2StxCal(unittest.TestCase):

    def setUp(self):
        self.dt1 = '2016-12-15'
        self.dt2 = '2016-12-16'
        self.dt3 = '2016-12-18'
        self.dt4 = '2016-12-23'
        self.dt5 = '2016-12-27'
        self.dt6 = '2014-07-04'

    def tearDown(self):
        pass

    def test_1_next_busday(self):
        res1 = stxcal.next_busday(self.dt1)
        res2 = stxcal.next_busday(self.dt2)
        res3 = stxcal.next_busday(self.dt3)
        res4 = stxcal.next_busday(self.dt4)
        self.assertTrue((res1 == '2016-12-16') and (res2 == '2016-12-19') and
                        (res3 == '2016-12-19') and (res4 == '2016-12-27'))

    def test_2_prev_busday(self):
        res1 = stxcal.prev_busday(self.dt1)
        res2 = stxcal.prev_busday(self.dt2)
        res3 = stxcal.prev_busday(self.dt3)
        res5 = stxcal.prev_busday(self.dt5)
        self.assertTrue((res1 == '2016-12-14') and (res2 == '2016-12-15') and
                        (res3 == '2016-12-16') and (res5 == '2016-12-23'))

    def test_3_is_busday(self):
        self.assertTrue(stxcal.is_busday(self.dt1) and
                        (not stxcal.is_busday(self.dt3)) and
                        stxcal.is_busday(self.dt4) and
                        (not stxcal.is_busday(self.dt6)))

    def test_4_move_busdays(self):
        res1 = stxcal.move_busdays(self.dt2, -1)
        res2 = stxcal.move_busdays(self.dt2,  1)
        res3 = stxcal.move_busdays(self.dt2,  0)
        res4 = stxcal.move_busdays(self.dt3, -1)
        res5 = stxcal.move_busdays(self.dt3,  0)
        res6 = stxcal.move_busdays(self.dt3,  1)
        res7 = stxcal.move_busdays(self.dt1,  7)
        res8 = stxcal.move_busdays(self.dt5, -7)
        self.assertTrue((res1 == self.dt1) and (res2 == '2016-12-19') and
                        (res3 == self.dt2) and (res4 == self.dt2) and
                        (res5 == self.dt2) and (res6 == res2) and
                        (res7 == self.dt5) and (res8 == self.dt1))

    def test_5_num_busdays(self):
        res1 = stxcal.num_busdays(self.dt2, self.dt2)
        res2 = stxcal.num_busdays(self.dt1, self.dt5)
        self.assertTrue((res1 == 0) and (res2 == 7))

    # call it like this: expiries('2015-01', '2015-08')
    def test_6_expiries(self):
        res = stxcal.expiries('2015-01', '2015-07')
        self.assertTrue((len(res) == 6) and (res[0] == '2015-01-17') and
                        (res[1] == '2015-02-20') and (res[2] == '2015-03-20')
                        and (res[3] == '2015-04-17') and
                        (res[4] == '2015-05-15') and (res[5] == '2015-06-19'))

    def test_7_next_expiry(self):
        res1 = stxcal.next_expiry(self.dt1)
        res2 = stxcal.next_expiry(self.dt1, 10)
        res3 = stxcal.next_expiry(self.dt1, 50)
        res4 = stxcal.next_expiry(self.dt1, 100)
        self.assertTrue((res1 == '2016-12-16') and (res2 == '2017-01-20') and
                        (res3 == '2017-03-17') and (res4 == '2017-05-19'))

    def test_8_prev_expiry(self):
        res1 = stxcal.prev_expiry(self.dt1, 0)
        res2 = stxcal.prev_expiry(self.dt1, 1)
        res3 = stxcal.prev_expiry(self.dt1, 50)
        res4 = stxcal.prev_expiry(self.dt1, 100)
        self.assertTrue((res1 == '2016-11-18') and (res2 == '2016-11-18') and
                        (res3 == '2016-09-16') and (res4 == '2016-07-15'))


# class Test3StxTS(unittest.TestCase):

#     def setUp(self):
#         pass

#     def tearDown(self):
#         pass

#     def test_1_find(self):
#         stk = 'TASR'
#         sd = '2002-04-01'
#         ed = '2002-04-11'
#         ts = StxTS(stk, sd, ed)
#         res1 = ts.find('2002-04-10')
#         res2 = ts.find('2002-04-06', -1)
#         res3 = ts.find('2002-04-06', 1)
#         self.assertTrue((res1 == 7) and (res2 == 4) and (res3 == 5))

#     def test_2_set_day_split(self):
#         stk = 'VXX'
#         sd = '2012-10-01'
#         ed = '2012-10-10'
#         ts = StxTS(stk, sd, ed)
#         ts.set_day('2012-10-04')
#         res1 = ts.df.ix['2012-10-04']
#         res2 = ts.df.ix['2012-10-05']
#         self.assertTrue((res1.c == 8.65) and (res1.v == 39762200) and
#                         (res2.c == 34.12) and (res2.v == 38797400))

#     def test_3_set_day_split(self):
#         stk = 'VXX'
#         sd = '2012-10-01'
#         ed = '2012-10-10'
#         ts = StxTS(stk, sd, ed)
#         ts.set_day('2012-10-05')
#         res1 = ts.df.ix['2012-10-04']
#         res2 = ts.df.ix['2012-10-05']
#         self.assertTrue((res1.c == 34.60) and (res1.v == 9940550) and
#                         (res2.c == 34.12) and (res2.v == 38797400))

#     def test_4_set_day_split(self):
#         stk = 'VXX'
#         sd = '2012-10-01'
#         ed = '2012-10-10'
#         ts = StxTS(stk, sd, ed)
#         ts.set_day('2012-10-05')
#         ts.set_day('2012-10-04')
#         res1 = ts.df.ix['2012-10-04']
#         res2 = ts.df.ix['2012-10-05']
#         self.assertTrue((res1.c == 8.65) and (res1.v == 39762200) and
#                         (res2.c == 34.12) and (res2.v == 38797400))

#     def test_5_next_day_split(self):
#         stk = 'VXX'
#         sd = '2012-10-01'
#         ed = '2012-10-10'
#         ts = StxTS(stk, sd, ed)
#         ts.set_day('2012-10-05')
#         ts.set_day('2012-10-04')
#         ts.next_day()
#         res1 = ts.df.ix['2012-10-04']
#         res2 = ts.df.ix['2012-10-05']
#         self.assertTrue((res1.c == 34.60) and (res1.v == 9940550) and
#                         (res2.c == 34.12) and (res2.v == 38797400))


# class Test4OptEod(unittest.TestCase):

#     def setUp(self):
#         self.start_date = '2002-02'
#         self.end_date = '2002-02'

#     def teardown(self):
#         pass

#     def test_01_load_data(self):
#         opt_eod = OptEOD()
#         opt_eod.load_opts(self.start_date, self.end_date)
#         res1 = stxdb.db_read_cmd('select count(*) from equities')
#         print('res1 = {0:s}'.format(res1))
#         self.assertEqual(res1[0][0], 1944)
#         res2 = stxdb.db_read_cmd('select sum(spot) from opt_spots')
#         print('res2 = {0:s}'.format(res2))
#         self.assertEqual(res2[0][0], 929973.0)
#         res3 = stxdb.db_read_cmd(
#             "select count(*) from options where expiry='2002-06-22'")
#         print('res3 = {0:s}'.format(res3))
#         self.assertEqual(res3[0][0], 101550)

class Test5StxEod(unittest.TestCase):

    def setUp(self):
        self.data_dir = os.getenv('DATA_DIR')
        self.my_eod_tbl = 'my_eods'
        self.dn_eod_tbl = 'dn_eods'
        self.md_eod_tbl = 'md_eods'
        self.ed_eod_tbl = 'ed_eods'
        self.my_split_tbl = 'my_dividends'
        self.dn_split_tbl = 'dn_dividends'
        self.md_split_tbl = 'md_dividends'
        self.ed_split_tbl = 'ed_dividends'
        self.recon_tbl = 'reconciliation'
        self.my_in_dir = '{0:s}/my_test'.format(self.data_dir)
        self.dn_in_dir = '/tmp/dn_test'
        self.md_in_dir = '{0:s}/md'.format(self.data_dir)
        self.ed_in_dir = '{0:s}/EODData'.format(self.data_dir)
        self.sq_in_dir = '{0:s}/ALL'.format(self.data_dir)
        self.my_dir = '{0:s}/bkp'.format(self.data_dir)
        self.dn_dir = '{0:s}/stockhistory_2017'.format(self.data_dir)
        self.md_dir = '{0:s}/md'.format(self.data_dir)
        self.stx = 'AEOS,EXPE,NFLX,TIE'
        self.dn_stx = 'AEOS,EXPE,NFLX,TIE,VXX'
        self.ed_stx = 'AA,EXPE,NFLX,VXX'
        self.sd = '2002-02-01'
        self.ed = '2012-12-31'
        self.sd_01 = '2012-12-03'
        self.sd_1 = '2013-01-02'
        self.ed_1 = '2013-11-15'
        self.eod_test = 'eods'
        self.split_test = 'dividends'
        self.my_stx = 'AA,AEOS,EXPE,INKT,MCT,NFLX,VXX'
        stk_list = self.stx.split(',')
        if not os.path.exists(self.my_in_dir):
            os.makedirs(self.my_in_dir)
        if not os.path.exists(self.dn_in_dir):
            os.makedirs(self.dn_in_dir)
        for stk in stk_list:
            copyfile('{0:s}/{1:s}.txt'.format(self.my_dir, stk),
                     '{0:s}/{1:s}.txt'.format(self.my_in_dir, stk))
        self.stk_list_db = "('{0:s}')".format("','".join(stk_list))

    def tearDown(self):
        pass

    def test_00_eod_name(self):
        my_eod = StxEOD(self.my_in_dir, 'my', self.recon_tbl)
        dn_eod = StxEOD(self.dn_in_dir, 'dn', self.recon_tbl)
        md_eod = StxEOD(self.md_in_dir, 'md', self.recon_tbl)
        ed_eod = StxEOD(self.ed_in_dir, 'ed', self.recon_tbl)
        sq_eod = StxEOD(self.sq_in_dir, 'sq', self.recon_tbl)
        eod = StxEOD(self.sq_in_dir, '', self.recon_tbl)
        self.assertEqual(my_eod.name, 'my')
        self.assertEqual(dn_eod.name, 'deltaneutral')
        self.assertEqual(md_eod.name, 'marketdata')
        self.assertEqual(ed_eod.name, 'eoddata')
        self.assertEqual(sq_eod.name, 'stooq')
        self.assertEqual(eod.name, 'final')

    def test_01_load_my_data(self):
        my_eod = StxEOD(self.my_dir, 'my', self.recon_tbl)
        my_eod.load_my_files(self.my_stx)
        res1 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.my_eod_tbl))
        res2 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.my_split_tbl))
        in_stx = ','.join(["'{0:s}'".format(x)
                           for x in self.my_stx.split(',')])
        res3 = stxdb.db_read_cmd(
            'select stk, min(date), max(date), count(*) from {0:s} where '
            'stk in ({1:s}) group by stk order by stk'.
            format(my_eod.eod_tbl, in_stx))
        res4 = stxdb.db_read_cmd(
            "select stk, sum(ratio), count(*) from {0:s} where stk in ({1:s}) "
            "group by stk order by stk".format(my_eod.divi_tbl, in_stx))
        print('res1 = {0:s}'.format(res1))
        print('res2 = {0:s}'.format(res2))
        print('res3 = {0:s}'.format(res3))
        print('res4 = {0:s}'.format(res4))
        self.assertEqual(len(res1), 1)
        self.assertEqual(len(res2), 1)
        self.assertEqual(
            res3[0], ('AA', date(1962, 1, 2), date(2012, 12, 31), 12836))
        self.assertEqual(
            res3[1], ('AEOS', date(1994, 4, 14), date(2007, 1, 26), 3214))
        self.assertEqual(
            res3[2], ('EXPE', date(2000, 5, 4), date(2012, 12, 31), 2694))
        self.assertEqual(
            res3[3], ('INKT', date(2000, 5, 4), date(2003, 3, 19), 720))
        self.assertEqual(
            res3[4], ('MCT', date(2000, 5, 4), date(2001, 11, 29), 394))
        self.assertEqual(
            res3[5], ('NFLX', date(2002, 5, 29), date(2012, 12, 31), 2668))
        self.assertEqual(
            res3[6], ('VXX', date(2009, 1, 30), date(2012, 12, 31), 987))
        self.assertEqual(res4[0], ('AA', Decimal('4.4800'), 7))
        self.assertEqual(res4[1], ('AEOS', Decimal('3.6800'), 6))
        self.assertEqual(res4[2], ('NFLX', Decimal('0.5000'), 1))
        self.assertEqual(res4[3], ('VXX', Decimal('8.0100'), 2))

    def test_02_load_9899_data(self):
        my_eod = StxEOD(self.my_dir, 'my', self.recon_tbl)
        my_eod.load_my_9899_files(self.my_stx)
        in_stx = ','.join(["'{0:s}'".format(x)
                           for x in self.my_stx.split(',')])
        res1 = stxdb.db_read_cmd(
            'select stk, min(date), max(date), count(*) from {0:s} where '
            'stk in ({1:s}) group by stk order by stk'.
            format(my_eod.eod_tbl, in_stx))
        print('test_02_load_9899_data')
        print('res1 = {0:s}'.format(res1))
        self.assertEqual(
            res1[0], ('AA', date(1962, 1, 2), date(2012, 12, 31), 12836))
        self.assertEqual(
            res1[1], ('AEOS', date(1994, 4, 14), date(2007, 1, 26), 3214))
        self.assertEqual(
            res1[2], ('EXPE', date(1999, 11, 19), date(2012, 12, 31), 2806))
        self.assertEqual(
            res1[3], ('INKT', date(1998, 6, 10), date(2003, 3, 19), 1200))
        self.assertEqual(
            res1[4], ('MCT', date(1998, 11, 24), date(2001, 11, 29), 757))
        self.assertEqual(
            res1[5], ('NFLX', date(2002, 5, 29), date(2012, 12, 31), 2668))
        self.assertEqual(
            res1[6], ('VXX', date(2009, 1, 30), date(2012, 12, 31), 987))

    def test_03_load_dn_data(self):
        dn_eod = StxEOD(self.dn_in_dir, 'dn', self.recon_tbl)
        dn_eod.load_deltaneutral_files(self.dn_stx)
        res1 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.dn_eod_tbl))
        res2 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.dn_split_tbl))
        res3 = stxdb.db_read_cmd('select distinct stk from {0:s}'.
                                 format(self.dn_eod_tbl))
        res4 = stxdb.db_read_cmd(
            "select stk, count(*) from {0:s} where stk in "
            "('NFLX', 'AEOS', 'TIE', 'EXPE') and date <= '2012-12-31' "
            "group by stk order by stk".format(self.dn_eod_tbl))
        res5 = stxdb.db_read_cmd("select stk, sum(ratio) from {0:s} where stk "
                                 "in ('NFLX', 'AEOS', 'TIE', 'EXPE') and date "
                                 " <= '2012-12-31' group by stk order by stk".
                                 format(self.dn_split_tbl))
        print('test_03_load_dn_data')
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
        self.assertTrue(len(res1) == 1 and len(res2) == 1 and len(res3) == 5
                        and res4[0][0] == 'AEOS' and res4[0][1] == 1549 and
                        res4[1][0] == 'EXPE' and res4[1][1] == 1875 and
                        res4[2][0] == 'NFLX' and res4[2][1] == 2671 and
                        res4[3][0] == 'TIE' and res4[3][1] == 3017 and
                        res5[0][0] == 'NFLX' and float(res5[0][1]) == 0.5 and
                        res5[1][0] == 'TIE' and float(res5[1][1]) == 15.1793)

    def test_04_load_md_data(self):
        md_eod = StxEOD(self.md_in_dir, 'md', self.recon_tbl)
        log_fname = 'splitsdivistest{0:s}.csv'.format(datetime.datetime.now().
                                                      strftime('%Y%m%d%H%M%S'))
        db_stx, stx_dct = md_eod.create_exchange()
        with open(log_fname, 'w') as logfile:
            md_eod.load_marketdata_file(os.path.join(
                self.md_in_dir, 'NASDAQ', 'EXPE.csv'), logfile, db_stx)
            md_eod.load_marketdata_file(os.path.join(
                self.md_in_dir, 'NASDAQ', 'NFLX.csv'), logfile, db_stx)
            md_eod.load_marketdata_file(os.path.join(
                self.md_in_dir, 'NYSE', 'IBM.csv'), logfile, db_stx)
            md_eod.load_marketdata_file(os.path.join(
                self.md_in_dir, 'AMEX', 'VXX.csv'), logfile, db_stx)
        os.remove(log_fname)
        res1 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.md_eod_tbl))
        res2 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.md_split_tbl))
        res3 = stxdb.db_read_cmd('select distinct stk from {0:s}'.
                                 format(self.md_eod_tbl))
        res4 = stxdb.db_read_cmd("select stk, count(*) from {0:s} group by stk"
                                 " order by stk".format(self.md_eod_tbl))
        print('res1')
        print(res1)
        print('res2')
        print(res2)
        print('res3')
        print(res3)
        print('res4')
        print(res4)
        self.assertTrue(len(res1) == 1 and len(res2) == 1 and len(res3) == 4
                        and res4[0][0] == 'EXPE' and res4[0][1] == 2820 and
                        res4[1][0] == 'IBM' and res4[1][1] == 13783 and
                        res4[2][0] == 'NFLX' and res4[2][1] == 3616 and
                        res4[3][0] == 'VXX' and res4[3][1] == 1932)

    def test_05_load_ed_data(self):
        ed_eod = StxEOD(self.ed_in_dir, 'ed', self.recon_tbl)
        ed_eod.load_eoddata_files(sd=self.sd_01, stks=self.ed_stx)
        res1 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.ed_eod_tbl))
        res2 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(self.ed_split_tbl))
        res3 = stxdb.db_read_cmd('select distinct stk from {0:s}'.
                                 format(self.ed_eod_tbl))
        res4 = stxdb.db_read_cmd("select stk, count(*) from {0:s} where stk in"
                                 " ('AA', 'NFLX', 'VXX', 'EXPE') group by stk "
                                 "order by stk".format(self.ed_eod_tbl))
        self.assertTrue(len(res1) == 1 and len(res2) == 1 and len(res3) == 4
                        and res4[0][0] == 'AA' and res4[0][1] == 242 and
                        res4[1][0] == 'EXPE' and res4[1][1] == 242 and
                        res4[2][0] == 'NFLX' and res4[2][1] == 242 and
                        res4[3][0] == 'VXX' and res4[3][1] == 241)

    def test_06_load_stooq_eod_files(self):
        sq_eod = StxEOD(self.sq_in_dir, 'sq', self.recon_tbl)
        sq_eod.parseeodfiles('2016-08-24', '2016-08-26')
        res1 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(sq_eod.eod_tbl))
        res2 = stxdb.db_read_cmd(
            StxEOD.sql_show_tables.format(sq_eod.divi_tbl))
        res3 = stxdb.db_read_cmd('select distinct stk from {0:s}'.
                                 format(sq_eod.eod_tbl))
        res4 = stxdb.db_read_cmd("select stk, count(*) from {0:s} where stk in"
                                 " ('AA', 'NFLX', 'VXX', 'EXPE') group by stk "
                                 "order by stk".format(sq_eod.eod_tbl))
        print('res1')
        print(res1)
        print('res2')
        print(res2)
        print('res3')
        print(res3)
        print('res4')
        print(res4)
        self.assertTrue(len(res1) == 1 and len(res2) == 1 and len(res3) == 8118
                        and res4[0][0] == 'AA' and res4[0][1] == 3 and
                        res4[1][0] == 'EXPE' and res4[1][1] == 3 and
                        res4[2][0] == 'NFLX' and res4[2][1] == 3 and
                        res4[3][0] == 'VXX' and res4[3][1] == 3)


if __name__ == '__main__':
    unittest.main()
