from decimal import Decimal
import os
import unittest
from shutil import copyfile
import stxcal
import stxdb
from stxeod import StxEOD
from stxts import StxTS


class Test1StxDB(unittest.TestCase):
    def setUp(self):
        self.tbl_name = 'eod_test'
        self.sql_create_tbl = 'CREATE TABLE `{0:s}` ('\
                              '`stk` varchar(8) NOT NULL,'\
                              '`dt` varchar(10) NOT NULL,'\
                              '`o` decimal(9,2) DEFAULT NULL,'\
                              '`h` decimal(9,2) DEFAULT NULL,'\
                              '`l` decimal(9,2) DEFAULT NULL,'\
                              '`c` decimal(9,2) DEFAULT NULL,'\
                              '`v` int(11) DEFAULT NULL,'\
                              'PRIMARY KEY (`stk`,`dt`)'\
                              ') ENGINE=MyISAM DEFAULT CHARSET=utf8'.\
                              format(self.tbl_name)
        self.sql_select_1 = "select * from eod where "\
                            "stk = 'A' and dt between '2010-01-01' and "\
                            "'2010-01-31'"
        self.sql_tbls_like = "show tables like '{0:s}'".format(self.tbl_name)
        self.sql_describe = 'describe {0:s}'.format(self.tbl_name)
        self.sql_select_2 = 'select * from {0:s}'.format(self.tbl_name)
        self.sql_drop_tbl = 'drop table {0:s}'.format(self.tbl_name)
        self.file_name = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/'\
                         'stxtest_upload.txt'
        with open(self.file_name, 'w') as f:
            f.write('A\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('B\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('C\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('A\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('A\t2000-01-02\t33.00\t34.00\t33.00\t34.00\t1000\n')

    def tearDown(self):
        os.remove(self.file_name)

    def test_1_read_cmd(self):
        res = stxdb.db_read_cmd(self.sql_select_1)
        self.assertEqual(len(res), 19)

    def test_2_create_missing_table(self):
        res0 = stxdb.db_read_cmd(self.sql_tbls_like)
        stxdb.db_create_missing_table(self.tbl_name, self.sql_create_tbl)
        res1 = stxdb.db_read_cmd(self.sql_tbls_like)
        res2 = stxdb.db_read_cmd(self.sql_describe)
        self.assertTrue((len(res0) == 0) and (len(res1) == 1) and
                        (res1[0][0] == self.tbl_name) and
                        (len(res2) == 7) and (res2[0][3] == 'PRI') and
                        (res2[1][3] == 'PRI') and (res2[2][3] == ''))

    def test_3_get_key_len(self):
        res = stxdb.db_get_key_len(self.tbl_name)
        self.assertEqual(res, 2)

    def test_4_upload_file(self):
        key_len = stxdb.db_get_key_len(self.tbl_name)
        stxdb.db_upload_file(self.file_name, self.tbl_name, key_len)
        res = stxdb.db_read_cmd(self.sql_select_2)
        self.assertEqual(len(res), 4)

    def test_5_drop_table(self):
        stxdb.db_write_cmd(self.sql_drop_tbl)
        res = stxdb.db_read_cmd(self.sql_tbls_like)
        self.assertEqual(len(res), 0)


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


class Test3StxTS(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_1_find(self):
        stk = 'TASR'
        sd = '2002-04-01'
        ed = '2002-04-11'
        ts = StxTS(stk, sd, ed)
        res1 = ts.find('2002-04-10')
        res2 = ts.find('2002-04-06', -1)
        res3 = ts.find('2002-04-06', 1)
        self.assertTrue((res1 == 7) and (res2 == 4) and (res3 == 5))

    def test_2_set_day_split(self):
        stk = 'VXX'
        sd = '2012-10-01'
        ed = '2012-10-10'
        ts = StxTS(stk, sd, ed)
        ts.set_day('2012-10-04')
        res1 = ts.df.ix['2012-10-04']
        res2 = ts.df.ix['2012-10-05']
        self.assertTrue((res1.c == 8.65) and (res1.v == 9939200) and
                        (res2.c == 34.12) and (res2.v == 38796800))

    def test_3_set_day_split(self):
        stk = 'VXX'
        sd = '2012-10-01'
        ed = '2012-10-10'
        ts = StxTS(stk, sd, ed)
        ts.set_day('2012-10-05')
        res1 = ts.df.ix['2012-10-04']
        res2 = ts.df.ix['2012-10-05']
        self.assertTrue((res1.c == 34.60) and (res1.v == 2484800) and
                        (res2.c == 34.12) and (res2.v == 38796800))

    def test_4_set_day_split(self):
        stk = 'VXX'
        sd = '2012-10-01'
        ed = '2012-10-10'
        ts = StxTS(stk, sd, ed)
        ts.set_day('2012-10-05')
        ts.set_day('2012-10-04')
        res1 = ts.df.ix['2012-10-04']
        res2 = ts.df.ix['2012-10-05']
        self.assertTrue((res1.c == 8.65) and (res1.v == 9939200) and
                        (res2.c == 34.12) and (res2.v == 38796800))

    def test_5_next_day_split(self):
        stk = 'VXX'
        sd = '2012-10-01'
        ed = '2012-10-10'
        ts = StxTS(stk, sd, ed)
        ts.set_day('2012-10-05')
        ts.set_day('2012-10-04')
        ts.next_day()
        res1 = ts.df.ix['2012-10-04']
        res2 = ts.df.ix['2012-10-05']
        self.assertTrue((res1.c == 34.60) and (res1.v == 2484800) and
                        (res2.c == 34.12) and (res2.v == 38796800))


class Test4StxEOD(unittest.TestCase):

    def setUp(self):
        self.my_eod_tbl = 'my_eod_test'
        self.dn_eod_tbl = 'dn_eod_test'
        self.my_split_tbl = 'my_split_test'
        self.dn_split_tbl = 'dn_split_test'
        self.my_in_dir = 'C:/goldendawn/my_test'
        self.dn_in_dir = 'C:/goldendawn/dn_test'
        self.my_dir = 'C:/goldendawn/bkp'
        self.dn_dir = 'C:/goldendawn/dn'
        self.stx = 'AEOS,EXPE,NFLX,TIE'
        self.sd = '2002-02-01'
        self.ed = '2012-12-31'
        self.eod_test = 'eod_test'
        self.split_test = 'split_test'
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

    def test_1_cleanup(self):
        stxdb.db_create_missing_table(
            self.dn_eod_tbl,
            StxEOD.sql_create_eod.format(self.dn_eod_tbl))
        stxdb.db_create_missing_table(
            self.dn_split_tbl,
            StxEOD.sql_create_split.format(self.dn_split_tbl))
        dirname = self.dn_in_dir
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        seod = StxEOD(self.dn_in_dir, self.dn_eod_tbl, self.dn_split_tbl)
        seod.cleanup()
        seod.cleanup_data_folder()
        res1 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.dn_eod_tbl))
        res2 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.dn_split_tbl))
        self.assertTrue((not res1) and (not res2) and
                        (not os.path.exists(dirname)))

    def test_2_load_my_data(self):
        my_eod = StxEOD(self.my_in_dir, self.my_eod_tbl, self.my_split_tbl)
        my_eod.load_my_files(self.stx)
        res1 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.my_eod_tbl))
        res2 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.my_split_tbl))
        res3 = stxdb.db_read_cmd('select distinct stk from {0:s}'.
                                 format(self.my_eod_tbl))
        res4 = stxdb.db_read_cmd("select stk, count(*) from {0:s} where stk "
                                 "in {1:s} and dt<='{2:s}' group by stk "
                                 "order by stk".format(self.my_eod_tbl,
                                                       self.stk_list_db,
                                                       self.ed))
        res5 = stxdb.db_read_cmd("select stk, sum(ratio) from {0:s} where stk "
                                 "in {1:s} and dt<='{2:s}' group by stk "
                                 "order by stk".format(self.my_split_tbl,
                                                       self.stk_list_db,
                                                       self.ed))
        self.assertTrue(len(res1) == 1 and len(res2) == 1 and len(res3) == 4
                        and res4[0][0] == 'AEOS' and res4[0][1] == 3214 and
                        res4[1][0] == 'EXPE' and res4[1][1] == 2694 and
                        res4[2][0] == 'NFLX' and res4[2][1] == 2668 and
                        res4[3][0] == 'TIE' and res4[3][1] == 4164 and
                        res5[0][0] == 'AEOS' and float(res5[0][1]) == 3.68 and
                        res5[1][0] == 'NFLX' and float(res5[1][1]) == 0.5)

    def test_3_load_dn_data(self):
        dn_eod = StxEOD(self.dn_in_dir, self.dn_eod_tbl, self.dn_split_tbl)
        dn_eod.load_deltaneutral_files(self.stx)
        res1 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.dn_eod_tbl))
        res2 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.dn_split_tbl))
        res3 = stxdb.db_read_cmd('select distinct stk from {0:s}'.
                                 format(self.dn_eod_tbl))
        res4 = stxdb.db_read_cmd("select stk, count(*) from {0:s} where stk in"
                                 " ('NFLX', 'AEOS', 'TIE', 'EXPE') and dt <= "
                                 "'2012-12-31' group by stk order by stk".
                                 format(self.dn_eod_tbl))
        res5 = stxdb.db_read_cmd("select stk, sum(ratio) from {0:s} where stk "
                                 "in ('NFLX', 'AEOS', 'TIE', 'EXPE') and dt <="
                                 " '2012-12-31' group by stk order by stk".
                                 format(self.dn_split_tbl))
        self.assertTrue(len(res1) == 1 and len(res2) == 1 and len(res3) == 4
                        and res4[0][0] == 'AEOS' and res4[0][1] == 1549 and
                        res4[1][0] == 'EXPE' and res4[1][1] == 1875 and
                        res4[2][0] == 'NFLX' and res4[2][1] == 2671 and
                        res4[3][0] == 'TIE' and res4[3][1] == 3017 and
                        res5[0][0] == 'NFLX' and float(res5[0][1]) == 0.5 and
                        res5[1][0] == 'TIE' and float(res5[1][1]) == 15.1793)

    def test_4_reconcile_my_data(self):
        my_eod = StxEOD(self.my_in_dir, self.my_eod_tbl, self.my_split_tbl)
        my_eod.reconcile_spots('2002-02-01', '2012-12-31', self.stx)
        res1 = stxdb.db_read_cmd("select * from reconciliation where "
                                 "recon_name='{0:s}' order by stk".
                                 format(my_eod.rec_name))
        res2 = stxdb.db_read_cmd("select * from {0:s} where implied=1 "
                                 "order by stk, dt".format(self.my_split_tbl))
        self.assertTrue(res1[0] == ('AEOS', my_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2007-03-09', '2002-02-04',
                                    '2007-01-26', 0, 97.73, 0.0009, 0) and
                        res1[1] == ('EXPE', my_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2012-12-31', '2002-02-01',
                                    '2012-12-31', 1, 100.0, 0.0001, 0) and
                        res1[2] == ('NFLX', my_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-12-11', '2012-12-31', '2002-05-29',
                                    '2012-12-31', 0, 100.0, 0.0012, 0) and
                        res1[3] == ('TIE', my_eod.rec_name,
                                    '20020201_20121231',
                                    '2005-10-03', '2012-12-31', '2002-02-04',
                                    '2012-12-31', 2, 100.0, 0.001, 0) and
                        res2[0] == ('EXPE', '2003-03-10', Decimal('0.5000'),
                                    1) and
                        res2[1] == ('TIE', '2006-02-16', Decimal('0.5000'),
                                    1) and
                        res2[2] == ('TIE', '2006-05-15', Decimal('0.5000'), 1))

    def test_5_reconcile_dn_data(self):
        dn_eod = StxEOD(self.dn_in_dir, self.dn_eod_tbl, self.dn_split_tbl)
        dn_eod.reconcile_spots('2002-02-01', '2012-12-31', self.stx)
        res1 = stxdb.db_read_cmd("select * from reconciliation where "
                                 "recon_name='{0:s}' order by stk".
                                 format(dn_eod.rec_name))
        res2 = stxdb.db_read_cmd("select * from {0:s} where implied=1 order"
                                 " by stk, dt".format(self.dn_split_tbl))
        self.assertTrue(res1[0] == ('AEOS', dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2007-03-09', '2002-02-04',
                                    '2007-03-09', 1, 100.0, 0.0011, 0) and
                        res1[1] == ('EXPE', dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-02-08', '2012-12-31', '2005-07-21',
                                    '2012-12-31', 0, 68.34, 0.0001, 0) and
                        res1[2] == ('NFLX', dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2002-12-11', '2012-12-31', '2002-05-23',
                                    '2012-12-31', 0, 100.0, 0.0013, 0) and
                        res1[3] == ('TIE', dn_eod.rec_name,
                                    '20020201_20121231',
                                    '2005-10-03', '2012-12-31', '2002-02-01',
                                    '2012-12-31', 0, 100.0, 0.0041, 0) and
                        res2[0] == ('AEOS', '2005-03-07', Decimal('0.4999'),
                                    1))

    def test_6_split_recon(self):
        # my_eod = StxEOD(self.my_in_dir, self.my_eod_tbl, self.my_split_tbl)
        dn_eod = StxEOD(self.dn_in_dir, self.dn_eod_tbl, self.dn_split_tbl)
        dn_eod.split_reconciliation('AEOS', '2001-01-01', '2012-12-31',
                                    ['splits', 'my_split_test'])
        res = stxdb.db_read_cmd("select * from {0:s} where stk='{1:s}'"
                                .format(dn_eod.split_tbl, 'AEOS'))
        self.assertTrue(
            res[0] == ('AEOS', '2005-03-07', Decimal('0.4999'), 1) and
            res[1] == ('AEOS', '2006-12-19', Decimal('0.6700'), 0))

    def test_7_merge_eod_tbls(self):
        my_eod = StxEOD(self.my_in_dir, self.my_eod_tbl, self.my_split_tbl)
        dn_eod = StxEOD(self.dn_in_dir, self.dn_eod_tbl, self.dn_split_tbl)
        my_eod.upload_eod(self.eod_test, self.split_test, self.stx, self.sd,
                          self.ed)
        dn_eod.upload_eod(self.eod_test, self.split_test, self.stx, self.sd,
                          self.ed)
        res1 = stxdb.db_read_cmd("select * from {0:s} where stk='EXPE' and "
                                 "dt between '2003-03-10' and '2003-03-11'".
                                 format(self.eod_test))
        res2 = stxdb.db_read_cmd("select * from {0:s} where stk='TIE' and dt "
                                 "between '2006-02-16' and '2006-02-17'".
                                 format(self.eod_test))
        res3 = stxdb.db_read_cmd("select * from {0:s} where stk='TIE' and dt "
                                 "between '2006-05-15' and '2006-05-16'".
                                 format(self.eod_test))
        res4 = stxdb.db_read_cmd("select * from {0:s} where stk='EXPE' and dt "
                                 "between '2003-08-08' and '2005-07-21'".
                                 format(self.eod_test))
        res5 = stxdb.db_read_cmd('select stk, count(*) from {0:s} '
                                 'group by stk'.format(self.split_test))
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
                        res4[0][1] == '2003-08-08' and
                        res4[1][1] == '2005-07-21' and
                        res5[0] == ('AEOS', 6) and
                        res5[1] == ('EXPE', 1) and
                        res5[2] == ('NFLX', 1) and
                        res5[3] == ('TIE', 2))

    def test_8_teardown(self):
        my_eod = StxEOD(self.my_in_dir, self.my_eod_tbl, self.my_split_tbl)
        dn_eod = StxEOD(self.dn_in_dir, self.dn_eod_tbl, self.dn_split_tbl)
        my_eod.cleanup()
        my_eod.cleanup_data_folder()
        dn_eod.cleanup()
        dn_eod.cleanup_data_folder()
        stxdb.db_write_cmd("delete from reconciliation where recon_name="
                           "'{0:s}'".format(my_eod.rec_name))
        stxdb.db_write_cmd("delete from reconciliation where recon_name="
                           "'{0:s}'".format(dn_eod.rec_name))
        stxdb.db_write_cmd('drop table {0:s}'.format(self.eod_test))
        stxdb.db_write_cmd('drop table {0:s}'.format(self.split_test))
        res1 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.my_eod_tbl))
        res2 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.my_split_tbl))
        res3 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.dn_eod_tbl))
        res4 = stxdb.db_read_cmd("show tables like '{0:s}'".
                                 format(self.dn_split_tbl))
        self.assertTrue((not res1) and (not res2) and (not res3) and
                        (not res4) and (not os.path.exists(self.my_in_dir))
                        and (not os.path.exists(self.dn_in_dir)))


if __name__ == '__main__':
    unittest.main()
