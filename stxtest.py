import os
import unittest
from stxcal import *
from stxdb import *

class Test1StxDB(unittest.TestCase) :
    def setUp(self) :
        self.tbl_name       = 'eod_test'
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
        self.sql_select_1   = "select * from eod where "\
                              "stk = 'A' and dt between '2010-01-01' and "\
                              "'2010-01-31'"
        self.sql_tbls_like  = "show tables like '{0:s}'".format(self.tbl_name)
        self.sql_describe   = 'describe {0:s}'.format(self.tbl_name)
        self.sql_select_2   = 'select * from {0:s}'.format(self.tbl_name)
        self.sql_drop_tbl   = 'drop table {0:s}'.format(self.tbl_name)
        self.file_name      = 'C:/ProgramData/MySQL/MySQL Server 5.7/'\
                              'Uploads/stxtest_upload.txt'
        with open(self.file_name, 'w') as f :
            f.write('A\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('B\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('C\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('A\t2000-01-01\t33.00\t34.00\t33.00\t34.00\t1000\n')
            f.write('A\t2000-01-02\t33.00\t34.00\t33.00\t34.00\t1000\n')
        

    def tearDown(self) :
        os.remove(self.file_name)

    def test_1_read_cmd(self) :
        res = db_read_cmd(self.sql_select_1)
        self.assertEqual(len(res), 19)
        
    def test_2_create_missing_table(self) :
        res0 = db_read_cmd(self.sql_tbls_like)
        db_create_missing_table(self.tbl_name, self.sql_create_tbl)
        res1 = db_read_cmd(self.sql_tbls_like)
        res2 = db_read_cmd(self.sql_describe)
        self.assertTrue((len(res0) == 0) and (len(res1) == 1) and \
                        (res1[0][0] == self.tbl_name) and \
                        (len(res2) == 7) and (res2[0][3] == 'PRI') and \
                        (res2[1][3] == 'PRI') and (res2[2][3] == ''))

    def test_3_get_key_len(self) :
        res = db_get_key_len(self.tbl_name)
        self.assertEqual(res, 2)

    def test_4_upload_file(self) :
        key_len = db_get_key_len(self.tbl_name)
        db_upload_file(self.file_name, self.tbl_name, key_len)
        res     = db_read_cmd(self.sql_select_2)
        self.assertEqual(len(res), 4)

    def test_5_drop_table(self) :
        db_write_cmd(self.sql_drop_tbl)
        res = db_read_cmd(self.sql_tbls_like)
        self.assertEqual(len(res), 0)


class Test2StxCal(unittest.TestCase) :

    def setUp(self) :
        self.dt1 = '2016-12-15'
        self.dt2 = '2016-12-16'
        self.dt3 = '2016-12-18'
        self.dt4 = '2016-12-23'
        self.dt5 = '2016-12-27'
        self.dt6 = '2014-07-04'
        
    def tearDown(self) :
        pass
    
    def test_1_next_busday(self) :
        res1 = next_busday(self.dt1)
        res2 = next_busday(self.dt2)
        res3 = next_busday(self.dt3)
        res4 = next_busday(self.dt4)
        self.assertTrue((res1 == '2016-12-16') and (res2 == '2016-12-19') and \
                        (res3 == '2016-12-19') and (res4 == '2016-12-27'))
        
    def test_2_prev_busday(self) :
        res1 = prev_busday(self.dt1)
        res2 = prev_busday(self.dt2)
        res3 = prev_busday(self.dt3)
        res5 = prev_busday(self.dt5)
        self.assertTrue((res1 == '2016-12-14') and (res2 == '2016-12-15') and \
                        (res3 == '2016-12-16') and (res5 == '2016-12-23'))

    def test_3_is_busday(self) :
        self.assertTrue(is_busday(self.dt1) and (not is_busday(self.dt3)) and \
                        is_busday(self.dt4) and (not is_busday(self.dt6)))

    def test_4_move_busdays(self):
        res1 = move_busdays(self.dt2, -1)
        res2 = move_busdays(self.dt2,  1)
        res3 = move_busdays(self.dt2,  0)
        res4 = move_busdays(self.dt3, -1)
        res5 = move_busdays(self.dt3,  0)
        res6 = move_busdays(self.dt3,  1)
        res7 = move_busdays(self.dt1,  7)
        res8 = move_busdays(self.dt5, -7)
        self.assertTrue((res1 == self.dt1) and (res2 == '2016-12-19') and \
                        (res3 == self.dt2) and (res4 == self.dt2) and \
                        (res5 == self.dt2) and (res6 == res2) and \
                        (res7 == self.dt5) and (res8 == self.dt1))

    def test_5_num_busdays(self):
        res1 = num_busdays(self.dt2, self.dt2)
        res2 = num_busdays(self.dt1, self.dt5)
        self.assertTrue((res1 == 0) and (res2 == 7))

    # call it like this: expiries('2015-01', '2015-08')
    def test_6_expiries(self) :
        res = expiries('2015-01', '2015-07')
        self.assertTrue((len(res) == 6) and (res[0] == '2015-01-17') and \
                        (res[1]=='2015-02-20') and (res[2]=='2015-03-20') and \
                        (res[3]=='2015-04-17') and (res[4]=='2015-05-15') and \
                        (res[5]=='2015-06-19'))

    def test_7_next_expiry(self) :
        res1 = next_expiry(self.dt1)
        res2 = next_expiry(self.dt1, 10)
        res3 = next_expiry(self.dt1, 50)
        res4 = next_expiry(self.dt1, 100)
        self.assertTrue((res1 == '2016-12-16') and (res2 == '2017-01-20') and \
                        (res3 == '2017-03-17') and (res4 == '2017-05-19'))

    def test_8_prev_expiry(self) :
        res1 = prev_expiry(self.dt1, 0)
        res2 = prev_expiry(self.dt1, 1)
        res3 = prev_expiry(self.dt1, 50)
        res4 = prev_expiry(self.dt1, 100)
        self.assertTrue((res1 == '2016-11-18') and (res2 == '2016-11-18') and \
                        (res3 == '2016-09-16') and (res4 == '2016-07-15'))

    

if __name__ == '__main__':
    unittest.main()
