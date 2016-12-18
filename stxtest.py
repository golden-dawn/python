import os
import unittest
from stxdb import StxDB

class TestStxDB(unittest.TestCase) :
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
        res = StxDB.read_cmd(self.sql_select_1)
        self.assertEqual(len(res), 19)
        
    def test_2_create_missing_table(self) :
        res0 = StxDB.read_cmd(self.sql_tbls_like)
        StxDB.create_missing_table(self.tbl_name, self.sql_create_tbl)
        res1 = StxDB.read_cmd(self.sql_tbls_like)
        res2 = StxDB.read_cmd(self.sql_describe)
        self.assertTrue((len(res0) == 0) and (len(res1) == 1) and \
                        (res1[0][0] == self.tbl_name) and \
                        (len(res2) == 7) and (res2[0][3] == 'PRI') and \
                        (res2[1][3] == 'PRI') and (res2[2][3] == ''))

    def test_3_get_key_len(self) :
        res = StxDB.get_key_len(self.tbl_name)
        self.assertEqual(res, 2)

    def test_4_upload_file(self) :
        key_len = StxDB.get_key_len(self.tbl_name)
        StxDB.upload_file(self.file_name, self.tbl_name, key_len)
        res     = StxDB.read_cmd(self.sql_select_2)
        self.assertEqual(len(res), 4)

    def test_5_drop_table(self) :
        StxDB.write_cmd(self.sql_drop_tbl)
        res = StxDB.read_cmd(self.sql_tbls_like)
        self.assertEqual(len(res), 0)


if __name__ == '__main__':
    unittest.main()
