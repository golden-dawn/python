import stxcal
import stxdb


def print_stk_data(stk, dt):
    s_date = stxcal.move_busdays(dt, -8)
    e_date = stxcal.move_busdays(dt, 8)
    sql = "select * from split where stk='{0:s}' and "\
        "dt between '{1:s}' and '{2:s}'".format(stk, s_date, e_date)
    res = stxdb.db_read_cmd(sql)
    print('{0:s}: splits'.format(stk))
    for x in res:
        print('{0:s} {1:s} {2:.4f} {3:d}'.format(x[0], x[1], float(x[2]),
                                                 x[3]))
    sql = "select * from eod where stk='{0:s}' and "\
        "dt between '{1:s}' and '{2:s}'".format(stk, s_date, e_date)
    res = stxdb.db_read_cmd(sql)
    print('{0:s}: eod'.format(stk))
    for x in res:
        print('{0:s} {1:s} {2:.2f} {3:.2f} {4:.2f} {5:.2f} {6:d}'.
              format(x[0], x[1], float(x[2]), float(x[3]), float(x[4]),
                     float(x[5]), x[6]))


def split_adjustments():
    stxdb.db_write_cmd("update eod set c=17.00 where stk='AACC' and "
                       "dt='2004-09-21'")
    stxdb.db_write_cmd("insert into split values ('AAUKY', '2001-05-07', "
                       "0.25, 0)")
    stxdb.db_write_cmd("insert into split values ('AAUKY', '2006-03-03', "
                       "0.5, 0)")
    stxdb.db_write_cmd("delete from eod where stk='ABVT'")
    stxdb.db_write_cmd("delete from eod where stk='AFN'")
    stxdb.db_write_cmd("insert into split values ('AI', '2002-11-01', "
                       "100.0, 0)")
    stxdb.db_write_cmd("insert into split values ('AIBYY', '2011-02-22', "
                       "4.0, 0)")
    stxdb.db_write_cmd("update split set dt='2012-07-23' where stk='AMPL' "
                       "and dt='2012-07-20'")
    stxdb.db_write_cmd("update split set dt='2006-04-19' where stk='APPX' "
                       "and dt='2006-04-18'")
    stxdb.db_write_cmd("insert into split values ('AT', '2006-07-17', "
                       "0.8333, 0)")
    stxdb.db_write_cmd("insert into split values ('AXAHY', '2001-05-17', "
                       "0.5, 0)")
    stxdb.db_write_cmd("insert into split values ('BAH', '2012-09-03', "
                       "0.67, 0)")
    stxdb.db_write_cmd("insert into split values ('BANRD', '2011-05-31', "
                       "6.0, 0)")
    stxdb.db_write_cmd("insert into split values ('BAY', '2001-08-07', "
                       "0.8333, 0)")
    stxdb.db_write_cmd("insert into split values ('BBH', '2009-03-31', "
                       "0.5, 0)")
    stxdb.db_write_cmd("insert into split values ('BCO', '2008-10-31', "
                       "0.55, 0)")
    stxdb.db_write_cmd("update split set dt='2003-11-21' where stk='BKE' "
                       "and dt='2003-11-20'")
    stxdb.db_write_cmd("update split set dt='2004-09-10' where stk='BKHM' "
                       "and dt='2004-09-09'")
    stxdb.db_write_cmd("insert into split values ('BZF', '2012-12-20', "
                       "0.8, 0)")
    stxdb.db_write_cmd("insert into split values ('CAGC', '2009-09-04', "
                       "4.5, 0)")
    stxdb.db_write_cmd("insert into split values ('CAH', '2009-08-31', "
                       "0.75, 0)")
    stxdb.db_write_cmd("insert into split values ('CCE', '2010-10-01', "
                       "0.67, 0)")
    stxdb.db_write_cmd("insert into split values ('CCMP', '2012-03-01', "
                       "0.67, 0)")
    stxdb.db_write_cmd("insert into split values ('CEL', '2003-05-09', "
                       "10.0, 0)")
    stxdb.db_write_cmd("update split set dt='2005-10-21' where stk='CHRW' "
                       "and dt='2005-10-14'")
    stxdb.db_write_cmd("update eod set o=1.5*o, h=1.5*h, l=1.5*l, c=1.5*c, "
                       "v=0.67*v where stk='COLM' and dt='2001-04-30'")
    stxdb.db_write_cmd("insert into split values ('', '', "
                       ", 0)")
    stxdb.db_write_cmd("update split set ratio=0.67 where stk='SNHY' "
                       "and dt='2011-07-15'")
