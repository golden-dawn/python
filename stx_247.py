import argparse
import datetime
from email.mime.text import MIMEText
import json
import numpy as np
import os
import pandas as pd
from psycopg2 import sql
import re
import smtplib
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS
import sys
import time
import traceback

class StxAnalyzer:

    def __init__(self):
        pass

#         for stk in stx:
#             ts = StxTS(stk, self.start_date, ana_date)
#             for ixx in range(1, 5):
#                 ts.df['hi_{0:d}'.format(ixx)] = ts.df['hi'].shift(ixx)
#                 ts.df['lo_{0:d}'.format(ixx)] = ts.df['lo'].shift(ixx)
#             ts.df['v_50'] = ts.df['volume'].rolling(50).mean()

    def do_analysis(self, crt_date, max_spread):
        exp_date = stxcal.next_expiry(crt_date)
        five_days_ago = stxcal.move_busdays(crt_date, -5)
        res = '{0:6s} {1:9s} {2:6s} {3:12s} {4:6s}'.format(
            'name', 'direction', 'spread', 'avg_volume', 'avg_rg')
        #      dt     |  stk  |  setup   | direction | triggered 
        # ------------+-------+----------+-----------+-----------
        #  2019-08-21 | MSM   | JC_1234  | D         | t
        q1 = sql.Composed([sql.SQL('select * from setups where dt='),
                           sql.Literal(crt_date), sql.SQL('and setup in ('),
                           sql.Literal('JC_5DAYS'), sql.SQL(','), 
                           sql.Literal('JC_1234'), sql.SQL(')')])
        df = pd.read_sql(q1, stxdb.db_get_cnx())
        df['d_5'] = five_days_ago

        def hiactfun(r):
            qha = sql.Composed(
                [sql.SQL('select * from setups where dt between '),
                 sql.Literal(r['d_5']), 
                 sql.SQL(' and '),
                 sql.Literal(r['dt']),
                 sql.SQL(' and stk='),
                 sql.Literal(r['stk']),
                 sql.SQL(' and setup in ('),
                 sql.SQL(',').join([sql.Literal('GAP_HV'), 
                                    sql.Literal('STRONG_CLOSE')]),
                 sql.SQL(')')])
            cnx = stxdb.db_get_cnx()
            rows = []
            with cnx.cursor() as crs:
                crs.execute(qha.as_string(cnx))
                rows = crs.fetchall()
            return len(rows) if rows else 0
        df['ha'] = df.apply(hiactfun, axis=1)

        q2 = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
                                   'where expiry='), sql.Literal(exp_date)])
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            crs.execute(q2.as_string(cnx))
            spread_dict = {x[0]: x[1] for x in crs}
        df['spread'] = df.apply(lambda r: spread_dict.get(r['stk']), axis=1)
        df.drop_duplicates(['stk', 'direction'], inplace=True)
        df = df[df.spread < max_spread]
        df.sort_values(by=['direction', 'spread'], inplace=True)
        s_date = stxcal.move_busdays(crt_date, -49)
        for _, row in df.iterrows():
            stk = row['stk']
            ts = StxTS(stk, s_date, crt_date)
            avg_volume = np.average(ts.df['v'].values[:])
            rgs = [max(h, c_1) - min(l, c_1) 
                   for h, l, c_1 in zip(ts.df['hi'].values[-20:], 
                                        ts.df['lo'].values[-20:], 
                                        ts.df['c'].values[-21:-1])]
            avg_rg = np.average(rgs)

            res = '{0:s}\r\n{1:6s} {2:9s} {3:6d} {4:12,d} {5:6.2f} '\
                '{6:d}'.format(res, stk, row['direction'], row['spread'],
                               int(1000 * avg_volume), avg_rg / 100, row['ha'])
        print('{0:s}:'.format(crt_date))
        print('===========')
        print(res)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--max_spread', type=int, default=33,
                        help='Maximum spread for leaders')
    parser.add_argument('-d', '--date', type=str, 
                        default=stxcal.current_busdate(),
                        help='Date to retrieve setups')
    args = parser.parse_args()
    stx_ana = StxAnalyzer()
    stx_ana.do_analysis(args.date, args.max_spread)
