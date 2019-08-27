import argparse
from psycopg2 import sqlimport datetime
from email.mime.text import MIMEText
import json
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

class StxMail:

    def __init__(self):
        pass

#         for stk in stx:
#             ts = StxTS(stk, self.start_date, ana_date)
#             for ixx in range(1, 5):
#                 ts.df['hi_{0:d}'.format(ixx)] = ts.df['hi'].shift(ixx)
#                 ts.df['lo_{0:d}'.format(ixx)] = ts.df['lo'].shift(ixx)
#             ts.df['v_50'] = ts.df['volume'].rolling(50).mean()

    def mail_analysis(self, analysis_type):
        crt_date = stxcal.current_busdate(hr=9)
        exp_date = stxcal.next_expiry(crt_date)
        smtp_server = os.getenv('EMAIL_SERVER')
        smtp_user = os.getenv('EMAIL_USER')
        smtp_passwd = os.getenv('EMAIL_PASSWD')
        smtp_email = os.getenv('EMAIL_USER')
        smtp_port = os.getenv('EMAIL_PORT')
        res = '{0:6s} {1:9s} {2:6s}'.format(
            'stock', 'direction', 'spread')
        #      dt     |  stk  |  setup   | direction | triggered 
        # ------------+-------+----------+-----------+-----------
        #  2019-08-21 | MSM   | JC_1234  | D         | t
        q1 = sql.Composed([sql.SQL('select * from setups where dt='),
                          sql.Literal(crt_date)])
        df = pd.read_sql(q1, stxdb.db_get_cnx())
        q2 = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
                                   'where expiry='), sql.Literal(exp_date)])
        cnx = stxdb.db_get_cnx())
        with cnx.cursor() as crs:
            crs.execute(q2.as_string(cnx))
            spread_dict = {x[0]: x[1] for x in crs}
        df['spread'] = df.apply(lambda r: spread_dict.get(r['stk']))
        df.drop_duplicates(['stk', 'direction'], inplace=True)
        df.sort_values(by=['direction', 'spread'], inplace=True)
        for _, row in df.iterrows():
            res = '{0:s}\r\n{1:6s} {2:9s} {3:6d}'.format(
                    res, row['stk'], row['direction'], row['spread'])
#         for _, row in analysis.iterrows():
#             res = '{0:s}\r\n{1:s} {2:6s} {3:12s} {4:6.2f} {5:11d}'.format(
#                 res, str(row['date']), row['stk'], row['setup'], row['rg'],
#                 int(row['v_50']))
        try:
            try:
                s = smtplib.SMTP(host=smtp_server, port=smtp_port)
                s.starttls()
                s.login(smtp_user, smtp_passwd)
                msg = MIMEText(res, 'plain')
                msg['Subject'] = '{0:s} {1:s}'.format(analysis_type, crt_date)
                msg['From'] = smtp_email
                msg['To'] = smtp_email
                s.sendmail(smtp_email, smtp_email, msg.as_string())
            except:
                print('Something failed: {0:s}'.format(traceback.print_exc()))
            finally:
                s.quit()
        except:
            print('Failed to send email: {0:s}'.format(traceback.print_exc()))
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--leaders', action='store_true',
                        help='Require leader calculation')
    parser.add_argument('-s', '--setups', action='store_true',
                        help='Require setup calculation')
    parser.add_argument('-o', '--get-options', action='store_true',
                        help='Retrieve option prices')
    parser.add_argument('-a', '--min_act', type=int,
                        help='Minimum activity for leaders')
    parser.add_argument('-r', '--range_close_ratio', type=float,
                        help='Minimum range to close ratio for leaders')
    parser.add_argument('-e', '--eod', action='store_true',
                        help="Run EOD analysis")
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    
    args = parser.parse_args()
    analysis_type = 'EOD'
    if args.eod:
        analysis_type = 'EOD'
    if args.intraday:
        analysis_type = 'Intraday'
    stx_mail = StxMail()
    stx_mail.mail_analysis(analysis_type)
