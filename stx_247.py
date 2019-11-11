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

    def get_triggered_setups(self, dt):
        q = sql.Composed([
                sql.SQL('select * from setups where dt='), sql.Literal(dt), 
                sql.SQL(' and setup in ('), sql.SQL(', ').join(
                    [sql.Literal('JC_5DAYS'), sql.Literal('JC_1234')]), 
                sql.SQL(') and triggered='), sql.Literal(True)])
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def get_setups_for_tomorrow(self, dt):
        next_dt = stxcal.next_busday(dt)
        q = sql.Composed([
                sql.SQL('select * from setups where dt='), 
                sql.Literal(next_dt), sql.SQL(' and setup in ('), 
                sql.SQL(', ').join(
                    [sql.Literal('JC_5DAYS'), sql.Literal('JC_1234')]), 
                sql.SQL(')')])
        df = pd.read_sql(q, stxdb.db_get_cnx())
        return df

    def get_high_activity(self, dt, df):
        eight_days_ago = stxcal.move_busdays(dt, -8)
        df['d_8'] = eight_days_ago
        def hiactfun(r):
            qha = sql.Composed(
                [sql.SQL('select * from setups where dt between '),
                 sql.Literal(r['d_8']), 
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
        df['hi_act'] = df.apply(hiactfun, axis=1)

    def get_opt_spreads(self, crt_date, eod):
        exp_date = stxcal.next_expiry(crt_date, min_days=(1 if eod else 0))
        q = sql.Composed([sql.SQL('select stk, opt_spread from leaders '
                                  'where expiry='), sql.Literal(exp_date)])
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            crs.execute(q.as_string(cnx))
            spread_dict = {x[0]: x[1] for x in crs}
        return spread_dict

    def filter_spreads_hiact(self, df, spreads, max_spread):
        df['spread'] = df.apply(lambda r: spreads.get(r['stk']), axis=1)
        df.drop_duplicates(['stk', 'direction'], inplace=True)
        df = df[df.spread < max_spread]
        df = df[df.hi_act >= 3]
        df.sort_values(by=['direction', 'hi_act'], ascending=False, 
                       inplace=True)
        return df

    def get_report(self, crt_date, df):
        s_date = stxcal.move_busdays(crt_date, -22)
        res = ''
        for _, row in df.iterrows():
            stk = row['stk']
            ts = StxTS(stk, s_date, crt_date)
            day_ix = ts.set_day(crt_date)
            if day_ix == -1:
                continue
            avg_volume = np.average(ts.df['v'].values[-20:])
            rgs = [max(h, c_1) - min(l, c_1) 
                   for h, l, c_1 in zip(ts.df['hi'].values[-20:], 
                                        ts.df['lo'].values[-20:], 
                                        ts.df['c'].values[-21:-1])]
            avg_rg = np.average(rgs)
            res = '{0:s}\r\n{1:6s} {2:9s} {3:6d} {4:12,d} {5:6.2f} '\
                '{6:d}'.format(res, stk, row['direction'], int(row['spread']),
                               int(1000 * avg_volume), avg_rg / 100, 
                               row['hi_act'])
        return res

    def do_analysis(self, crt_date, max_spread, eod):
        spreads = self.get_opt_spreads(crt_date, eod)
        df_1 = self.get_triggered_setups(crt_date)
        self.get_high_activity(crt_date, df_1)
        df_1 = self.filter_spreads_hiact(df_1, spreads, max_spread)
        res = 'TODAY\n=====\n'
        res += '{0:6s} {1:9s} {2:6s} {3:12s} {4:6s} {5:6s}\n'.format(
            'name', 'direction', 'spread', 'avg_volume', 'avg_rg', 'hi_act')
        res += self.get_report(crt_date, df_1)
        if eod:
            df_2 = self.get_setups_for_tomorrow(crt_date)
            self.get_high_activity(crt_date, df_2)
            df_2 = self.filter_spreads_hiact(df_2, spreads, max_spread)
            res += '\n\nTOMORROW\n========\n'
            res += self.get_report(crt_date, df_2)            
        print('{0:s}:'.format(crt_date))
        print('===========')
        print(res)
        return res

    def mail_analysis(self, analysis_results, analysis_type):
        smtp_server = os.getenv('EMAIL_SERVER')
        smtp_user = os.getenv('EMAIL_USER')
        smtp_passwd = os.getenv('EMAIL_PASSWD')
        smtp_email = os.getenv('EMAIL_USER')
        smtp_port = os.getenv('EMAIL_PORT')
        try:
            try:
                s = smtplib.SMTP(host=smtp_server, port=smtp_port)
                s.starttls()
                s.login(smtp_user, smtp_passwd)
                msg = MIMEText(analysis_results, 'plain')
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
    parser.add_argument('-s', '--max_spread', type=int, default=33,
                        help='Maximum spread for leaders')
    parser.add_argument('-d', '--date', type=str, 
                        default=stxcal.current_busdate(hr=9),
                        help='Date to retrieve setups')
    parser.add_argument('-e', '--eod', action='store_true',
                        help="Run EOD analysis")
    parser.add_argument('-i', '--intraday', action='store_true',
                        help="Run Intraday analysis")    
    parser.add_argument('-m', '--mail', action='store_true',
                        help="Email analysis results")    
    args = parser.parse_args()
    analysis_type = 'Analysis'
    eod = False
    if args.eod:
        analysis_type = 'EOD'
        eod = True
    if args.intraday:
        analysis_type = 'Intraday'
    if args.date:
        crt_date = args.date
    stx_ana = StxAnalyzer()
    analysis_results = stx_ana.do_analysis(crt_date, args.max_spread, eod)
    if args.mail:
        stx_ana.mail_analysis(analysis_results, analysis_type)
