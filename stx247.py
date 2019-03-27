import argparse
import datetime
from email.mime.text import MIMEText
import json
import os
import pandas as pd
from psycopg2 import sql
import re
import requests
import schedule
import smtplib
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS
import sys
import time
import traceback

old_out = sys.stdout


class St_ampe_dOut:
    """Stamped stdout."""
    nl = True
    def write(self, x):
        """Write function overloaded."""
        if x == '\n':
            old_out.write(x)
            self.nl = True
        elif self.nl:
            old_out.write('%s:: %s' % (str(datetime.datetime.now()), x))
            self.nl = False
        else:
            old_out.write(x)
sys.stdout = St_ampe_dOut()


def valid_date(s):
    try:
        return str(datetime.datetime.strptime(s, "%Y-%m-%d").date())
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

class Stx247:    
    '''
    2. Schedule two runs, one at 15:30, the other at 20:00
    3. Email the analysis results:
       https://medium.freecodecamp.org/send-emails-using-code-4fcea9df63f
    4. If the current date is an option expiry, the 20:00 run should
       be generating new list of leaders.
    '''
    yhoo_url = 'https://query1.finance.yahoo.com/v7/finance/options/{0:s}?' \
               'formatted=true&crumb=BfPVqc7QhCQ&lang=en-US&region=US&' \
               'date={1:d}&corsDomain=finance.yahoo.com'

    def __init__(self, max_atm_price=5.0, num_stx=150):
        self.tbl_name = 'eods'
        self.opt_tbl_name = 'opt_cache'
        self.ldr_tbl_name = 'leaders'
        self.setup_tbl_name = 'setups'
        self.exclude_tbl_name = 'exclusions'
        self.sql_create_opt_tbl = "CREATE TABLE {0:s} ("\
                                  'expiry date NOT NULL,'\
                                  'und character varying(16) NOT NULL,'\
                                  'cp character varying(1) NOT NULL,'\
                                  'strike numeric(10,2) NOT NULL,'\
                                  'dt date NOT NULL,'\
                                  'bid numeric(10,2),'\
                                  'ask numeric(10,2),'\
                                  'volume integer,'\
                                  'PRIMARY KEY (expiry,und,cp,strike,dt)'\
                                  ')'.format(self.opt_tbl_name)
        self.sql_create_ldr_tbl = "CREATE TABLE {0:s} ("\
                                  "exp date NOT NULL,"\
                                  "stk varchar(16) NOT NULL,"\
                                  "activity integer DEFAULT NULL,"\
                                  "opt_spread integer DEFAULT NULL,"\
                                  "atm_price numeric(6,2) DEFAULT NULL,"\
                                  "PRIMARY KEY (exp,stk)"\
                                  ")".format(self.ldr_tbl_name)
        self.sql_create_setup_tbl = "CREATE TABLE {0:s} ("\
                                    "stk varchar(8) NOT NULL,"\
                                    "dt date NOT NULL,"\
                                    "setup varchar(80) NOT NULL,"\
                                    "PRIMARY KEY (stk,dt)"\
                                  ")".format(self.setup_tbl_name)
        self.sql_create_exclude_tbl = "CREATE TABLE {0:s} ("\
                                      "stk varchar(8) NOT NULL,"\
                                      "PRIMARY KEY (stk)"\
                                      ")".format(self.exclude_tbl_name)
        stxdb.db_create_missing_table(self.opt_tbl_name,
                                      self.sql_create_opt_tbl)
        stxdb.db_create_missing_table(self.ldr_tbl_name,
                                      self.sql_create_ldr_tbl)
        stxdb.db_create_missing_table(self.setup_tbl_name,
                                      self.sql_create_setup_tbl)
        stxdb.db_create_missing_table(self.exclude_tbl_name,
                                      self.sql_create_exclude_tbl)
        # calculate the last date for which we have historical options
        prev_year = datetime.datetime.now().date().year - 1
        self.last_opt_date = stxcal.move_busdays(
            '{0:d}-12-31'.format(prev_year), 0)
        self.ts_dct = {}        
        self.start_date = '1985-01-01'
        self.end_date = stxcal.move_busdays(str(
            datetime.datetime.now().date()), 1)
        self.max_atm_price = max_atm_price
        self.num_stx = num_stx
        
    def intraday_job(self):
        # download data only for option spread leaders
        # calculate setups, include triggered setups
        # figure out how to email the results
        print('247 intraday job')
        self.intraday_analysis()

    def intraday_analysis(self):
        self.get_data(ana_date, get_for_all=False, save_eods=True,
                      save_opts=False)
        self.mail_analysis(analysis)

    def eod_job(self):
        print('247 end of day job')
        ana_date = stxcal.current_busdate(hr=10)
        print('    ana_date = {0:s}'.format(ana_date))
        self.eod_analysis(ana_date)

    def eod_analysis(self, ana_date):
        # special case when the date is an option expiry date:
        #   1. wait until eoddata is downloaded.
        #   2. calculate liquidity leaders
        #   3. download options for all liquidity leaders
        #   4. calculate option spread leaders
        #   5. populate leaders table
        #   6. MON vs. FRI
        next_exp = stxcal.next_expiry(ana_date, 0)
        if next_exp == ana_date:
            res = stxdb.db_read_cmd('select max(date) from eods where '
                                    'open_interest>=0')
            last_eod_date = str(res[0][0])
            while last_eod_date < ana_date:
                print('Could not find eod data for {0:s}, sleeping one hour'.
                      format(ana_date))
                time.sleep(3600)
                res = stxdb.db_read_cmd('select max(date) from eods where '
                                        'open_interest>=0')
                last_eod_date = str(res[0][0])
            self.get_liq_leaders(ana_date)
            self.get_data(ana_date, get_eod=False, get_for_all=True)
            self.get_opt_spread_leaders(ana_date)
        else:
            self.get_data(ana_date, get_for_all=False, get_eod=True)
        setup_df = self.calc_setups(ana_date)
        self.mail_analysis(setup_df)
            
    def eow_job(self):
        print('247 end of week job')
        print('247 end of week job')
        ana_date = '2002-04-19'
        crt_date = '2002-07-19'
        max_dt_q = stxdb.db_read_cmd('select max(dt) from leaders')
        if max_dt_q[0][0] is not None:
            ana_date = str(max_dt_q[0][0])
        crt_date = str(datetime.datetime.now().date())
        self.end_date = stxcal.move_busdays(str(
            datetime.datetime.now().date()), 1)
        while ana_date <= crt_date:
            self.get_liq_leaders(ana_date)
            self.get_opt_spread_leaders(ana_date)
            ana_date = stxcal.move_busdays(stxcal.next_expiry(ana_date), 0)

    def get_liq_leaders(self, ana_date, min_act=80000, min_rcr=0.015):
        stk_list = stxdb.db_read_cmd("select distinct stk from eods where "
                                     "date='{0:s}' order by stk".
                                     format(ana_date))
        all_stocks = [s[0] for s in stk_list
                      if re.match(r'^[A-Za-z]', str(s[0]))]
        print('Found {0:d} stocks for {1:s}'.format(len(all_stocks), ana_date))
        next_exp = stxcal.next_expiry(ana_date)
        next_exp_busday = stxcal.move_busdays(next_exp, 0)
        num_stx = 0
        num = 0
        liq_leaders = []
        for s in all_stocks:
            num+= 1
            ts = self.ts_dct.get(s)
            if ts is None:
                ts = StxTS(s, self.start_date, self.end_date)
                ts.set_day(str(ts.df.index[-1].date()))
                ts.df['activity'] = ts.df['volume'] * ts.df['c']
                ts.df['avg_act'] = ts.df['activity'].rolling(50).mean()
                ts.df['rg'] = ts.df['hi'] - ts.df['lo']
                ts.df['avg_rg'] = ts.df['rg'].rolling(50).mean()
                ts.df['rg_c_ratio'] = ts.df['avg_rg'] / ts.df['c']
                self.ts_dct[s] = ts
                num_stx += 1
            stk_act = [s]
            if self.is_liq_leader(ts, ana_date, min_act, min_rcr, stk_act):
                liq_leaders.append(stk_act)
            if num % 1000 == 0 or num == len(all_stocks):
                print('Processed {0:d} stocks, found {1:d} liquidity leaders'.
                      format(num, len(liq_leaders)))
        print('Found {0:d} liquidity leaders for {1:s}'.format(
            len(liq_leaders), ana_date))
        print('Loaded {0:d} stocks for {1:s}'.format(num_stx, ana_date))
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            for ldr in liq_leaders:
                crs.execute(
                    'insert into leaders(exp,stk,activity,opt_spread) values ' +
                    crs.mogrify('(%s,%s,%s,%s)',
                                [next_exp, ldr[0], int(ldr[1]), -1000]) +
                    'on conflict do nothing')

    def get_leaders(self, ldr_date, get_for_all=True):
        ldr_expiry = stxcal.next_expiry(ldr_date)
        cnx = stxdb.db_get_cnx()
        if get_for_all:
            q = sql.Composed([sql.SQL('select stk from leaders where exp='),
                              sql.Literal(ldr_expiry)])
        else:
            q = sql.Composed(
                [sql.SQL('select stk from leaders where exp='),
                 sql.Literal(ldr_expiry),
                 sql.SQL(' and opt_spread >= 0 and atm_price is not null '
                         'and atm_price<='), sql.Literal(self.max_atm_price),
                 sql.SQL('and stk not in (select * from exclusions) '
                         'order by opt_spread asc limit '),
                 sql.Literal(self.num_stx)])
        with cnx.cursor() as crs:
            crs.execute(q.as_string(cnx))
            ldrs = [x[0] for x in crs]
        return ldrs

    def calc_setups(self, ana_date):
        setup_df = pd.DataFrame(columns=['date', 'stk', 'setup', 'triggered'])
        # factors = [1.0, 1.5, 2.0]
        factors = [2.0]
        stx = self.get_leaders(ana_date, get_for_all=False)
        for stk in stx:
            ts = StxTS(stk, self.start_date, ana_date)
            for ixx in range(1, 5):
                ts.df['hi_{0:d}'.format(ixx)] = ts.df['hi'].shift(ixx)
                ts.df['lo_{0:d}'.format(ixx)] = ts.df['lo'].shift(ixx)
            ts.df['v_50'] = ts.df['volume'].rolling(50).mean()
            jl_list = []
            for factor in factors:
                jl = StxJL(ts, factor)
                jl.jl(ana_date)
                jl_list.append(jl)
            setup_df = self.setups(ts, jl_list, setup_df)
            print('Finished {0:s}'.format(stk))
        # setup_df = setup_df.sort_values(by=['date', 'setup', 'stk'])
        for _, row in setup_df.iterrows():
            print('{0:s} {1:12s} {2:s}'.
                  format(row['date'], row['stk'], row['setup']))
        return setup_df
            

    def analyze(self, exp):
        # 1. Select all the leaders for that expiry
        # 2. Run StxJL for each leader, for each factor
        # ldr_list = self.get_leaders(
        setup_df = pd.DataFrame(columns=['date', 'stk', 'setup'])
        factors = [1.0, 1.5, 2.0]
        q1 = "select min(dt), max(dt) from leaders where exp='{0:s}'".format(
            exp)
        date_list = stxdb.db_read_cmd(q1)
        exp_dt = datetime.datetime.strptime(exp, '%Y-%m-%d')
        jls_date = stxcal.next_busday('{0:d}-01-01'.format(exp_dt.year))
        jle_date = stxcal.move_busdays(exp, 0)
        q2 = "select distinct stk from leaders where exp='{0:s}'".format(exp)
        ldr_list = stxdb.db_read_cmd(q2)
        for ldr in ldr_list:
            s_date = str(date_list[0][0])
            e_date = str(date_list[0][1])
            stk = ldr[0]
            ts = StxTS(stk, jls_date, jle_date)
            for ixx in range(1, 5):
                ts.df['hi_{0:d}'.format(ixx)] = ts.df['hi'].shift(ixx)
                ts.df['lo_{0:d}'.format(ixx)] = ts.df['lo'].shift(ixx)
            
            jl_list = []
            for factor in factors:
                jl = StxJL(ts, factor)
                jl.jl(s_date)
                jl_list.append(jl)
            while s_date < e_date:
                setup_df = self.setups(ts, jl_list, setup_df)
                ts.next_day()
                for jl in jl_list:
                    jl.nextjl()
                s_date = stxcal.next_busday(s_date)
            print('Finished {0:s}'.format(stk))
        setup_df = setup_df.sort_values(by=['date', 'setup', 'stk'])
        for _, row in setup_df.iterrows():
            print('{0:s} {1:12s} {2:s}'.
                  format(row['date'], row['stk'], row['setup']))

    def setups(self, ts, jl_list, setup_df):
        # print('setups {0:s},{1:s}'.format(ts.stk, ts.current_date()))
        # jl10, jl15, jl20 = jl_list
        jl20 = jl_list[0]
        l20 = jl20.last
        if l20['prim_state'] == StxJL.UT and l20['prim_state'] == l20['state']:
            if ts.current('hi') < ts.current('hi_1') and \
               ts.current('hi_1') < ts.current('hi_2') and \
               ts.current('hi_2') < ts.current('hi_3'):
                print('++1234++: {0:s} {1:s}'.format(ts.stk, ts.current_date()))
                setup_df = setup_df.append(
                    dict(date=ts.current_date(), stk=ts.stk, setup='++1234++',
                    rg=jl20.avg_rg, v_50=ts.current('v_50')),
                    ignore_index=True)
        if l20['prim_state'] == StxJL.DT and l20['prim_state'] == l20['state']:
            if ts.current('lo') > ts.current('lo_1') and \
               ts.current('lo_1') > ts.current('lo_2') and \
               ts.current('lo_2') > ts.current('lo_3'):
                print('--1234--: {0:s} {1:s}'.format(ts.stk, ts.current_date()))
                setup_df = setup_df.append(
                    dict(date=ts.current_date(), stk=ts.stk, setup='--1234--',
                         rg=jl20.avg_rg, v_50=ts.current('v_50')),
                    ignore_index=True)
        return setup_df

    def is_liq_leader(self, ts, ana_date, min_act, min_rcr, stk_act):
        ts.set_day(ana_date)
        if ts.pos < 50:
            return False
        if ts.current_date() == ana_date and ts.current('avg_act') >= min_act \
           and ts.current('rg_c_ratio') >= min_rcr:
            stk_act.append(ts.current('avg_act'))
            return True
        return False

    def get_opt_spread_leaders(self, ldr_date):
        next_exp = stxcal.next_expiry(ldr_date)
        calc_exp = stxcal.next_expiry(ldr_date, 9)
        crt_date = stxcal.current_busdate()
        cnx = stxdb.db_get_cnx()
        stx = self.get_leaders(ldr_date)
        print('Calculating option spread for {0:d} stocks'.format(len(stx)))
        num = 0
        if ldr_date <= self.last_opt_date:
            opt_tbl_name = 'options'
            spot_tbl_name = 'opt_spots'
            spot_column = 'spot'
            opt_date_column = 'date'
        else:
            opt_tbl_name = 'opt_cache'
            spot_tbl_name = 'eods'
            spot_column = 'c'
            opt_date_column = 'dt'
        for stk in stx:
            print('stk = {0:s}'.format(stk))
            spot_q = sql.Composed([sql.SQL('select {} from {} where stk=').
                                   format(sql.Identifier(spot_column),
                                          sql.Identifier(spot_tbl_name)),
                                   sql.Literal(stk), sql.SQL(' and date='),
                                   sql.Literal(crt_date)])
            with cnx.cursor() as crs:
                crs.execute(spot_q.as_string(cnx))
                spot_res = crs.fetchone()
                if spot_res is None:
                    continue
                spot = float(spot_res[0])
            tokens = stk.split('.')
            und = '.'.join(tokens[:-1]) if tokens[-1].isdigit() else stk
            opt_q = sql.Composed(
                [sql.SQL('select * from {} where expiry=').
                 format(sql.Identifier(opt_tbl_name)),
                 sql.Literal(calc_exp), sql.SQL(' and und='), sql.Literal(und),
                 sql.SQL(' and {}=').format(sql.Identifier(opt_date_column)),
                 sql.Literal(crt_date)])
            opt_df = pd.read_sql(opt_q.as_string(cnx), cnx)
            if len(opt_df) < 6:
                continue
            opt_df['strike_spot'] = abs(opt_df['strike'] - spot)
            opt_df['spread'] = 100 * (1 - opt_df['bid'] / opt_df['ask'])
            opt_df.sort_values(by=['strike_spot'], inplace=True)
            opt_df['avg_spread'] = opt_df['spread'].rolling(6).mean()
            try:
                avg_spread = int(opt_df.iloc[5].avg_spread * 100)
                avg_atm_price = round(
                    (opt_df.iloc[0].ask + opt_df.iloc[1].ask) / 2, 2)
                with cnx.cursor() as crs:
                    crs.execute('update leaders set opt_spread=%s, '
                                'atm_price=%s where stk=%s and exp=%s',
                                (avg_spread, avg_atm_price, stk, next_exp))
            except:
                print('Failed to calc avg_spread for {0:s}'.format(stk))
            num += 1
            if num % 100 == 0 or num == len(stx):
                print('Calculated option spread for {0:d} stocks'.format(num))

    def get_data(self, crt_date, get_eod=True, get_for_all=True):
        expiries = stxcal.long_expiries()
        cnx = stxdb.db_get_cnx()
        ldrs = self.get_leaders(crt_date, get_for_all)
        exp_dates = [str(datetime.datetime.utcfromtimestamp(x).date())
                     for x in expiries[:3]]
        six = 1 if exp_dates[0] < crt_date else 0
        for ldr in ldrs:
            self.get_stk_data(ldr, crt_date, expiries[six], exp_dates[six],
                              save_eod=get_eod)
            self.get_stk_data(ldr, crt_date, expiries[six + 1],
                              exp_dates[six + 1], save_eod=False)

    def get_stk_data(self, stk, crt_date, expiry, exp_date, save_eod=False,
                     save_opts=True):
        res = requests.get(self.yhoo_url.format(stk, expiry))
        if res.status_code != 200:
            print('Failed to get {0:s} data for {1:s}: {2:d}'.
                  format(exp_date, stk, res.status_code))
            return
        res_json = json.loads(res.text)
        res_0 = res_json['optionChain']['result'][0]
        quote = res_0.get('quote', {})
        c = quote.get('regularMarketPrice', -1)
        if c == -1:
            print('Failed to get closing price for {0:s}'.format(stk))
            return
        if save_eod:
            v = quote.get('regularMarketVolume', -1)
            o = quote.get('regularMarketOpen', -1)
            hi = quote.get('regularMarketDayHigh', -1)
            lo = quote.get('regularMarketDayLow', -1)
            if o == -1 or hi == -1 or lo == -1 or v == -1:
                print('Failed to get EOD quote for {0:s}'.format(stk))
            else:
                stxdb.db_insert_eods([[stk, crt_date, o, hi, lo, c, v / 1000,
                                       -1]])
        if not save_opts:
            return
        opts = res_0.get('options', [{}])
        calls = opts[0].get('calls', [])
        puts = opts[0].get('puts', [])
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            for call in calls:
                crs.execute('insert into opt_cache values' +
                            crs.mogrify(
                                '(%s,%s,%s,%s,%s,%s,%s,%s)',
                                [call['expiration']['fmt'], stk, 'c',
                                 call['strike']['raw'], crt_date,
                                 call['bid']['raw'], call['ask']['raw'],
                                 call['volume']['raw']]) +
                            'on conflict do nothing')
            for put in puts:
                crs.execute('insert into opt_cache values' +
                            crs.mogrify(
                                '(%s,%s,%s,%s,%s,%s,%s,%s)',
                                [put['expiration']['fmt'], stk, 'p',
                                 put['strike']['raw'], crt_date,
                                 put['bid']['raw'], put['ask']['raw'],
                                 put['volume']['raw']]) +
                            'on conflict do nothing')
        print('Got {0:d} calls and {1:d} puts for {2:s} exp {3:s}'.format(
            len(calls), len(puts), stk, exp_date))

    def mail_analysis(analysis):
        smtp_fname = '{0:s}/.smtp.cfg'.format(os.getenv('HOME'))
        with open(filename, 'r') as f:
            lines = f.readlines()
        smtp_server = lines[0].strip()
        smtp_user = lines[1].strip()
        smtp_passwd = lines[2].strip()
        smtp_email = lines[3].strip()
        smtp_port = 587
        res = ''
        for _, row in analysis.iterrows():
            res = '{0:s}\r\n{1:s} {2:6s} {3:12s} {4:6.2f} {5:11d}'.format(
                res, str(res['date']), res['stk'], res['setup'], res['rg'],
                int(res['v_50']))
        try:
            try:
                s = smtplib.SMTP(host=smtp_server, port=smtp_port)
                s.starttls()
                s.login(smtp_user, smtp_passwd)
                msg = MIMEText(res, 'plain')
                msg['Subject'] = 'IDA {0:s}'.format(
                    stxcal.current_busdate(hr=12))
                msg['From'] = smtp_email
                msg['To'] = smtp_email
                s.sendmail(smtp_email, smtp_email, msg.as_string())
            finally:
                s.quit()
        except:
            print('Failed to send email: {0:s}'.traceback.print_exc())
        

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
    parser.add_argument('-d', '--ldr_date',  type=valid_date,
                        help="The date for leaders - format YYYY-MM-DD")
    parser.add_argument('-e', '--eod', action='store_true',
                        help="Run EOD analysis")
    
    args = parser.parse_args()
    if args.leaders:
        ldr_date = args.ldr_date if args.ldr_date else stxcal.current_busdate()
        min_act = args.min_act if args.min_act else 80000
        min_rcr = args.range_close_ratio if args.range_close_ratio else 0.015
        print('Will calculate leaders for {0:s} w/ min act of {1:d} and '
              'min range to close ratio of {2:.3f}'.format(ldr_date, min_act,
                                                           min_rcr))
        s247= Stx247()
        s247.get_liq_leaders(ldr_date, min_act, min_rcr)
        if args.get_options:
            print('Will retrieve options and calculate spread liquidity')
            s247.get_opt_spread_leaders(ldr_date)
        exit(0)
    if args.eod:
        s247= Stx247()
        s247.eod_job()
        exit(0)

    s247= Stx247()
    s247.eow_job()
    schedule.every().monday.at("15:30").do(s247.intraday_job)
    schedule.every().tuesday.at("15:30").do(s247.intraday_job)
    schedule.every().wednesday.at("15:30").do(s247.intraday_job)
    schedule.every().thursday.at("15:30").do(s247.intraday_job)
    schedule.every().friday.at("15:30").do(s247.intraday_job)
    schedule.every().monday.at("21:00").do(s247.eod_job)
    schedule.every().tuesday.at("21:00").do(s247.eod_job)
    schedule.every().wednesday.at("21:00").do(s247.eod_job)
    schedule.every().thursday.at("21:00").do(s247.eod_job)
    schedule.every().friday.at("21:00").do(s247.eod_job)
    schedule.every().friday.at("23:00").do(s247.eod_job)
    while True:
        schedule.run_pending()
        time.sleep(1)

'''
exp = '2002-05-18'
from stx247 import Stx247
s247 = Stx247()
s247.analyze(exp)
'''
