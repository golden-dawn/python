import datetime
import pandas as pd
import schedule
import stxcal
import stxdb
from stxts import StxTS
import sys
import time


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

class Stx247:    
    '''
    1. In the constructor, create the database tables, if non-existent
    2. Schedule two runs, one at 15:30, the other at 20:00
    3. Email the analysis results:
       https://medium.freecodecamp.org/send-emails-using-code-4fcea9df63f
    4. If the current date is an option expiry, the 20:00 run should
       be generating new list of leaders.
    5. Have exclusion list, to remove stocks I don't need.
    6. 
    '''
    def __init__(self):
        self.tbl_name = 'cache'
        self.opt_tbl_name = 'opt_cache'
        self.ldr_tbl_name = 'leaders'
        self.setup_tbl_name = 'setups'
        self.sql_create_eod_tbl = "CREATE TABLE {0:s} ("\
                                  "stk varchar(8) NOT NULL,"\
                                  "dt date NOT NULL,"\
                                  "o numeric(10,2) DEFAULT NULL,"\
                                  "hi numeric(10,2) DEFAULT NULL,"\
                                  "lo numeric(10,2) DEFAULT NULL,"\
                                  "c numeric(10,2) DEFAULT NULL,"\
                                  "v integer DEFAULT NULL,"\
                                  "PRIMARY KEY (stk,dt)"\
                                  ")".format(self.tbl_name)
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
                                  "dt date NOT NULL,"\
                                  "stk varchar(16) NOT NULL,"\
                                  "exp date NOT NULL,"\
                                  "PRIMARY KEY (dt,stk)"\
                                  ")".format(self.ldr_tbl_name)
        self.sql_create_setup_tbl = "CREATE TABLE {0:s} ("\
                                    "stk varchar(8) NOT NULL,"\
                                    "dt date NOT NULL,"\
                                    "setup varchar(80) NOT NULL,"\
                                    "PRIMARY KEY (stk,dt)"\
                                  ")".format(self.setup_tbl_name)
        stxdb.db_create_missing_table(self.tbl_name, self.sql_create_eod_tbl)
        stxdb.db_create_missing_table(self.opt_tbl_name,
                                      self.sql_create_opt_tbl)
        stxdb.db_create_missing_table(self.ldr_tbl_name,
                                      self.sql_create_ldr_tbl)
        stxdb.db_create_missing_table(self.setup_tbl_name,
                                      self.sql_create_setup_tbl)
        self.df_dct = {}

    def intraday_job(self):
        print('247 intraday job')

    def eod_job(self):
        print('247 end of day job')
        max_dt_q = stxdb.db_read_cmd('select max(dt) from setups')
        if max_dt_q[0][0] is not None:
            ana_date = str(max_dt_q[0][0])
        crt_date = str(datetime.datetime.now().date())
        self.ts_dct = {}
        self.start_date = '1985-01-01'
        self.end_date = stxcal.move_busdays(str(
            datetime.datetime.now().date()), 1)
        while ana_date <= crt_date:
            self.analyze(ana_date)
            ana_date = stxcal.move_busdays(stxcal.next_expiry(ana_date), 0)

    def eow_job(self):
        print('247 end of week job')
        print('247 end of week job')
        ana_date = '2002-04-19'
        crt_date = '2002-07-19'
        max_dt_q = stxdb.db_read_cmd('select max(dt) from leaders')
        if max_dt_q[0][0] is not None:
            ana_date = str(max_dt_q[0][0])
        crt_date = str(datetime.datetime.now().date())
        self.ts_dct = {}
        self.start_date = '1985-01-01'
        self.end_date = stxcal.move_busdays(str(
            datetime.datetime.now().date()), 1)
        while ana_date <= crt_date:
            self.get_leaders(ana_date)
            ana_date = stxcal.move_busdays(stxcal.next_expiry(ana_date), 0)

    def get_leaders(self, ana_date):
        stk_list = stxdb.db_read_cmd("select distinct stk from eods where "
                                     "date='{0:s}'".format(ana_date))
        all_stocks = []
        for s in stk_list:
            sn = s[0]
            if not sn.startswith('^') and not sn.startswith('#'):
                all_stocks.append(sn)
        print('Found {0:d} stocks for {1:s}'.format(len(all_stocks), ana_date))
        next_exp = stxcal.next_expiry(ana_date)
        next_exp_busday = stxcal.move_busdays(next_exp, 0)
        num_stx = 0
        num = 0
        leaders = []
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
            if self.is_leader(ts, ana_date, next_exp):
                leaders.append(s)
            if num % 1000 == 0 or num == len(all_stocks):
                print('Processed {0:d} stocks, found {1:d} leaders'.
                      format(num, len(leaders)))
        leaders.sort()
        print('Found {0:d} leaders for {1:s}'.format(len(leaders), ana_date))
        # print('{0:s} leaders: {1:s}'.format(ana_date, ','.join(leaders)))
        print('Loaded {0:d} stocks for {1:s}'.format(num_stx, ana_date))
        ldr_fname = '/tmp/leaders.txt'
        with open(ldr_fname, 'w') as f:
            crs_date = ana_date
            while crs_date < next_exp_busday:
                for ldr in leaders:
                    f.write('{0:s}\t{1:s}\t{2:s}\n'.
                            format(crs_date, ldr, next_exp))
                crs_date = stxcal.next_busday(crs_date)
        stxdb.db_upload_file(ldr_fname, self.ldr_tbl_name)
        
    def is_leader(self, ts, ana_date, next_exp):
        ts.set_day(ana_date)
        if ts.pos < 50:
            return False
        if ts.current_date() == ana_date and ts.current('avg_act') >= 80000 \
           and ts.current('rg_c_ratio') >= 0.015:
            tokens = ts.stk.split('.')
            if tokens[-1].isdigit():
                und = '.'.join(tokens[:-1])
            else:
                und = ts.stk
            opt_q = "select count(*) from options where expiry='{0:s}' " \
                    "and und='{1:s}' and date = '{2:s}'".format(
                        next_exp, und, ana_date)
            num_opts = stxdb.db_read_cmd(opt_q)
            if num_opts[0][0] > 0:
                return True
        return False

        
if __name__ == '__main__':
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
    schedule.every().friday.at("23:00").do(s247.eow_job)
    while True:
        schedule.run_pending()
        time.sleep(1)
