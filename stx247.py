import datetime
import pandas as pd
import schedule
import stxcal
import stxdb
import time

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
                                  "stk varchar(8) NOT NULL,"\
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

    def eow_job(self):
        print('247 end of week job')
        ana_date = '2002-03-15'
        max_dt_q = stxdb.db_read_cmd('select max(dt) from leaders')
        if max_dt_q[0][0] is not None:
            ana_date = str(max_dt_q[0][0])
        crt_date = str(datetime.datetime.now().date())
        self.ts_dct = {}
        start_date = '1985-01-01'
        end_date = stxcal.move_busdays(str(datetime.datetime.now().date()), 1)
        while ana_date <= crt_date:
            self.analyze(ana_date,start_date, end_date)
            ana_date = stxcal.move_busdays(stxcal.next_expiry(ana_date), 0)

    def analyze(self, ana_date, start_date, end_date):
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
        for s in all_stocks:
            ts = self.ts_dct.get(s)
            if ts is None:
                ts = StxTS(s, start_date, end_date)
                ts_dct[s] = ts
                
        start_date = stxcal.move_busdays(ana_date, -50)
        q = "select stk from eods where date='{0:s}'".format(ana_date)
        df = pd.read_sql(q, stxdb.db_get_cnx())
        df['activity'] = df['volume'] * df['c']
        stx = df['stk'].unique().tolist()
        ix = 0
        leaders = []
        for stk in stx:
            dfs = df.query('stk==@stk')
            dfs['avg_act'] = dfs['activity'].rolling(50).mean()
            dfs['rg'] = dfs['hi'] - dfs['lo']
            dfs['avg_rg'] = dfs['rg'].rolling(50).mean()
            last_rec = dfs.iloc[-1]
            if last_rec.date == ana_date && last_rec.avg_activity >= 100000 &&\
               last_rec.avg_rg >= 0.015 * last_rec.c:
                opt_q = "select count(*) from options where expiry='{0:s}' " \
                        "and und='{1:s}' and dt = '{2:s}'".format(
                            next_exp, stk, ana_date)
                num_opts = stxdb.db_read_cmd(opt_q)
                if num_opts[0][0] > 0:
                    leaders.append(stk)
        upload_fname = '/tmp/leaders.txt'
        with open(upload_fname, 'w') as f:
            crt_date = ana_date
            while crt_date < mext_exp_busday:
                for stk in leaders:
                    f.write('{0:s}\t{1:s}\t{2:s}\n'.format(
                        str(crt_date), stk, str(next_expiry)))
                ctr_date = stxcal.next_busday(crt_date)
        stxdb.db_upload_file(upload_fname, 'leaders')
        print('Uploaded {0:d} leaders from {1:s} until {2:s}'.
              format(len(leaders), ana_date, next_exp_busday))
                
        # print('Found {0:d} stocks'.format(len(df)))
        # df['rg'] = df['hi'] - df['lo']
        # df_1 = df.query('volume>1000 & c>30 & c<500 & rg>0.015*c')
        # stx = df_1['stk'].tolist()
        # print('Found {0:d} leaders'.format(len(stx)))
        # start_date = stxcal.move_busdays(selected_date, -60)
        # print('start_date is: {0:s}'.format(str(start_date)))
        # ixx = 0
        # for stk in stx:
        #     ixx += 1
        #     ts = StxTS(stk, start_date, selected_date)
        #     # adjust the whole thing for splits, etc.
        #     ts.set_day(str(ts.df.index[-1].date()))
        #     ts.df['hi_1'] = ts.df['hi'].shift(1)
        #     ts.df['lo_1'] = ts.df['lo'].shift(1)
        #     ts.df['rg'] = ts.df['hi'] - ts.df['lo']
        #     ts.df['act'] = ts.df['volume'] * ts.df['c']
        #     ts.df['avg_v'] = ts.df['volume'].rolling(50).mean()
        #     ts.df['avg_c'] = ts.df['c'].rolling(50).mean()
        #     ts.df['avg_rg'] = ts.df['rg'].rolling(50).mean()
        #     ts.df['avg_act'] = ts.df['act'].rolling(50).mean()
        #     rec = ts.df.ix[-1]
        #     if rec.avg_v > 2000 and rec.avg_c > 40 and \
        #        rec.avg_act > 100000 and rec.avg_rg > 0.015 * rec.avg_c:
        #         res.append(stk)
        #         sc = StxCandles(stk)
        #         setup_ts = sc.calculate_setups(sd=start_date)
        #         setups = ['gap', 'marubozu', 'hammer', 'doji', 'engulfing',
        #                   'piercing', 'harami', 'star', 'engulfharami',
        #                   'three_m', 'three_in', 'three_out',
        #                   'up_gap_two_crows']
        #         with open('/home/cma/setups/{0:s}.csv'.format(stk), 'w') as f:
        #             for index, row in setup_ts.df.iterrows():
        #                 f.write('{0:s};'.format(str(index.date())))
        #                 for setup in setups:
        #                     if row[setup] != 0:
        #                         f.write('  {0:s}: {1:.0f} '.format(
        #                             setup.upper(), row[setup]))
        #                 f.write('\n')
        #     if ixx == len(stx) or ixx % 50 == 0:
        #         print('Processed {0:d} leaders'.format(ixx))
        # print('Found {0:d} super leaders'.format(len(res)))
        # return res

        
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
