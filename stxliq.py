import numpy as np
import os
import pandas as pd
import stxcal
import stxdb
from stxts import StxTS
from stx_candles import StxCandles
import sys


class StxLiquidity:
    def __init__(self, activity_threshold=250):
        self.activity_threshold = activity_threshold
        db_stx = stxdb.db_read_cmd(
            'select ticker from equities order by ticker')
        self.all_stx = [x[0] for x in db_stx]

    def find_all_liquid_stocks(self):
        stx_ix = 0
        stx_len = len(self.all_stx)
        for stk in self.all_stx:
            stx_ix += 1
            if self.liquid_stk(stk) is not None:
                with open('super_liquid_stx.txt', 'a') as f:
                    f.write('{0:s}\n'.format(stk))
            if stx_ix % 1000 == 0 or stx_ix == stx_len:
                print('Processed {0:5d} / {1:5d} stocks'.format(
                    stx_ix, stx_len))

    def pick_random_liquid_stock(self):
        nums = np.random.randint(len(self.all_stx), size=100)
        for num in nums:
            stk = self.all_stx[num]
            print('Trying stock {0:s} ...'.format(stk))
            if self.liquid_stk(stk) is not None:
                print('Found liquid stock {0:s}'.format(stk))
                return stk
            print('Not good')
        return None

    def liquid_stk(self, stk, s_date='1985-01-01', e_date='2020-12-31'):
        ts = StxTS(stk, s_date, e_date)
        ts.df['activity'] = ts.df['c'] * ts.df['volume']
        ts.df['act_20'] = ts.df['activity'].shift().rolling(20).mean()
        df_liq = ts.df.query('act_20>10000')
        ts.df.act_20.fillna(ts.df.activity, inplace=True)
        if (len(df_liq) > 750 and len(df_liq) > 0.75 * len(ts.df)):
            return ts
        return None

    def find_all_liquid_stocks_as_of(self, selected_date):
        res = []
        q = "select * from eods where date = '{0:s}'".format(selected_date)
        df = pd.read_sql(q, stxdb.db_get_cnx())
        print('Found {0:d} stocks'.format(len(df)))
        df['rg'] = df['hi'] - df['lo']
        df_1 = df.query('volume>1000 & c>30 & rg>0.015*c')
        stx = df_1['stk'].tolist()
        print('Found {0:d} leaders'.format(len(stx)))
        start_date = stxcal.move_busdays(selected_date, -60)
        print('start_date is: {0:s}'.format(str(start_date)))
        ixx = 0
        for stk in stx:
            ixx += 1
            ts = StxTS(stk, start_date, selected_date)
            # adjust the whole thing for splits, etc.
            ts.set_day(str(ts.df.index[-1].date()))
            ts.df['hi_1'] = ts.df['hi'].shift(1)
            ts.df['lo_1'] = ts.df['lo'].shift(1)
            ts.df['rg'] = ts.df['hi'] - ts.df['lo']
            ts.df['act'] = ts.df['volume'] * ts.df['c']
            ts.df['avg_v'] = ts.df['volume'].rolling(50).mean()
            ts.df['avg_c'] = ts.df['c'].rolling(50).mean()
            ts.df['avg_rg'] = ts.df['rg'].rolling(50).mean()
            ts.df['avg_act'] = ts.df['act'].rolling(50).mean()
            rec = ts.df.ix[-1]
            if rec.avg_v > 2000 and rec.avg_c > 40 and \
               rec.avg_act > 100000 and rec.avg_rg > 0.015 * rec.avg_c:
                res.append(stk)
                sc = StxCandles(stk)
                setup_ts = sc.calculate_setups(sd=start_date)
                setups = ['gap', 'marubozu', 'hammer', 'doji', 'engulfing',
                          'piercing', 'harami', 'star', 'engulfharami',
                          'three_m', 'three_in', 'three_out',
                          'up_gap_two_crows']
                with open('/home/cma/setups/{0:s}.csv'.format(stk), 'w') as f:
                    for index, row in setup_ts.df.iterrows():
                        f.write('{0:s};'.format(str(index.date())))
                        for setup in setups:
                            if row[setup] != 0:
                                f.write('  {0:s}: {1:.0f} '.format(
                                    setup.upper(), row[setup]))
                        f.write('\n')
            if ixx == len(stx) or ixx % 50 == 0:
                print('Processed {0:d} leaders'.format(ixx))
        print('Found {0:d} super leaders'.format(len(res)))
        return res


if __name__ == '__main__':
    liq = StxLiquidity()
    selected_date = sys.argv[1]
    stx = liq.find_all_liquid_stocks_as_of(selected_date)
    print(stx)
    with open('{0:s}/leaders/{1:s}.csv'.format(os.getenv('HOME'),
                                               selected_date), 'w') as f:
        f.write(', '.join(stx))
    # liq.find_all_liquid_stocks()
