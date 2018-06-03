import numpy as np
import stxdb
from stxts import StxTS


class StxLiquidity:
    def __init__(self, activity_threshold=250):
        self.activity_threshold = activity_threshold
        self.all_stx = stxdb.db_read_cmd('select ticker from equities')

    def pick_liquid_stock(self):
        nums = np.random.randint(len(self.all_stx), size=100)
        for num in nums:
            stk = self.all_stx[num][0]
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
        df_liq = ts.df.query('act_20>250')
        ts.df.act_20.fillna(ts.df.activity, inplace=True)
        if (len(df_liq) > 750 and len(df_liq) > 0.75 * len(ts.df)):
            return ts
        return None
