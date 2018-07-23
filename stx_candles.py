import datetime
# import stxcal
from stxts import StxTS


class StxCandles:
    def __init__(self, stk_name, avg_volume_days=50, avg_range_days=10):
        self.stk_name = stk_name
        self.avg_volume_days = avg_volume_days
        self.avg_range_days = avg_range_days
        self.marubozu_ratio = 0.8
        self.long_day_avg_ratio = 1.3
        self.short_day_avg_ratio = 0.5
        self.engulfing_ratio = 0.7
        self.harami_ratio = 0.3
        self.hammer_body_long_shadow_ratio = 0.5
        self.hammer_short_shadow_range_ratio = 0.15
        self.doji_body_range_ratio = 0.025
        self.equal_values_range_ratio = 0.025

    def calculate_setups(self, setup_list=None):
        sd = '1985-01-01'
        ed = datetime.datetime.now().strftime('%Y-%m-%d')
        ts = StxTS(self.stk_name, sd, ed)
        # adjust the whole thing for splits, etc.
        ts.set_day(str(ts.df.index[-1].date()))
        ts.df['body'] = abs(ts.df['o'] - ts.df['c'])
        for x in range(1, 5):
            ts.df['o_{0:d}'.format(x)] = ts.df['o'].shift(x)
            ts.df['hi_{0:d}'.format(x)] = ts.df['hi'].shift(x)
            ts.df['lo_{0:d}'.format(x)] = ts.df['lo'].shift(x)
            ts.df['c_{0:d}'.format(x)] = ts.df['c'].shift(x)
            ts.df['body_{0:d}'.format(x)] = ts.df['body'].shift(x)
        ts.df['avg_body'] = ts.df['body'].rolling(self.avg_range_days)
        ts.df['avg_v'] = ts.df['volume'].rolling(self.avg_volume_days)
        ts.df['upper_shadow'] = ts.df.apply(
            lambda r: r['hi'] - max(r['o'], r['c']), axis=1)
        ts.df['lower_shadow'] = ts.df.apply(
            lambda r: min(r['o'], r['c']) - r['lo'], axis=1)

        def gapfun(r):
            if r['o'] < r['lo_1']:
                return -2
            if r['o'] < r['c_1']:
                return -1
            if r['o'] > r['hi_1']:
                return 2
            if r['o'] > r['c_1']:
                return 1
            return 0
        ts.df['gap'] = ts.df.apply(gapfun, axis=1)
        for x in range(1, 5):
            ts.df['gap_{0:d}'.format(x)] = ts.df['gap'].shift(x)

        def marubozufun(r):
            if r['body'] < self.marubozu_ratio * (r['hi'] - r['lo']):
                return 0
            if r['c'] > r['o']:
                return 2 if(r['body'] >=
                            self.long_day_avg_ratio * float(r['avg_body'])) else 1
            return -2 if(r['body'] >=
                         self.long_day_avg_ratio * float(r['avg_body'])) else -1
        ts.df['marubozu'] = ts.df.apply(marubozufun, axis=1)
        marubozu_df = ts.df.query('abs(marubozu)==2')
        for x in range(1, 5):
            ts.df['marubozu_{0:d}'.format(x)] = ts.df['marubozu'].shift(x)
        for index, row in marubozu_df.iterrows():
            print('Marubozu', str(index.date()), row['marubozu'])

        def hammerfun(r):
            if ((r['body'] <= self.hammer_body_long_shadow_ratio *
                r['lower_shadow']) and
                (r['upper_shadow'] <= self.hammer_short_shadow_range_ratio *
                 (r['hi'] - r['lo']))):
                return 1
            if ((r['body'] <= self.hammer_body_long_shadow_ratio *
                r['upper_shadow']) and
                (r['lower_shadow'] <= self.hammer_short_shadow_range_ratio *
                 (r['hi'] - r['lo']))):
                return -1
            return 0
        ts.df['hammer'] = ts.df.apply(hammerfun, axis=1)
        hammer_df = ts.df.query('hammer!=0')
        for index, row in hammer_df.iterrows():
            print('Hammer', str(index.date()), row['hammer'])

        def dojifun(r):
            if r['body'] <= self.doji_body_range_ratio * (r['hi'] - r['lo']):
                return 1
            return 0
        ts.df['doji'] = ts.df.apply(dojifun, axis=1)
        doji_df = ts.df.query('doji!=0')
        for index, row in doji_df.iterrows():
            print('Doji', str(index.date()), row['doji'])

        def engulfingfun(r):
            if r['body'] < self.engulfing_ratio * r['body_1']:
                return 0
            if(r['o'] > r['c'] and r['o_1'] < r['c_1'] and
               r['o'] > r['c_1'] and r['c'] < r['o_1']):
                return -1
            if(r['o'] < r['c'] and r['o_1'] > r['c_1'] and
               r['o'] < r['c_1'] and r['c'] > r['o_1']):
                return 1
            return 0
        ts.df['engulfing'] = ts.df.apply(engulfingfun, axis=1)
        engulfing_df = ts.df.query('engulfing!=0')
        for index, row in engulfing_df.iterrows():
            print('Engulfing', str(index.date()), row['engulfing'])

        def piercingfun(r):
            if r['marubozu'] * r['marubozu_1'] != -1:
                return 0
            if(r['marubozu'] == 1 and r['o'] < r['c_1'] and
               2 * r['c'] > r['o_1'] + r['c_1']):
                return 1
            if(r['marubozu'] == -1 and r['o'] > r['c_1'] and
               2 * r['c'] < r['o_1'] + r['c_1']):
                return -1
            return 0
        ts.df['piercing'] = ts.df.apply(piercingfun, axis=1)
        piercing_df = ts.df.query('piercing!=0')
        for index, row in piercing_df.iterrows():
            print('Piercing', str(index.date()), row['piercing'])

        for index, row in marubozu_df.iterrows():
            print('Marubozu', str(index.date()), row['marubozu'])
        # def starfun(r):
        #     if r['marubozu_2'] == 0:
        #         return 0
        #     if(r['marubozu_2'] == -1 and r['gap_1'] < 0 and
        #        abs(r['o_1'] - r['c_1']) <
        #        self.harami_ratio * (r['hi_1'] - r['lo_1']) and
        #        2 * r['c'] > r['o_1'] + r['c_1']):
        #         return 1
        #     if(r['marubozu'] == -1 and r['o'] > r['c_1'] and
        #        2 * r['c'] < r['o_1'] + r['c_1']):
        #         return -1
        #     return 0
        # ts.df['piercing'] = ts.df.apply(piercingfun, axis=1)
        # piercing_df = ts.df.query('piercing!=0')
        # for index, row in piercing_df.iterrows():
        #     print('Piercing', str(index.date()), row['piercing'])
