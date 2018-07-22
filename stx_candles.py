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
        ts.df['avg_body'] = ts.df['body'].rolling(self.avg_range_days)
        ts.df['avg_v'] = ts.df['volume'].rolling(self.avg_volume_days)
        ts.df['upper_shadow'] = ts.df.apply(
            lambda r: r['hi'] - max(r['o'], r['c']), axis=1)
        ts.df['lower_shadow'] = ts.df.apply(
            lambda r: min(r['o'], r['c']) - r['lo'], axis=1)

        def marubozufun(r):
            if r['body'] < self.marubozu_ratio * (r['hi'] - r['lo']):
                return 0
            return 1 if r['c'] > r['o'] else -1
        ts.df['is_marubozu'] = ts.df.apply(marubozufun, axis=1)
        marubozu_df = ts.df.query('is_marubozu!=0')
        for index, row in marubozu_df.iterrows():
            print('Marubozu', str(index.date()), row['is_marubozu'])

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
        ts.df['is_hammer'] = ts.df.apply(hammerfun, axis=1)
        hammer_df = ts.df.query('is_hammer!=0')
        for index, row in hammer_df.iterrows():
            print('Hammer', str(index.date()), row['is_hammer'])

