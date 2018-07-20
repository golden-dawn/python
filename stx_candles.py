import datetime
import stxcal
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
        self.umbrella_body_lower_shadow_ratio = 0.33
        self.umbrella_upper_shadow_range_ratio = 0.1
        self.doji_body_range_ratio = 0.025
        self.equal_values_range_ratio = 0.025

    def calculate_setups(self):
        sd = '1985-01-01'
        ed = datetime.datetime.now().strftime('%Y-%m-%d')
        ts = StxTS(self.stk_name, sd, ed)
        # adjust the whole thing for splits, etc.
        ts.set_day(str(ts.df.index[-1].date()))
        ts.df['body'] = abs(ts.df['o'] - ts.df['c'])
        ts.df['avg_body'] = ts.df['body'].rolling(self.avg_range_days)
        ts.df['avg_v'] = ts.df['volume'].rolling(self.avg_volume_days)
        
