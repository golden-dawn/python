import datetime
import stxcal
from stxts import StxTS


class StxCandles:
    def __init__(self, stk_name, avg_volume_days=50, avg_range_days=10):
        self.stk_name = stk_name
        self.avg_volume_days = avg_volume_days
        self.avg_range_days = avg_range_days

    def calculate_setups(self):
        sd = '1985-01-01'
        ed = datetime.datetime.now().strftime('%Y-%m-%d')
        ts = StxTS(self.stk_name, sd, ed)
        # adjust the whole thing for splits, etc.
        ts.set_day(str(ts.df.index[-1].date()))
        ts.df['body'] = abs(ts.df['o'] - ts.df['c'])
        ts.df['avg_body'] = ts.df['body'].rolling(self.avg_range_days)
        ts.df['avg_v'] = ts.df['volume'].rolling(self.avg_volume_days)
