import datetime
# import stxcal
from stxts import StxTS
import sys


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
        ts.df['avg_body'] = ts.df['body'].rolling(self.avg_range_days).mean()
        ts.df['avg_v'] = ts.df['volume'].rolling(self.avg_volume_days).mean()
        ts.df['upper_shadow'] = ts.df.apply(
            lambda r: r['hi'] - max(r['o'], r['c']), axis=1)
        ts.df['lower_shadow'] = ts.df.apply(
            lambda r: min(r['o'], r['c']) - r['lo'], axis=1)

        def gapfun(r):
            if r['hi'] < r['lo_1']:
                return -8 if r['c'] < r['o'] else -7
            if r['o'] < r['lo_1']:
                if r['c'] < r['lo_1']:
                    return -6 if r['c'] < r['o'] else -5
                else:
                    return -4
            if r['o'] < r['c_1']:
                if r['c'] < r['c_1']:
                    return -3 if r['c'] < r['o'] else -2
                else:
                    return -1
            if r['lo'] > r['hi_1']:
                return 8 if r['c'] > r['o'] else 7
            if r['o'] > r['hi_1']:
                if r['c'] > r['hi_1']:
                    return 6 if r['c'] > r['o'] else 5
                else:
                    return 4
            if r['o'] > r['c_1']:
                if r['c'] > r['c_1']:
                    return 3 if r['c'] > r['o'] else 2
                else:
                    return 1
            return 0

        ts.df['gap'] = ts.df.apply(gapfun, axis=1)
        for x in range(1, 5):
            ts.df['gap_{0:d}'.format(x)] = ts.df['gap'].shift(x)

        def marubozufun(r):
            if r['body'] < self.marubozu_ratio * (r['hi'] - r['lo']):
                return 0
            if r['c'] > r['o']:
                if r['body'] >= self.long_day_avg_ratio * r['avg_body']:
                    return 2
                return 1
            if r['body'] >= self.long_day_avg_ratio * r['avg_body']:
                return -2
            return -1
        ts.df['marubozu'] = ts.df.apply(marubozufun, axis=1)
        # marubozu_df = ts.df.query('abs(marubozu)!=0')
        # for index, row in marubozu_df.iterrows():
        #     print('Marubozu', str(index.date()), row['marubozu'])
        for x in range(1, 5):
            ts.df['marubozu_{0:d}'.format(x)] = ts.df['marubozu'].shift(x)

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
        # hammer_df = ts.df.query('hammer!=0')
        # for index, row in hammer_df.iterrows():
        #     print('Hammer', str(index.date()), row['hammer'])

        def dojifun(r):
            if r['body'] <= self.doji_body_range_ratio * (r['hi'] - r['lo']):
                return 1
            return 0
        ts.df['doji'] = ts.df.apply(dojifun, axis=1)
        # doji_df = ts.df.query('doji!=0')
        # for index, row in doji_df.iterrows():
        #     print('Doji', str(index.date()), row['doji'])

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
        # engulfing_df = ts.df.query('engulfing!=0')
        # for index, row in engulfing_df.iterrows():
        #     print('Engulfing', str(index.date()), row['engulfing'])
        for x in range(1, 5):
            ts.df['engulfing_{0:d}'.format(x)] = ts.df['engulfing'].shift(x)

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
        # piercing_df = ts.df.query('piercing!=0')
        # for index, row in piercing_df.iterrows():
        #     print('Piercing', str(index.date()), row['piercing'])

        def haramifun(r):
            if(r['body_1'] >= r['avg_body'] * self.long_day_avg_ratio and
               r['body'] <= r['body_1'] * self.harami_ratio and
               max(r['o'], r['c']) < max(r['o_1'], r['c_1']) and
               min(r['o'], r['c']) > min(r['o_1'], r['c_1'])):
                if r['o_1'] > r['c_1']:
                    return 2 if r['doji'] == 1 else 1
                else:
                    return -2 if r['doji'] == 1 else -1
            return 0
        ts.df['harami'] = ts.df.apply(haramifun, axis=1)
        for x in range(1, 5):
            ts.df['harami_{0:d}'.format(x)] = ts.df['harami'].shift(x)

        def starfun(r):
            if(r['body_2'] < r['avg_body'] * self.long_day_avg_ratio or
               r['body'] < r['avg_body'] or
               r['body_1'] > r['body'] * self.harami_ratio or
               (r['o_2'] - r['c_2']) * (r['o'] - r['c']) >= 0):
                return 0
            if r['o_2'] > r['c_2']:
                if r['hi_1'] < min(r['lo_2'], r['lo']):
                    return 2
                if(max(r['o_1'], r['c_1']) < min(r['c_2'], r['o']) and
                   r['hi_1'] - r['lo_1'] <= r['avg_body']):
                    return 1
            else:
                if r['lo_1'] > max(r['hi_2'], r['hi']):
                    return -2
                if min(r['o_1'], r['c_1']) > max(r['c_2'], r['o']):
                    return -1
            return 0
        ts.df['star'] = ts.df.apply(starfun, axis=1)
        # star_df = ts.df.query('star!=0')
        # for index, row in star_df.iterrows():
        #     print('Star', str(index.date()), row['star'])

        def engulfingharamifun(r):
            if(r['marubozu'] == 0 or
               (r['marubozu_3'] == 0 and r['marubozu_4'] == 0)):
                return 0
            if r['marubozu_3'] != 0:
                if r['marubozu'] * r['marubozu_3'] < 0:
                    return 0
                if r['harami_2'] == 1:
                    return r['engulfing']
            else:
                if r['marubozu'] * r['marubozu_4'] < 0:
                    return 0
                if(r['harami_3'] == 1 and
                   r['hi_2'] < max(r['hi_1'], r['hi_3']) and
                   r['lo_2'] > min(r['lo_1'], r['lo_3'])):
                    return r['engulfing']
            return 0
        ts.df['engulfharami'] = ts.df.apply(engulfingharamifun, axis=1)
        # engulfharami_df = ts.df.query('engulfharami!=0')
        # for index, row in engulfharami_df.iterrows():
        #     print('Engulfharami', str(index.date()), row['engulfharami'])

        def threemfun(r):
            if(r['marubozu'] > 0 and r['marubozu_1'] > 0 and
               r['marubozu_2'] > 0):
                return 1
            if(r['marubozu'] < 0 and r['marubozu_1'] < 0 and
               r['marubozu_2'] < 0):
                return -1
            return 0
        ts.df['three_m'] = ts.df.apply(threemfun, axis=1)

        def threeinfun(r):
            if(r['harami_1'] > 0 and r['c'] > r['o'] and
               (r['body'] >= r['avg_body'] or
                r['c'] - r['c_1'] >= r['avg_body'])):
                return 1
            if(r['harami_1'] < 0 and r['c'] < r['o'] and
               (r['body'] >= r['avg_body'] or
                r['c_1'] - r['c'] >= r['avg_body'])):
                return -1
            return 0
        ts.df['three_in'] = ts.df.apply(threeinfun, axis=1)
        # threein_df = ts.df.query('three_in!=0')
        # for index, row in threein_df.iterrows():
        #     print('3IN', str(index.date()), row['three_in'])

        def threeoutfun(r):
            if(r['engulfing_1'] > 0 and r['c'] > r['o'] and
               (r['body'] >= r['avg_body'] or
                r['c'] - r['c_1'] >= r['avg_body'])):
                return 1
            if(r['engulfing_1'] < 0 and r['c'] < r['o'] and
               (r['body'] >= r['avg_body'] or
                r['c_1'] - r['c'] >= r['avg_body'])):
                return -1
            return 0
        ts.df['three_out'] = ts.df.apply(threeoutfun, axis=1)
        # threeout_df = ts.df.query('three_out!=0')
        # for index, row in threeout_df.iterrows():
        #     print('3OUT', str(index.date()), row['three_out'])
        return ts


if __name__ == '__main__':
    stk = sys.argv[1]
    sc = StxCandles(stk)
    ts = sc.calculate_setups()

    setups = ['gap', 'marubozu', 'hammer', 'doji', 'engulfing', 'piercing',
              'harami', 'star', 'engulfharami', 'three_m', 'three_in',
              'three_out']
    with open('/home/cma/setups/{0:s}.csv'.format(stk), 'w') as f:
        for index, row in ts.df.iterrows():
            f.write('{0:s};'.format(str(index.date())))
            for setup in setups:
                if row[setup] != 0:
                    f.write('  {0:s}: {1:.0f} '.format(setup.upper(),
                                                       row[setup]))
            f.write('\n')
