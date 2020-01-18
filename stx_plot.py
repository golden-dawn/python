import mplfinance as mpf
from stxts import StxTS
import sys

class StxPlot:
    def __init__(self, stock, start_date, end_date):
        ts = StxTS(stock, start_date, end_date)
        ts.df.index.name='Date'
        ts.df.drop('oi', inplace=True, axis=1)
        ts.df['o'] /= 100
        ts.df['hi'] /= 100
        ts.df['lo'] /= 100
        ts.df['c'] /= 100
        ts.df.rename(columns={'o': 'Open', 
                              'hi': 'High', 
                              'lo': 'Low', 
                              'c': 'Close', 
                              'v': 'Volume'}, 
                     inplace=True)
        self.ts = ts

    def plot_to_file(self):
        mpf.plot(self.ts.df, type='candle', volume=True, no_xgaps=True, 
                 savefig='/tmp/{0:s}.png'.format(self.ts.stk))

    def plot(self):
        mpf.plot(self.ts.df, type='candle', volume=True, no_xgaps=True)

if __name__ == '__main__':
    # TODO: use argparser and all that stuff
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    sorp = sys.argv[4]
    sp = StxPlot(stk, sd, ed)
    if sorp.startswith('s'):
        sp.plot_to_file()
    elif sorp.startswith('p'):
        sp.plot()
    else:
        printf('Dont know what to do with {}'.format(sorp))
