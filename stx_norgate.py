import os
import numpy as np
import pandas as pd
import subprocess
import stxdb


class StxNorgate:
    root_dir = os.getenv('NORGATE_DIR')
    upload_dir = '/tmp'

    def __init__(self):
        '''
        - Create US exchange, if not already there
        - Define the directories from which we will be reading the data
        - Specify the atem commands used to get stock info and prices
        '''
        xchgs = stxdb.db_read_cmd("select * from exchanges where name='US'")
        if not xchgs:
            stxdb.db_write_cmd("insert into exchanges values('US')")
        self.input_dirs = ['AMEX', 'Indices', 'NASDAQ', 'NYSE', 'NYSE Arca']
        self.atem_prices_cmd = [
            'atem', '-o', '{0:s}/prices.txt'.format(self.upload_dir),
            '--format=symbol,date,open,high,low,close,volume,openint',
            '--float-openint']
        self.atem_info_cmd = [
            'atem', '-o', '{0:s}/names.txt'.format(self.upload_dir),
            '--format=symbol,long_name']

    def parse_all_data(self):
        for input_dir in self.input_dirs:
            in_dir = '{0:s}/{1:s}/'.format(self.root_dir, input_dir)
            sdirs = [x[0] for x in os.walk(in_dir) if x[0] != in_dir]
            sdirs.sort()
            for sdir in sdirs:
                p = subprocess.Popen(self.atem_info_cmd, cwd=sdir)
                p.wait()
                p = subprocess.Popen(self.atem_prices_cmd, cwd=sdir)
                p.wait()
                self.process_data(sdir)

    def process_data(self, in_dir):
        stx = self.upload_names(in_dir)
        self.upload_prices(in_dir, sorted(stx))

    def upload_names(self, in_dir):
        with open('{0:s}/names.txt'.format(self.upload_dir), 'r') as f:
            lines = f.readlines()
        dct = {}
        for l in lines[1:]:
            tokens = l.strip().split('\t')
            dct[tokens[0]] = tokens[1]
        for ticker, name in dct.items():
            stxdb.db_write_cmd(
                "INSERT INTO equities VALUES ('{0:s}', '{1:s}', 'US Stocks', "
                "'US') on conflict (stk) do nothing".format(ticker, name))
        return dct.keys()

    def upload_prices(self, in_dir, stx):
        df = pd.read_csv('{0:s}/prices.txt', sep='\t', header=0)
        for stk in stx:
            stk_df = df.query("symbol=='{0:s}'".format(stk))
            stk_df, splits = self.get_splits(stk_df)

    def get_splits(self, df):
        df['r'] = df['close'] / df['openint']
        df['r_1'] = df['r'].shift()
        df.r_1.fillna(df.r, inplace=True)
        df['rr'] = np.round(df['r'] / df['r_1'], 4)
        df['rr_1'] = df['rr'].shift(-1)
        df.rr_1.fillna(df.rr, inplace=True)
        splits = df.query('rr_1<0.999 | rr_1>1.001')
        split_dct = {}
        for ixx, row in splits.iterrows():
            ratio = 1 / row['rr_1']
            split_dct[row['date']] = ratio
            df.loc[0:ixx, 'open'] = np.round(df['open'] / ratio, 2)
            df.loc[0:ixx, 'high'] = np.round(df['high'] / ratio, 2)
            df.loc[0:ixx, 'low'] = np.round(df['low'] / ratio, 2)
            df.loc[0:ixx, 'close'] = np.round(df['close'] / ratio, 2)
            df.loc[0:ixx, 'volume'] = np.round(df['volume'] * ratio, 0)
        df.drop(['r', 'r_1', 'rr', 'rr_1'], axis=1, inplace=True)
        return df, split_dct


if __name__ == '__main__':
    pass
'''
from stx_norgate import StxNorgate
sn = StxNorgate()
import pandas as pd
df_g = pd.read_csv('/tmp/prices_g.txt', sep='\t', header=0)
df = df_g.query("symbol=='GGG'")
res, splits = sn.get_splits(df)
'''
