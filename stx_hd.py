import argparse
import os
import pandas as pd
import stxdb

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stock save historical data')
    parser.add_argument('-s', '--stockname',
                        help='name of the stock for which we upload data',
                        type=str,
                        required=True)
    parser.add_argument('-f', '--filepath',
                        help='full path to the stock data file',
                        type=str,
                        required=True)
    args = parser.parse_args()

    print('stx_hd: stock name = {0:s}, data file = {1:s}'.
          format(args.stockname, args.filepath))
    df = pd.read_csv(args.filepath, )
    df.drop(labels='Adj Close', axis=1, inplace=True)
    df.insert(loc=0, column='stk', value=args.stockname)
    df['stk'] = args.stockname
    df['oi'] = 0
    print(df.head())
    print("The dataframe has {0:d} rows".format(len(df)))
    print("Columns has {} elements: {}".format(len(df.columns), df.columns))
    df.columns = ['stk', 'dt', 'o', 'hi', 'lo', 'c', 'v', 'oi']
    df['o'] *= 100
    df['hi'] *= 100
    df['lo'] *= 100
    df['c'] *= 100
    df['v'] /= 1000
    df.o = df.o.round(0)
    df.hi = df.hi.round(0)
    df.lo = df.lo.round(0)
    df.c = df.c.round(0)
    df['o'] = df['o'].apply(int)
    df['hi'] = df['hi'].apply(int)
    df['lo'] = df['lo'].apply(int)
    df['c'] = df['c'].apply(int)
    df['v'] = df['v'].apply(int)
    print(df.head())
    df.to_csv('/tmp/a.csv', sep='\t', header=False, index=False)
    stxdb.db_upload_file('/tmp/a.csv', 'eods')