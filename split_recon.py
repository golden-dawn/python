import pandas as pd
import stxdb
from stxts import StxTS

def split_analysis(cnx, sc, stk, sd, ed, eod_tbl) :
    q          = "select dt, spot from opt_spots where stk='{0:s}'".format(stk)
    spot_df    = pd.read_sql(q, cnx)
    spot_df.set_index('dt', inplace=True)
    ts         = StxTS(stk, sd, ed, eod_tbl)
    df         = ts.df.join(spot_df)
    df['r']    = df['spot'] / df['c']
    df['r1']   = df['r'].shift(-1)
    df['r2']   = df['r'].shift(-2)
    df['r3']   = df['r'].shift(-3)
    df['r_1']  = df['r'].shift()
    df['r_2']  = df['r'].shift(2)
    df['r_3']  = df['r'].shift(3)
    df['r'].fillna(method='bfill', inplace=True)
    df['r1'].fillna(method='bfill', inplace=True)
    df['r2'].fillna(method='bfill', inplace=True)
    df['r3'].fillna(method='bfill', inplace=True)
    df['r_1'].fillna(method='bfill', inplace=True)
    df['r_2'].fillna(method='bfill', inplace=True)
    df['r_3'].fillna(method='bfill', inplace=True)
    df['rr']   = df['r1']/df['r']
    df_f1      = df[(abs(df['rr'] - 1) > 0.05) & \
                    (round(df['r_1'] - df['r'], 2) == 0) & \
                    (round(df['r_2'] - df['r'], 2) == 0) & \
                    (round(df['r_3'] - df['r'], 2) == 0) & \
                    (round(df['r2']  - df['r1'], 2) == 0) & \
                    (round(df['r3']  - df['r1'], 2) == 0) & \
                    (df['c'] > 1.0)]
    df, cov, acc = quality(sc, df, df_f1, ts, spot_df)
    print('Coverage: {0:.2f}, accuracy: {1:.4f}'.format(cov, acc))
    return df, df_f1

def quality(sc, df, df_f1, ts, spot_df) :
    s_spot     = str(spot_df.index[0])
    e_spot     = str(spot_df.index[-1])
    spot_days  = sc.num_busdays(s_spot, e_spot)
    s_ts       = str(ts.df.index[0].date())
    e_ts       = str(ts.df.index[-1].date())
    if s_ts < s_spot :
        s_ts   = s_spot
    if e_ts > e_spot :
        e_ts   = e_spot
    ts_days    = sc.num_busdays(s_ts, e_ts)
    coverage   = round(100.0 * ts_days / spot_days, 2)
    # apply the split adjustments
    ts.splits.clear()
    for r in df_f1.iterrows():
        ts.splits[r[0]] = r[1]['rr']
    ts.adjust_splits_date_range(0, len(ts.df) - 1, inv = 1)
    df.drop(['c'], inplace = True, axis = 1)
    df         = df.join(ts.df[['c']])
    # calculate statistics: coverage and mean square error
    df['sqrt'] = pow(1 - df['spot']/df['c'], 2)
    accuracy = pow(df['sqrt'].sum() / len(df['sqrt']), 0.5)
    return df, coverage, accuracy
    
