import os
import pandas as pd
import stxdb
import zipfile


class OptEOD:
    upload_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads'
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April',
                   5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September',
                   10: 'October', 11: 'November', 12: 'December'}
    sql_create_opts = 'CREATE TABLE `{0:s}` ('\
                      '`exp` date NOT NULL,'\
                      '`und` char(6) NOT NULL,'\
                      '`cp` char(1) NOT NULL,'\
                      '`strike` decimal(9, 2) NOT NULL,'\
                      '`dt` date NOT NULL,'\
                      '`bid` decimal(9, 2) NOT NULL,'\
                      '`ask` decimal(9, 2) NOT NULL,'\
                      'PRIMARY KEY (`exp`,`und`,`cp`,`strike`,`dt`)'\
                      ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
    sql_create_spots = 'CREATE TABLE `{0:s}` ('\
                       '`stk` char(6) NOT NULL,'\
                       '`dt` date NOT NULL,'\
                       '`spot` decimal(9, 2) NOT NULL,'\
                       'PRIMARY KEY (`stk`,`dt`)'\
                       ') ENGINE=MyISAM DEFAULT CHARSET=utf8'

    def __init__(self, in_dir='c:/goldendawn/options', opt_tbl='options',
                 spot_tbl='opt_spots'):
        self.in_dir = in_dir
        self.opt_tbl = opt_tbl
        self.spot_tbl = spot_tbl
        stxdb.db_create_missing_table(opt_tbl, self.sql_create_opts)
        stxdb.db_create_missing_table(spot_tbl, self.sql_create_spots)

    def load_opts(self, start_year, end_year):
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                zip_fname = '{0:s}/bb_{1:d}_{2:s}.zip'.\
                        format(self.in_dir, year, self.month_names[month])
                if os.path.exists(zip_fname):
                    self.load_opts_archive(zip_fname)

    def load_opts_archive(self, zip_fname):
        tmp_dir = '{0:s}/tmp'.format(self.in_dir)
        # TODO: remove the tmp directory (if it already exists, and recreate it
        print('Unzipping {0:s} into {1:s}'.format(zip_fname, tmp_dir))
        with zipfile.ZipFile(zip_fname, 'r') as zip_file:
            zip_file.extractall(tmp_dir)
        daily_opt_files = os.listdir(tmp_dir)
        for opt_file in daily_opt_files:
            print('  Processing {0:s}'.format(opt_file))
            opt_df = pd.read_csv(os.path.join(tmp_dir, opt_file),
                                 parse_dates=['Expiration', 'DataDate'],
                                 infer_datetime_format='%m/%d/%Y')
            # TODO: round the strike and the spot to 2 significant digits

            # replace 'call' and 'put' with 'c' and 'p'
            # TODO: apply the function to the cp column (not Type),
            # after renaming the column
            def cpfun(x):
                return x['Type'][:1]
            opt_df['cp'] = opt_df.apply(cpfun, axis=1)
            opt_df.drop(['Exchange', 'OptionRoot', 'OptionExt', 'Type', 'Last',
                         'OpenInterest', 'T1OpenInterest', 'Volume'], axis=1,
                        inplace=True)
            opt_df.columns = ['und', 'spot', 'exp', 'dt', 'strike',
                              'bid', 'ask', 'cp']
            # TODO: calculate the next 6 monthly expiries, and filter
            # the dataframe to only include those expiries
            # TODO: round the strikes and spots, as well as change
            # call and put after filtering
            spot_df = opt_df[['und', 'spot', 'dt']].drop_duplicates()
            spot_df.columns = ['stk', 'spot', 'dt']
            spot_df.set_index(['stk', 'dt'], inplace=True)
            spot_df.to_sql(self.spot_tbl, stxdb.db_get_cnx(), flavor='mysql',
                           if_exists='append')
            opt_df.drop(['spot'], axis=1, inplace=True)
            opt_df.set_index(['exp', 'und', 'cp', 'strike', 'dt'],
                             inplace=True)
            opt_df.to_sql(self.opt_tbl, stxdb.db_get_cnx(), flavor='mysql',
                          if_exists='append')
        # TODO: remove the tmp directory after we are done with the
        # current archive
