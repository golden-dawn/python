import schedule
import stxdb
import time

class Stx247:    
    '''1. In the constructor, create the database tables, if non-existent
    2. Schedule two runs, one at 15:30, the other at 20:00
    3. Email the analysis results:
       https://medium.freecodecamp.org/send-emails-using-code-4fcea9df63f
    4. If the current date is an option expiry, the 20:00 run should
       be generating new list of leaders.
    5. Have exclusion list, to remove stocks I don't need.
    6. 
    '''
    def __init__(self):
        self.tbl_name = 'cache'
        self.opt_tbl_name = 'opt_cache'
        self.ldr_tbl_name = 'leaders'
        self.setup_tbl_name = 'setups'
        self.sql_create_eod_tbl = "CREATE TABLE {0:s} ("\
                                  "stk varchar(8) NOT NULL,"\
                                  "dt date NOT NULL,"\
                                  "o numeric(10,2) DEFAULT NULL,"\
                                  "hi numeric(10,2) DEFAULT NULL,"\
                                  "lo numeric(10,2) DEFAULT NULL,"\
                                  "c numeric(10,2) DEFAULT NULL,"\
                                  "v integer DEFAULT NULL,"\
                                  "PRIMARY KEY (stk,dt)"\
                                  ")".format(self.tbl_name)
        self.sql_create_opt_tbl = "CREATE TABLE {0:s} ("\
                                  'expiry date NOT NULL,'\
                                  'und character varying(16) NOT NULL,'\
                                  'cp character varying(1) NOT NULL,'\
                                  'strike numeric(10,2) NOT NULL,'\
                                  'dt date NOT NULL,'\
                                  'bid numeric(10,2),'\
                                  'ask numeric(10,2),'\
                                  'volume integer,'\
                                  'PRIMARY KEY (expiry,und,cp,strike,dt)'\
                                  ')'.format(self.opt_tbl_name)
        self.sql_create_ldr_tbl = "CREATE TABLE {0:s} ("\
                                  "stk varchar(8) NOT NULL,"\
                                  "dt date NOT NULL,"\
                                  "exp date NOT NULL,"\
                                  "PRIMARY KEY (stk,dt)"\
                                  ")".format(self.ldr_tbl_name)
        self.sql_create_setup_tbl = "CREATE TABLE {0:s} ("\
                                    "stk varchar(8) NOT NULL,"\
                                    "dt date NOT NULL,"\
                                    "setup varchar(80) NOT NULL,"\
                                    "PRIMARY KEY (stk,dt)"\
                                  ")".format(self.setup_tbl_name)
        stxdb.db_create_missing_table(self.tbl_name, self.sql_create_eod_tbl)
        stxdb.db_create_missing_table(self.opt_tbl_name,
                                      self.sql_create_opt_tbl)
        stxdb.db_create_missing_table(self.ldr_tbl_name,
                                      self.sql_create_ldr_tbl)
        stxdb.db_create_missing_table(self.setup_tbl_name,
                                      self.sql_create_setup_tbl)

    def job(self):
        print('247 job')


if __name__ == '__main__':
    s247= Stx247()
    schedule.every(10).seconds.do(s247.job)

    while True:
        schedule.run_pending()
        time.sleep(1)
