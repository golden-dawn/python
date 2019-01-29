require 'schedule'

class StxTFS:
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
                              ')'.format(self.tbl_name)
    
    '''
1. In the constructor, create the database tables, if non-existent
2. Schedule two runs, one at 15:30, the other at 20:00
3. Email the analysis results: https://medium.freecodecamp.org/send-emails-using-code-4fcea9df63f
   
    '''
