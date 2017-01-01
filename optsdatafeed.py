from io import BytesIO
import json
import pycurl
from stxcal import *
from stxdb import *

sql_create_options = 'CREATE TABLE `options` (' \
                     '`exp` char(10) NOT NULL,' \
                     '`und` varchar(6) NOT NULL,' \
                     '`cp` char(1) NOT NULL,' \
                     '`strike` decimal(9, 2) NOT NULL,' \
                     '`dt` char(10) NOT NULL,' \
                     '`bid` decimal(7, 2) NOT NULL,' \
                     '`ask` decimal(7, 2) NOT NULL,' \
                     'PRIMARY KEY (`exp`,`und`,`cp`,`strike`,`dt`)' \
                     ') ENGINE=MyISAM DEFAULT CHARSET=utf8'
yhoo_url           = 'https://query1.finance.yahoo.com/v7/finance/options/'\
                     '{0:s}?date={1:d}'
file_name          = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/options.txt'

def get_opts(stx = None) :
    db_create_missing_table('options', sql_create_options)
    stx_list   = [] if stx is None else stx.split(',')
    dt         = move_busdays(datetime.strftime(datetime.now(), '%Y-%m-%d'), 0)
    l_exps     = long_expiries()
    c          = pycurl.Curl()
    c.setopt(pycurl.SSL_VERIFYPEER, 0)   
    c.setopt(pycurl.SSL_VERIFYHOST, 0)
    first      = True
    for stk in stx_list :
        y_exps = get_chain(c, stk, l_exps[0], dt, 'w' if first else 'a')
        first  = False
        exps   = [val for val in l_exps if val in y_exps]
        for exp in exps[1:] :
            get_chain(c, stk, exp, dt, 'a')
    db_upload_file(file_name, 'options', 5)

def get_chain(c, stk, exp, dt, mode) :
    expiry     = datetime.strftime(datetime.utcfromtimestamp(exp), '%Y-%m-%d') 
    res_buffer = BytesIO()
    c.setopt(c.URL, yhoo_url.format(stk, exp))
    c.setopt(c.WRITEDATA, res_buffer)
    c.perform()
    res        = json.loads(res_buffer.getvalue().decode('iso-8859-1'))
    y_exps     = res['optionChain']['result'][0]['expirationDates']
    y_spot     = res['optionChain']['result'][0]['quote']['regularMarketPrice']
    calls      = res['optionChain']['result'][0]['options'][0]['calls']
    puts       = res['optionChain']['result'][0]['options'][0]['puts']
    with open(file_name, mode) as ofile :
        for call in calls :
            ofile.write('{0:s}\t{1:s}\tC\t{2:.2f}\t{3:s}\t{4:.2f}\t{5:.2f}\n'.\
                        format(expiry, stk, call['strike'], dt, call['bid'],
                               call['ask']))
        for put in puts :
            ofile.write('{0:s}\t{1:s}\tP\t{2:.2f}\t{3:s}\t{4:.2f}\t{5:.2f}\n'.\
                        format(expiry, stk, put['strike'], dt, put['bid'],
                               put['ask']))
    return y_exps
