import csv
from datetime import datetime
from stxcal import StxCal

class StockHistory :

    def __init__(self, sh_dir = 'c:/goldendawn/stockhistory',
                 d_dir = 'c:/goldendawn/data',
                 db_dir = 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads') :
        self.sh_dir = sh_dir
        self.d_dir  = d_dir
        self.db_dir = db_dir
        
    def parse_eod_data(self) :
        for yr in range(2001, 2017) :
            fname    = '{0:s}/stockhistory_{1:d}.csv'.format(self.sh_dir, yr)
            stx      = {}
            with open(fname) as csvfile :
                frdr = csv.reader(csvfile)
                for row in frdr :
                    stk  = row[0].strip()
                    data = stx.get(stk, [])
                    data.append('{0:s},{1:s},{2:.2f},{3:.2f},{4:.2f},{5:.2f},' \
                                '{6:d},{7:.2f}\n'.\
                                format(stk, str(datetime.strptime(row[1],
                                                                  '%m/%d/%Y').\
                                                date()),
                                       float(row[2]), float(row[3]),
                                       float(row[4]), float(row[5]),
                                       int(row[7]), float(row[6])))
                    stx[stk] = data
            print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
            for stk, recs in stx.items() :
                if stk in ['AUX', 'PRN'] or stk.find('/') != -1 or \
                   stk.find('*') != -1 :
                    continue
                with open('{0:s}/{1:s}.csv'.format(self.d_dir, stk), 'a') \
                     as ofile :
                    for rec in recs :
                        ofile.write(rec)

    def load_splits(self) :
        fname            = '{0:s}/stocksplits.csv'.format(self.sh_dir)
        sc               = StxCal()
        with open(fname) as csvfile :
            frdr         = csv.reader(csvfile)
            stx          = {}
            for row in frdr :
                stk      = row[0].strip()
                data     = stx.get(stk, {})
                dt       = str(datetime.strptime(row[1], '%m/%d/%Y').date())
                if dt in data :
                    print('Duplicate entry for {0:s} on {1:s}'.format(stk, dt))
                    continue
                data[dt] = 1 / float(row[2])
                stx[stk] = data
            print('Found splits for {0:d} stocks'.format(len(stx)))
        fname            = 'c:/goldendawn/stocksplits.csv'
        with open(fname, 'w') as ofile :
            for stk, splits in stx.items() :
                for dt, ratio in splits.items() :
                    ofile.write('{0:s},{1:s},{2:f}\n'.format(stk, dt, ratio))

if __name__ == '__main__' :
    sh = StockHistory()
    sh.parse_eod_data()
