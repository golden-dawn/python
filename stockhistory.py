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
                dt       = str(sc.prev_busday(dt).date())
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
        return stx

    def load_stk(self, stk) :
        stk_fname    = '{0:s}/{1:s}.csv'.format(self.d_dir, stk)
        stk_data = []
        with open(stk_fname, 'r') as ifile :
            frdr     = csv.reader(ifile)
            for row in frdr :
                stk_data.append(row)
        return stk_data
        
    def gen_split_db_upload(self, stx) :
        split_list       = []
        for stk, splits in stx.items() :
            split_dates  = list(splits.keys())
            split_dates.sort()
            six          = 0
            split_dt     = split_dates[six]
            stk_data     = self.load_stk(stk)
            sdate, edate = stk_data[0][1], stk_data[-1][1]
            while six < len(split_dates) and split_dt < sdate :
                print('{0:s}:{1:s}: split before start'.format(stk, split_dt))
                six      = six + 1
                if six < len(split_dates) :
                    split_dt = split_dates[six]
            if six >= len(split_dates) :
                continue
            for r1, r2 in zip(stk_data, stk_data[1:]) :
                if r1[1] <= split_dt and r2[1] > split_dt :
                    try :
                        r_1  = float(r1[5]) / float(r1[7])
                        r_2  = float(r2[5]) / float(r2[7])
                        rr   = round(r_2 / r_1, 4)
                        if round(rr, 2) == 1.0 :
                            print('{0:s}:{1:s}: no split'.format(stk, split_dt))
                        else :
                            ratio  = round(splits.get(split_dt), 4)
                            if round(rr, 2) != round(ratio, 2) :
                                print('{0:s}: {1:s}: diff split ratio: {2:f} ' \
                                      'instead of {3:f}'.\
                                      format(stk, split_dt, rr, ratio))
                                ratio = rr
                            split_list.append('{0:s}\t{1:s}\t{2:.4f}\n'.\
                                              format(stk, split_dt, ratio))
                    except ZeroDivisionError :
                        print('{0:s}:{1:s}:ZeroDivError'.format(stk, split_dt))
                    six           = six + 1
                    if six >= len(split_dates) :
                        break
                    else :
                        split_dt  = split_dates[six]
            while six < len(split_dates) :
                print('{0:s}:{1:s}: split after end'.format(stk, split_dt))
                six = six + 1
                if six < len(split_dates) :
                    split_dt  = split_dates[six]
        self.gen_upload_file(split_list)

    def gen_upload_file(self, split_list) :
        fname            = '{0:s}/stocksplits.txt'.format(self.db_dir)
        with open(fname, 'w') as ofile :
            for split in split_list :
                ofile.write(split)

if __name__ == '__main__' :
    sh = StockHistory()
    # sh.parse_eod_data()
    stx = sh.load_splits()
    sh.gen_split_db_upload(stx)
