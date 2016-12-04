import csv
from datetime import datetime

for yr in range(2001, 2017) :
    fname = 'c:/goldendawn/stockhistory/stockhistory_{0:d}.csv'.format(yr)
    stx          = {}
    with open(fname) as csvfile :
        frdr     = csv.reader(csvfile)
        for row in frdr :
            stk  = row[0].strip()
            data = stx.get(stk, [])
            data.append('{0:s}\t{1:s}\t{2:.2f}\t{3:.2f}\t{4:.2f}\t{5:.2f}\t' \
                        '{6:d}\n'.\
                        format(stk,
                               str(datetime.strptime(row[1],'%m/%d/%Y').date()),
                               float(row[2]), float(row[3]), float(row[4]),
                               float(row[5]), int(row[7])))
            stx[stk] = data
        print('{0:s}: got data for {1:d} stocks'.format(fname, len(stx)))
    for stk, recs in stx.items() :
        if stk in ['AUX', 'PRN'] or stk.find('/') != -1 or stk.find('*') != -1 :
            continue
        with open('c:/goldendawn/data/{0:s}.txt'.format(stk), 'a') as ofile :
            for rec in recs :
                ofile.write(rec)
