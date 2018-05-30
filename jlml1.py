import csv
import datetime
import math
import numpy as np
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS
import traceback


class JLML1:
    sql_create_tbl = 'CREATE TABLE {0:s} ('\
                     'stk varchar(16) NOT NULL,'\
                     'dt date NOT NULL,'\
                     'pl_3 decimal(2,1) DEFAULT NULL,'\
                     'pl_5 decimal(2,1) DEFAULT NULL,'\
                     'pl_exp decimal(2,1) DEFAULT NULL,'\
                     'jl11p1_time float DEFAULT NULL,'\
                     'jl11p1_dist float DEFAULT NULL,'\
                     'jl11p1_udv float DEFAULT NULL,'\
                     'jl11p1_udd float DEFAULT NULL,'\
                     'jl11p2_time float DEFAULT NULL,'\
                     'jl11p2_dist float DEFAULT NULL,'\
                     'jl11p2_udv float DEFAULT NULL,'\
                     'jl11p2_udd float DEFAULT NULL,'\
                     'jl11p3_time float DEFAULT NULL,'\
                     'jl11p3_dist float DEFAULT NULL,'\
                     'jl11p3_udv float DEFAULT NULL,'\
                     'jl11p3_udd float DEFAULT NULL,'\
                     'jl11p4_time float DEFAULT NULL,'\
                     'jl11p4_dist float DEFAULT NULL,'\
                     'jl11p4_udv float DEFAULT NULL,'\
                     'jl11p4_udd float DEFAULT NULL,'\
                     'jl16p1_time float DEFAULT NULL,'\
                     'jl16p1_dist float DEFAULT NULL,'\
                     'jl16p1_udv float DEFAULT NULL,'\
                     'jl16p1_udd float DEFAULT NULL,'\
                     'jl16p2_time float DEFAULT NULL,'\
                     'jl16p2_dist float DEFAULT NULL,'\
                     'jl16p2_udv float DEFAULT NULL,'\
                     'jl16p2_udd float DEFAULT NULL,'\
                     'jl16p3_time float DEFAULT NULL,'\
                     'jl16p3_dist float DEFAULT NULL,'\
                     'jl16p3_udv float DEFAULT NULL,'\
                     'jl16p3_udd float DEFAULT NULL,'\
                     'jl16p4_time float DEFAULT NULL,'\
                     'jl16p4_dist float DEFAULT NULL,'\
                     'jl16p4_udv float DEFAULT NULL,'\
                     'jl16p4_udd float DEFAULT NULL,'\
                     'PRIMARY KEY (stk,dt)'\
                     ')'

    def __init__(self, sd='1950-01-01', ed='2020-12-31'):
        self.sd = sd
        self.ed = ed
        self.factors = [0.6, 1.1, 1.6]
        self.time_adj = {0.6: 30.0, 1.1: 120.0, 1.6: 240.0}
        stxdb.db_create_missing_table('ml', self.sql_create_tbl)

    def gen_stk_data(self, stk):
        res = []
        ts = StxTS(stk, self.sd, self.ed)
        ts.df['c3'] = ts.df['c'].shift(-3)
        ts.df['c5'] = ts.df['c'].shift(-5)
        ts.df['c_1'] = ts.df['c'].shift(1)
        # print(ts.df.tail(10))
        ts.df['udv'] = (ts.df['c'] - ts.df['c_1']) * ts.df['volume']
        ts.df['tot_v'] = np.abs(ts.df['udv'])
        ts.df['sgn'] = np.sign(ts.df['udv'])
        for sdt in ts.splits:
            ratio = ts.splits[sdt][0]
            ixx = ts.find(str(sdt.date()))
            ts.df.loc[ixx-3:ixx, 'c3'] = ts.df['c3'] / ratio
            ts.df.loc[ixx-5:ixx, 'c5'] = ts.df['c5'] / ratio
        jl_lst = []
        for factor in self.factors:
            jl = StxJL(ts, factor)
            jl_start = jl.w + ts.start
            ts.set_day(str(ts.ix(jl_start).name.date()))
            start_w = jl.initjl()
            jl_lst.append(jl)
        for ixx in range(start_w, ts.end + 1):
            ts.next_day()
            lst = [stk, ts.current_date(),
                   self.f((ts.current('c3') - ts.current('c')) / jl.avg_rg),
                   self.f((ts.current('c5') - ts.current('c')) / jl.avg_rg)]
            for jl in jl_lst:
                # jl = jl_lst[0]
                jl.nextjl()
                pivs = jl.get_num_pivots(4)
                # pivs = []
                lst += self.pivot_stats(jl, ts, pivs)
                # print('{0:.1f} {1:s} {2:.2f} {3:.2f} {4:.2f} rg: {5:.2f} '
                #       'c3 = {6:.2f}, c5 = {7:.2f}'.format(
                #           jl.f, ts.current_date(), ts.current('c'),
                #           (ts.current('c3') - ts.current('c')) / jl.avg_rg,
                #           (ts.current('c5') - ts.current('c')) / jl.avg_rg,
                #           jl.avg_rg, ts.current('c3'), ts.current('c5')))
                # print('{0:.1f} {1:s} {2:.2f} {3:.2f} {4:.2f} rg: {5:.2f} '
                #       '{6:s}'.format(
                #           jl.f, ts.current_date(), ts.current('c'),
                #           (ts.current('c3') - ts.current('c')) / jl.avg_rg,
                #           (ts.current('c5') - ts.current('c')) / jl.avg_rg,
                #           jl.avg_rg, self.print_stats(jl, ts, pivs)))
            res.append(lst)
        fname = '/tmp/jlml.csv'
        with open(fname, 'w') as f:
            wrtr = csv.writer(f, delimiter='\t')
            for r in res[:-6]:
                wrtr.writerow(r)
                # f.write('{0:s}\n'.format('\t'.join([str(x) for x in r])))
        stxdb.db_upload_file(fname, 'ml')
        return len(res)

    def f(self, x, bound=3):
        if x <= -bound:
            return -bound
        elif x > -bound and x <= -0.5:
            return round((x + 0.25) * 2) / 2
        elif x > -0.5 and x < 0.5:
            return 0
        elif x >= 0.5 and x < bound:
            return round((x - 0.25) * 2) / 2
        else:  # x >= bound
            return bound

    def pivot_stats(self, jl, ts, pivs):
        piv_data = []
        cc = ts.current('c')
        # ixx = 1
        for piv in reversed(pivs):
            num_days = stxcal.num_busdays(piv.dt, ts.current_date())
            if num_days == 0:
                num_days = 0.5
            dist = (cc - piv.price) / (jl.avg_rg * math.sqrt(num_days))
            udv, udd = self.up_down_volume(ts, piv)
            piv_data += [round(num_days / self.time_adj[jl.f], 3),
                         round(dist, 3), round(udv, 3), round(udd, 3)]
            # piv_data += [num_days / self.time_adj[jl.f], dist, udv, udd]
            # piv_data.append('P{0:d}: {1:s} {2:.2f} {3:.2f} {4:.2f} {5:.2f}'.
            #             format(ixx, piv.dt, num_days / self.time_adj[jl.f],
            #                        dist, udv, udd))
            # ixx += 1
        return piv_data + [0.0] * (16 - len(piv_data))
        # return '; '.join(piv_data)

    def up_down_volume(self, ts, piv):
        start_ix = ts.find(piv.dt) + 1
        end_ix = ts.pos + 1
        if start_ix > ts.pos:
            start_ix = ts.pos
        total_volume = ts.df['tot_v'][start_ix: end_ix].sum()
        signed_volume = ts.df['udv'][start_ix: end_ix].sum()
        signed_days = ts.df['sgn'][start_ix: end_ix].sum()
        return signed_volume / total_volume, signed_days / (end_ix - start_ix)


if __name__ == '__main__':
    stk = 'NFLX'
    jlml = JLML()
    # jlml = JLML('2002-05-20', '2018-08-30')
    res = stxdb.db_read_cmd("select count(*) from ml where stk='{0:s}'".
                            format(stk))
    if res[0][0] == 0:
        t1 = datetime.datetime.now()
        try:
            num = jlml.gen_stk_data(stk)
        except Exception as ex:
            traceback.print_exc()
            num = 0
        t2 = datetime.datetime.now()
        delta = t2 - t1
        print('{0:s}: {1:d} records. Execution time: {2:f} seconds'.format(
            stk, num, (t2 - t1).seconds + (t2 - t1).microseconds / 1000000.0))
    else:
        print('Already done')
    r_stx = stxdb.db_read_cmd(
        "select ticker from equities where ticker like 'R%'")
    ixx = 0
    for r_stk in r_stx:
        stk = r_stk[0]
        print(stk)
        res = stxdb.db_read_cmd("select count(*) from ml where stk='{0:s}'".
                                format(stk))
        if res[0][0] == 0:
            t1 = datetime.datetime.now()
            try:
                num = jlml.gen_stk_data(stk)
            except Exception as ex:
                traceback.print_exc()
                num = 0
            t2 = datetime.datetime.now()
            delta = t2 - t1
            print('{0:s}: {1:d} records. Execution time: {2:f} seconds'.format(
                stk, num,
                (t2 - t1).seconds + (t2 - t1).microseconds / 1000000.0))
        else:
            print('Already done')
        ixx += 1
        if ixx % 10 == 0:
            print('{0:4d} / {1:4d}'.format(ixx, len(r_stx)))
