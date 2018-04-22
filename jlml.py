import math
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS


class JLML:
    sql_create_tbl = 'CREATE TABLE {0:s} ('\
                     'stk varchar(16) NOT NULL,'\
                     'dt date NOT NULL,'\
                     'pl_3 decimal(2,1) DEFAULT NULL,'\
                     'pl_5 decimal(2,1) DEFAULT NULL,'\
                     'jl06p1_time float DEFAULT NULL,'\
                     'jl06p1_dist float DEFAULT NULL,'\
                     'jl06p1_udv float DEFAULT NULL,'\
                     'jl06p1_udd float DEFAULT NULL,'\
                     'jl06p2_time float DEFAULT NULL,'\
                     'jl06p2_dist float DEFAULT NULL,'\
                     'jl06p2_udv float DEFAULT NULL,'\
                     'jl06p2_udd float DEFAULT NULL,'\
                     'jl06p3_time float DEFAULT NULL,'\
                     'jl06p3_dist float DEFAULT NULL,'\
                     'jl06p3_udv float DEFAULT NULL,'\
                     'jl06p3_udd float DEFAULT NULL,'\
                     'jl06p4_time float DEFAULT NULL,'\
                     'jl06p4_dist float DEFAULT NULL,'\
                     'jl06p4_udv float DEFAULT NULL,'\
                     'jl06p4_udd float DEFAULT NULL,'\
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
        ts.df['c_3'] = ts.df['c'].shift(-3)
        ts.df['c_5'] = ts.df['c'].shift(-5)
        for sdt in ts.splits:
            ratio = ts.splits[sdt][0]
            ixx = ts.find(str(sdt.date()))
            ts.df.loc[ixx-3:ixx, 'c_3'] = ts.df['c_3'] / ratio
            ts.df.loc[ixx-5:ixx, 'c_5'] = ts.df['c_5'] / ratio
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
                   self.f((ts.current('c_3') - ts.current('c')) / jl.avg_rg),
                   self.f((ts.current('c_5') - ts.current('c')) / jl.avg_rg)]
            for jl in jl_lst:
                # jl = jl_lst[0]
                jl.nextjl()
                pivs = jl.get_num_pivots(4)
                lst += self.pivot_stats(jl, ts, pivs)
                # print('{0:.1f} {1:s} {2:.2f} {3:.2f} {4:.2f} rg: {5:.2f} '
                #       '{6:s}'.format(
                #           jl.f, ts.current_date(), ts.current('c'),
                #           (ts.current('c_3') - ts.current('c')) / jl.avg_rg,
                #           (ts.current('c_5') - ts.current('c')) / jl.avg_rg,
                #           jl.avg_rg, self.print_stats(jl, ts, pivs)))
            res.append(lst)
        fname = '/tmp/jlml.txt'
        with open(fname, 'w') as f:
            for r in res:
                f.write('{0:s}\n'.format('\t'.join([str(x) for x in r])))
        stxdb.db_upload_file(fname, 'ml')

    def f(x, bound=3):
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
            piv_data += [num_days / self.time_adj[jl.f], dist, udv, udd]
            # piv_data.append('P{0:d}: {1:s} {2:.2f} {3:.2f} {4:.2f} {5:.2f}'.
            #             format(ixx, piv.dt, num_days / self.time_adj[jl.f],
            #                        dist, udv, udd))
            # ixx += 1
        return piv_data + [''] * (16 - len(piv_data))
        # return '; '.join(piv_data)

    def up_down_volume(self, ts, piv):
        total_volume = 0
        signed_volume = 0
        all_days = 0.0
        signed_days = 0.0
        start_ix = ts.find(piv.dt) + 1
        if start_ix > ts.pos:
            start_ix = ts.pos
        for ixx in range(start_ix, ts.pos + 1):
            total_volume += ts.ix(ixx).volume * math.fabs(ts.ix(ixx).c -
                                                          ts.ix(ixx - 1).c)
            signed_volume += ts.ix(ixx).volume * (ts.ix(ixx).c -
                                                  ts.ix(ixx - 1).c)
            all_days += 1
            if ts.ix(ixx).c > ts.ix(ixx - 1).c:
                signed_days += 1
            elif ts.ix(ixx).c < ts.ix(ixx - 1).c:
                signed_days -= 1
        return signed_volume / total_volume, signed_days / all_days


if __name__ == '__main__':
    stk = 'NFLX'
    jlml = JLML('2002-05-20', '2003-05-30')
    jlml.gen_stk_data(stk)
