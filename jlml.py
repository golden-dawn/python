import math
import stxcal
# import stxdb
from stxjl import StxJL
from stxts import StxTS


class JLML:
    def __init__(self, sd='1950-01-01', ed='2020-12-31'):
        self.sd = sd
        self.ed = ed
        self.factors = [0.6, 1.1, 1.6]
        self.time_adj = {0.6: 30.0, 1.1: 120.0, 1.6: 240.0}

    def gen_stk_data(self, stk):
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
            for jl in jl_lst:
                # jl = jl_lst[0]
                jl.nextjl()
                pivs = jl.get_num_pivots(4)
                print('{0:.1f} {1:s} {2:.2f} {3:.2f} {4:.2f} rg: {5:.2f} '
                      '{6:s}'.format(
                          jl.f, ts.current_date(), ts.current('c'),
                          (ts.current('c_3') - ts.current('c')) / jl.avg_rg,
                          (ts.current('c_5') - ts.current('c')) / jl.avg_rg,
                          jl.avg_rg, self.print_stats(jl, ts, pivs)))

    def print_stats(self, jl, ts, pivs):
        piv_data = []
        cc = ts.current('c')
        ixx = 1
        for piv in reversed(pivs):
            num_days = stxcal.num_busdays(piv.dt, ts.current_date())
            if num_days == 0:
                num_days = 0.5
            dist = (cc - piv.price) / (jl.avg_rg * math.sqrt(num_days))
            udv, udd = self.up_down_volume(ts, piv)
            piv_data.append('P{0:d}: {1:s} {2:.2f} {3:.2f} {4:.2f} {5:.2f}'.
                            format(ixx, piv.dt, num_days / self.time_adj[jl.f],
                                   dist, udv, udd))
            ixx += 1
        return '; '.join(piv_data)

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
    jlml = JLML('2002-05-20', '2005-09-30')
    jlml.gen_stk_data(stk)
