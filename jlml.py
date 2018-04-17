import math
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS


class JLML:
    def __init__(self, sd='1950-01-01', ed='2020-12-31'):
        self.sd = sd
        self.ed = ed
        self.factors = [0.6, 1.1, 1.6]

    def gen_stk_data(self, stk):
        ts = StxTS(stk, self.sd, self.ed)
        for factor in self.factors:
            jl = StxJL(ts, factor)
            jl_start = jl.w + ts.start
            ts.set_day(str(ts.ix(jl_start).name.date()))
            start_w = jl.initjl()
            for ixx in range(start_w, ts.end + 1):
                ts.next_day()
                jl.nextjl()
                pivs = jl.get_num_pivots(4)
                print('{0:s} {1:.2f} {2:.2f} {3:.2f} {4:.2f} {5:.0f} '
                      'rg: {6:.2f} {7:s}'.format(
                          ts.current_date(), ts.current('o'), ts.current('hi'),
                          ts.current('lo'), ts.current('c'),
                          ts.current('volume'), jl.avg_rg,
                          self.print_stats(jl, ts, pivs)))

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
            piv_data.append('P{0:d}: {1:s} {2:d} {3:.2f} {4:.2f} {5:.2f}'.
                            format(ixx, piv.dt, num_days, dist, udv, udd))
            ixx += 1
        return '; '.join(piv_data)

    def up_down_volume(self, ts, piv):
        total_volume = 0
        signed_volume = 0
        all_days = 0.0
        signed_days = 0.0
        start_ix = ts.find(piv.dt)
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
