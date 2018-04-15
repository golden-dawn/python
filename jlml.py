import math
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS


class JLML:
    def __init__(self):
        self.sd = '1950-01-01'
        self.ed = '2020-12-31'
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
                          self.print_stats(ts, pivs)))

    def print_stats(self, jl, ts, pivs):
        piv_data = []
        cc = ts.current('c')
        for piv in reversed(pivs):
            num_days = stxcal.num_busdays(piv.dt, ts.current_date())
            dist = (piv.price - cc) / (jl.avg_rg * math.sqrt(num_days))

    def up_down_volume(self, ts, piv):
        total_volume = 0
        signed_volume = 0
        all_days = 0
        signed_days = 0
        sotart_ix = ts.find(piv.dt)
        for ixx in range(start_ix, ts.pos + 1):
            total_volume += ts.ix(ixx).volume * math.abs(ts.ix(ixx).c -
                                                         ts.ix(ixx - 1).c)
            signed_volume += ts.ix(ixx).volume * (ts.ix(ixx).c -
                                                  ts.ix(ixx - 1).c)
            all_days += 1
            if ts.ix(ixx).c > ts.ix(ixx - 1).c:
                signed_days += 1
            elif ts.ix(ixx).c < ts.ix(ixx - 1).c:
                signed_days -= 1
        return signed_volume / total_volume, signed_days / all_days
