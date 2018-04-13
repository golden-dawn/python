import stxcal
import sys
from stxts import StxTS
from stxjl import StxJL

stk = 'NFLX'
sd = '2002-02-01'
ed = '2018-03-09'
factor = 1.5
ts = StxTS(stk, sd, ed)
jl = StxJL(ts, factor)
jl_start = jl.w + ts.start
ts.set_day(str(ts.ix(jl_start).name.date()))
start_w = jl.initjl()
for ixx in range(start_w, ts.end + 1):
    ts.next_day()
    jl.nextjl()
    pivs = jl.get_num_pivots(4)
    print('{0:s} {1:.2f} {2:.2f} {3:.2f} {4:.2f} {5:.0f}, avg rg: {6:.2f}; '
          '4 pivs:'.format(ts.current_date(), ts.current('o'),
                           ts.current('hi'), ts.current('lo'),
                           ts.current('c'), ts.current('volume'), jl.avg_rg))
    jl.print_pivs(pivs)
