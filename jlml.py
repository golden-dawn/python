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
start_w = jl.initjl()
for ixx in range(start_w, ts.end + 1):
    ts.next_day()
    jl.nextjl()
    pivs = jl.get_num_pivots(4)
    print("{0:s}: avg range: {1:.2f}; 4 pivs:".format(ts.current_date(),
                                                      jl.avg_rg))
    jl.print_pivs(pivs)
