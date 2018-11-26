from stx_jl import Stx_JL
from stxjl import StxJL
from stxts import StxTS
import time

sts = StxTS('NFLX', '2002-04-04', '2018-11-30')
s_jl = Stx_JL(sts, 1.0)
t1 = int(time.time() * 1000)
jlres = s_jl.jl('2018-11-23')
s_jl.jl_print()
t2 = int(time.time() * 1000)
print('Total time: {0:d}'.format(t2 - t1))

sjl = StxJL(sts, 1.0)
t1 = int(time.time() * 1000)
jlres = sjl.jl('2018-11-23')
sjl.jl_print()
t2 = int(time.time() * 1000)
print('Total time: {0:d}'.format(t2 - t1))
