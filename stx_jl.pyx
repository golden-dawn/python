import numpy as np
import pandas as pd
from recordclass import recordclass
from stxts import StxTS
import sys

JLPivot = recordclass('JLPivot', 'dt, state, price, rg')


class Stx_JL:
    # static variables
    Nil = -1
    SRa = 0
    NRa = 1
    UT = 2
    DT = 3
    NRe = 4
    SRe = 5
    m_NRa = 6
    m_NRe = 7
    UT_fmt = '\x1b[1;32;40m'
    DT_fmt = '\x1b[1;31;40m'
    UP_piv_fmt = '\x1b[4;32;40m'  # '4;30;42'
    DN_piv_fmt = '\x1b[4;31;40m'  # '4;37;41'
    # UP_piv_fmt = '\x1b[0;30;42m' # '4;30;42'
    # DN_piv_fmt = '\x1b[0;37;41m' # '4;37;41'

    def __init__(self, ts, f, w=20):
        self.ts = ts
        self.f = f
        self.w = w
        self.cols = ['dt', 'rg', 'state', 'price', 'pivot', 'state2',
                     'price2', 'pivot2', 'p1_dt', 'p1_px', 'p1_s',
                     'lns_dt', 'lns_px', 'lns_s', 'lns', 'ls_s', 'ls']
        self.col_ix = dict(zip(self.cols, range(0, len(self.cols))))
        self.jl_recs = []
        self.jl_recs.append(self.cols)
        self.jlix = {}  # map dates to indices in JL records list (jl_rec)
        self.jl_ix = 0  # current index in the jlix table
        self.last = {'prim_px': 0, 'prim_state': Stx_JL.Nil, 'px': 0,
                     'state': Stx_JL.Nil}
        self.ts.df['hb4l'] = self.ts.df.apply(lambda r: 1 if
                                              2*r['c'] < r['hi']+r['lo'] else 0,
                                              axis=1)

    def jl(self, dt):
        self.ts.set_day(dt, -1)
        cdef int end = self.ts.pos
        cdef int start_w = self.initjl()
        cdef int ixx
        print('jl(): start_w={0:d}'.format(start_w))
        for ixx in range(start_w, end + 1):
            self.ts.next_day()
            self.nextjl()
        return self.jl_recs

    def initjl(self):
        ss = self.ts.start
        w1 = self.ts.pos - ss + 1
        win = self.w if w1 >= self.w else w1
        self.ts.set_day(str(self.ts.df.index[ss+win-1].date()))
        # print('initjl: ss={0:d} w1={1:d} win={2:d} set_day:{3:s}'.
        #       format(ss, w1, win, str(self.ts.df.index[ss+win-1].date())))
        df_0 = self.ts.df[ss: ss + win]
        max_dt = df_0['hi'].idxmax()
        min_dt = df_0['lo'].idxmin()
        hi = df_0.loc[max_dt].hi
        lo = df_0.loc[min_dt].lo
        self.trs = []
        self.trs.append(df_0.ix[0].hi - df_0.ix[0].lo)
        for h, l, c_1 in zip(df_0['hi'].values[1:], df_0['lo'].values[1:],
                             df_0['c'].values[:-1]):
            self.trs.append(max(h, c_1) - min(l, c_1))
        self.avg_rg = np.mean(self.trs)
        # assign hi to SRa, NRa, UT, m_NRa, and lo to SRe, NRe, DT, m_NRe
        self.lp = [hi, hi, hi, lo, lo, lo, hi, lo]
        for ixx in range(0, len(df_0)):
            dtc = df_0.index[ixx]
            dtcs = str(dtc.date())
            self.jlix[dtcs] = ixx + 1
            if dtc == max_dt and dtc == min_dt:
                self.rec_day(Stx_JL.NRa, Stx_JL.NRe, self.ts.start + ixx)
            elif dtc == max_dt:
                self.rec_day(Stx_JL.NRa, Stx_JL.Nil, self.ts.start + ixx)
            elif dtc == min_dt:
                self.rec_day(Stx_JL.Nil, Stx_JL.NRe, self.ts.start + ixx)
            else:
                self.rec_day(Stx_JL.Nil, Stx_JL.Nil, self.ts.start + ixx)
        return self.ts.start + win

    def init_first_rec(self, dt):
        return {'dt': dt, 'rg': self.avg_rg, 'state': Stx_JL.Nil, 'price': 0,
                'pivot': 0, 'state2': Stx_JL.Nil, 'price2': 0, 'pivot2': 0,
                'p1_dt': '', 'p1_px': 0, 'p1_s': Stx_JL.Nil, 'lns_dt': '',
                'lns_px': 0, 'lns_s': Stx_JL.Nil, 'lns': Stx_JL.Nil,
                'ls_s': Stx_JL.Nil, 'ls': Stx_JL.Nil}

    def init_rec(self, dt, list_ix):
        # print("init_rec: list_ix = {0:d}, dt = {1:s}".format(list_ix, dt))
        prev = self.jl_recs[list_ix]
        return {'dt': dt, 'rg': self.avg_rg, 'state': Stx_JL.Nil, 'price': 0,
                'pivot': 0, 'state2': Stx_JL.Nil, 'price2': 0, 'pivot2': 0,
                'p1_dt': prev[self.col_ix['p1_dt']],
                'p1_px': prev[self.col_ix['p1_px']],
                'p1_s': prev[self.col_ix['p1_s']],
                'lns_dt': prev[self.col_ix['lns_dt']],
                'lns_px': prev[self.col_ix['lns_px']],
                'lns': prev[self.col_ix['lns']],
                'lns_s': prev[self.col_ix['lns_s']],
                'ls': prev[self.col_ix['ls']],
                'ls_s': prev[self.col_ix['ls_s']]}

    def rec_day(self, sh, sl, ixx=-1):
        if ixx == -1:
            ixx = self.ts.pos
        sr = self.ts.df.ix[ixx]
        dtc = str(self.ts.df.index[ixx].date())
        lix = ixx - self.ts.start
        # print("lix = %d" % lix)
        dd = self.init_first_rec(dtc) if lix == 0 else self.init_rec(dtc, lix)
        if sh != Stx_JL.Nil and sl != Stx_JL.Nil:
            if sr.hb4l == 1:
                dd.update({'state': sh, 'price': sr['hi'], 'state2': sl,
                           'price2': sr['lo']})
            else:
                dd.update({'state': sl, 'price': sr['lo'], 'state2': sh,
                           'price2': sr['hi']})
        elif sh != Stx_JL.Nil:
            dd.update({'state': sh, 'price': sr['hi']})
        elif sl != Stx_JL.Nil:
            dd.update({'state': sl, 'price': sr['lo']})
        else:
            pass  # nothing to do, the record is already initialized
        if dd['state'] != Stx_JL.Nil:
            self.update_last(dd)
            self.update_lns_pivots(dd, lix)
        lst = [dd[col] for col in self.cols]
        self.jl_recs.append(lst)
        self.jlix[dtc] = lix + 1

    def update_last(self, dd):
        if dd['state2'] == Stx_JL.Nil:
            if dd['state'] != Stx_JL.Nil:
                self.last['px'] = dd['price']
                self.last['state'] = dd['state']
                if self.primary(dd['state']):
                    self.last['prim_px'] = dd['price']
                    self.last['prim_state'] = dd['state']
                self.lp[dd['state']] = dd['price']
        else:
            self.last['px'] = dd['price2']
            self.last['state'] = dd['state2']
            self.lp[dd['state2']] = dd['price2']
            self.lp[dd['state']] = dd['price']
            if self.primary(dd['state2']):
                self.last['prim_px'] = dd['price2']
                self.last['prim_state'] = dd['state2']
            elif self.primary(dd['state']):
                self.last['prim_px'] = dd['price']
                self.last['prim_state'] = dd['state']

    def update_lns_pivots(self, dd, list_ix):
        if (self.up(dd['state']) and self.dn(dd['lns'])) or \
           (self.dn(dd['state']) and self.up(dd['lns'])):
            self.update_pivot_diff_day(dd)
        if dd['state'] != Stx_JL.Nil:
            dd['ls_s'] = dd['ls']
            dd['ls'] = dd['state']
        if self.primary(dd['state']):
            dd['lns_dt'] = dd['dt']
            dd['lns_px'] = dd['price']
            dd['lns_s'] = dd['lns']
            dd['lns'] = dd['state']
        if (self.up(dd['state2']) and self.dn(dd['lns'])) or \
           (self.dn(dd['state2']) and self.up(dd['lns'])):
            if dd['lns_dt'] == dd['dt']:
                dd['pivot'] = 1
                dd['p1_dt'] = dd['dt']
                dd['p1_px'] = dd['price']
                dd['p1_s'] = dd['state']
            else:
                self.update_pivot_diff_day(dd)
        if dd['state2'] != Stx_JL.Nil:
            dd['ls_s'] = dd['ls']
            dd['ls'] = dd['state2']
        if self.primary(dd['state2']):
            dd['lns_dt'] = dd['dt']
            dd['lns_px'] = dd['price2']
            dd['lns_s'] = dd['lns']
            dd['lns'] = dd['state2']

    def update_pivot_diff_day(self, dd):
        # print(self.jlix)
        piv_rec = self.jl_recs[self.jlix[dd['lns_dt']]]
        if self.primary(piv_rec[self.col_ix['state2']]):
            piv_rec[self.col_ix['pivot2']] = 1
            dd['p1_px'] = piv_rec[self.col_ix['price2']]
            dd['p1_s'] = piv_rec[self.col_ix['state2']]
        else:
            piv_rec[self.col_ix['pivot']] = 1
            dd['p1_px'] = piv_rec[self.col_ix['price']]
            dd['p1_s'] = piv_rec[self.col_ix['state']]
        dd['p1_dt'] = dd['lns_dt']

    def nextjl(self):
        dtc = self.ts.current_date()
        split_info = self.ts.splits.get(pd.Timestamp(dtc))
        if split_info is not None:
            self.adjust_for_splits(split_info[0])
        cdef float fctr = self.f * self.avg_rg
        if self.last['state'] == Stx_JL.SRa:
            self.sRa(fctr)
        elif self.last['state'] == Stx_JL.NRa:
            self.nRa(fctr)
        elif self.last['state'] == Stx_JL.UT:
            self.uT(fctr)
        elif self.last['state'] == Stx_JL.DT:
            self.dT(fctr)
        elif self.last['state'] == Stx_JL.NRe:
            self.nRe(fctr)
        elif self.last['state'] == Stx_JL.SRe:
            self.sRe(fctr)
        self.trs.pop(0)
        sr = self.ts.df.ix[self.ts.pos]
        sr_1 = self.ts.df.ix[self.ts.pos - 1]
        self.trs.append(max(sr.hi, sr_1.c) - min(sr.lo, sr_1.c))
        self.avg_rg = np.mean(self.trs)

    def adjust_for_splits(self, float ratio):
        cdef int ixx
        for ixx in range(0, len(self.lp)):
            self.lp[ixx] = self.lp[ixx] * ratio
        for jlr in self.jl_recs[1:]:
            jlr[self.col_ix['rg']] = jlr[self.col_ix['rg']] * ratio
            jlr[self.col_ix['price']] = jlr[self.col_ix['price']] * ratio
            jlr[self.col_ix['price2']] = jlr[self.col_ix['price2']] * ratio
            jlr[self.col_ix['p1_px']] = jlr[self.col_ix['p1_px']] * ratio
            jlr[self.col_ix['lns_px']] = jlr[self.col_ix['lns_px']] * ratio
        self.last['prim_px'] = self.last['prim_px'] * ratio
        self.last['px'] = self.last['px'] * ratio
        self.trs[:] = [x * ratio for x in self.trs]

    def sRa(self, float fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = Stx_JL.Nil, Stx_JL.Nil
        if self.lp[Stx_JL.UT] < r.hi:
            sh = Stx_JL.UT
        elif self.lp[Stx_JL.m_NRa] + fctr < r.hi:
            if self.last['prim_state'] in [Stx_JL.NRa, Stx_JL.UT]:
                sh = Stx_JL.UT if r.hi > self.last['prim_px'] else Stx_JL.SRa
            else:
                sh = Stx_JL.UT
        elif self.lp[Stx_JL.NRa] < r.hi and self.last['prim_state'] != Stx_JL.UT:
            sh = Stx_JL.NRa
        elif self.lp[Stx_JL.SRa] < r.hi:
            sh = Stx_JL.SRa
        if self.up(sh) and self.dn(self.last['prim_state']):
            self.lp[Stx_JL.m_NRe] = self.last['prim_px']
        if r.lo < self.lp[Stx_JL.SRa] - 2 * fctr:
            if self.lp[Stx_JL.NRe] < r.lo:
                sl = Stx_JL.SRe
            else:
                sl = Stx_JL.DT if(r.lo < self.lp[Stx_JL.DT] or
                                 r.lo < self.lp[Stx_JL.m_NRe] - fctr) \
                    else Stx_JL.NRe
                if self.up(self.last['prim_state']):
                    self.lp[Stx_JL.m_NRa] = self.last['prim_px']
        self.rec_day(sh, sl)

    def nRa(self, float fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = Stx_JL.Nil, Stx_JL.Nil
        if self.lp[Stx_JL.UT] < r.hi or self.lp[Stx_JL.m_NRa] + fctr < r.hi:
            sh = Stx_JL.UT
        elif self.lp[Stx_JL.NRa] < r.hi:
            sh = Stx_JL.NRa
        if r.lo < self.lp[Stx_JL.NRa] - 2 * fctr:
            if self.lp[Stx_JL.NRe] < r.lo:
                sl = Stx_JL.SRe
            elif r.lo < self.lp[Stx_JL.DT] or r.lo < self.lp[Stx_JL.m_NRe] - fctr:
                sl = Stx_JL.DT
            else:
                sl = Stx_JL.NRe
            if sl != Stx_JL.SRe:
                self.lp[Stx_JL.m_NRa] = self.lp[Stx_JL.NRa]
        self.rec_day(sh, sl)

    def uT(self, float fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = Stx_JL.Nil, Stx_JL.Nil
        if self.lp[Stx_JL.UT] < r.hi:
            sh = Stx_JL.UT
        if r.lo <= self.lp[Stx_JL.UT] - 2 * fctr:
            sl = Stx_JL.DT if (r.lo < self.lp[Stx_JL.DT] or
                              r.lo < self.lp[Stx_JL.m_NRe] - fctr) \
                else Stx_JL.NRe
            self.lp[Stx_JL.m_NRa] = self.lp[Stx_JL.UT]
        self.rec_day(sh, sl)

    def sRe(self, float fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = Stx_JL.Nil, Stx_JL.Nil
        if self.lp[Stx_JL.DT] > r.lo:
            sl = Stx_JL.DT
        elif self.lp[Stx_JL.m_NRe] - fctr > r.lo:
            if self.last['prim_state'] in [Stx_JL.NRe, Stx_JL.DT]:
                sl = Stx_JL.DT if r.lo < self.last['prim_px'] else Stx_JL.SRe
            else:
                sl = Stx_JL.DT
        elif self.lp[Stx_JL.NRe] > r.lo and self.last['prim_state'] != Stx_JL.DT:
            sl = Stx_JL.NRe
        elif self.lp[Stx_JL.SRe] > r.lo:
            sl = Stx_JL.SRe
        if self.dn(sl) and self.up(self.last['prim_state']):
            self.lp[Stx_JL.m_NRa] = self.last['prim_px']
        if r.hi > self.lp[Stx_JL.SRe] + 2 * fctr:
            if self.lp[Stx_JL.NRa] > r.hi:
                sh = Stx_JL.SRa
            else:
                sh = Stx_JL.UT if(r.hi > self.lp[Stx_JL.UT] or
                                 r.hi > self.lp[Stx_JL.m_NRa] + fctr) \
                                 else Stx_JL.NRa
                if self.dn(self.last['prim_state']):
                    self.lp[Stx_JL.m_NRe] = self.last['prim_px']
        self.rec_day(sh, sl)

    def dT(self, float fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = Stx_JL.Nil, Stx_JL.Nil
        if self.lp[Stx_JL.DT] > r.lo:
            sl = Stx_JL.DT
        if r.hi >= self.lp[Stx_JL.DT] + 2 * fctr:
            sh = Stx_JL.UT if (r.hi > self.lp[Stx_JL.UT] or
                              r.hi > self.lp[Stx_JL.m_NRa] + fctr) \
                else Stx_JL.NRa
            self.lp[Stx_JL.m_NRe] = self.lp[Stx_JL.DT]
        self.rec_day(sh, sl)

    def nRe(self, float fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = Stx_JL.Nil, Stx_JL.Nil
        if self.lp[Stx_JL.DT] > r.lo or self.lp[Stx_JL.m_NRe] - fctr > r.lo:
            sl = Stx_JL.DT
        elif self.lp[Stx_JL.NRe] > r.lo:
            sl = Stx_JL.NRe
        if r.hi > self.lp[Stx_JL.NRe] + 2 * fctr:
            if self.lp[Stx_JL.NRa] > r.hi:
                sh = Stx_JL.SRa
            elif r.hi > self.lp[Stx_JL.UT] or r.hi > self.lp[Stx_JL.m_NRa] + fctr:
                sh = Stx_JL.UT
            else:
                sh = Stx_JL.NRa
            if sh != Stx_JL.SRa:
                self.lp[Stx_JL.m_NRe] = self.lp[Stx_JL.NRe]
        self.rec_day(sh, sl)

    def up(self, state):
        return state in [Stx_JL.NRa, Stx_JL.UT]

    def dn(self, state):
        return state in [Stx_JL.NRe, Stx_JL.DT]

    def up_all(self, state):
        return state in [Stx_JL.SRa, Stx_JL.NRa, Stx_JL.UT]

    def dn_all(self, state):
        return state in [Stx_JL.SRe, Stx_JL.NRe, Stx_JL.DT]

    def primary(self, state):
        return state in [Stx_JL.NRa, Stx_JL.UT, Stx_JL.NRe, Stx_JL.DT]

    def secondary(self, state):
        return state in [Stx_JL.SRa, Stx_JL.SRe]

    def jlr_print(self, jlr):
        return 'dt:{0:s} rg:{1:.2f} s:{2:d} px:{3:.2f} p:{4:d} s2:{5:d} ' \
            'px2:{6:.2f} p2:{7:d} p1dt:{8:s} p1px:{9:.2f} p1s:{10:d} ' \
            'ldt:{11:s} lpx:{12:.2f} lns_s:{13:d} lns:{14:d} ls_s:{15:d} ' \
            'ls:{16:d}'. \
            format(jlr[self.col_ix['dt']], jlr[self.col_ix['rg']],
                   jlr[self.col_ix['state']], jlr[self.col_ix['price']],
                   jlr[self.col_ix['pivot']], jlr[self.col_ix['state2']],
                   jlr[self.col_ix['price2']], jlr[self.col_ix['pivot2']],
                   jlr[self.col_ix['p1_dt']], jlr[self.col_ix['p1_px']],
                   jlr[self.col_ix['p1_s']], jlr[self.col_ix['lns_dt']],
                   jlr[self.col_ix['lns_px']], jlr[self.col_ix['lns_s']],
                   jlr[self.col_ix['lns']], jlr[self.col_ix['ls_s']],
                   jlr[self.col_ix['ls']])

    def jlr_print2(self, jlr):
        return 's:{0:d} px:{1:.2f} p:{2:d} s2:{3:d} px2:{4:.2f} p2:{5:d} ' \
            'p1dt:{6:s} p1px:{7:.2f} p1s:{8:d} ldt:{9:s} lpx:{10:.2f} ' \
            'lns:{11:d} ls_s:{12:d} ls:{13:d}'. \
            format(jlr[self.col_ix['state']], jlr[self.col_ix['price']],
                   jlr[self.col_ix['pivot']], jlr[self.col_ix['state2']],
                   jlr[self.col_ix['price2']], jlr[self.col_ix['pivot2']],
                   jlr[self.col_ix['p1_dt']], jlr[self.col_ix['p1_px']],
                   jlr[self.col_ix['p1_s']], jlr[self.col_ix['lns_dt']],
                   jlr[self.col_ix['lns_px']], jlr[self.col_ix['lns']],
                   jlr[self.col_ix['ls_s']], jlr[self.col_ix['ls']])

    def get_formatted_price(self, state, pivot, price):
        s_fmt = ''
        e_fmt = '\x1b[0m'
        if state == Stx_JL.UT:
            s_fmt = Stx_JL.UT_fmt if pivot == 0 else Stx_JL.UP_piv_fmt
        elif state == Stx_JL.DT:
            s_fmt = Stx_JL.DT_fmt if pivot == 0 else Stx_JL.DN_piv_fmt
        elif pivot == 1:
            s_fmt = Stx_JL.UP_piv_fmt if state == Stx_JL.NRe else \
                    Stx_JL.DN_piv_fmt
        else:
            e_fmt = ''
        s_price = '{0:s}{1:9.2f}{2:s}'.format(s_fmt, price, e_fmt)
        return '{0:s}'.format(54 * ' ') if state == Stx_JL.Nil else \
            '{0:s}{1:s}{2:s}'.format((9 * state) * ' ', s_price,
                                     (9 * (5 - state)) * ' ')

    def jl_print(self, print_pivots_only=False, print_nils=False,
                 print_dbg=False):
        output = ''
        for jlr in self.jl_recs[1:]:
            state = jlr[self.col_ix['state']]
            pivot = jlr[self.col_ix['pivot']]
            price = jlr[self.col_ix['price']]
            if print_pivots_only and pivot == 0:
                continue
            if not print_nils and state == Stx_JL.Nil:
                continue
            px_str = self.get_formatted_price(state, pivot, price)
            output += '{0:s}{1:s}{2:6.2f} {3:s}\n'. \
                format(jlr[self.col_ix['dt']], px_str, jlr[self.col_ix['rg']],
                       '' if not print_dbg else self.jlr_print2(jlr))
            state2 = jlr[self.col_ix['state2']]
            if state2 == Stx_JL.Nil:
                continue
            pivot2 = jlr[self.col_ix['pivot2']]
            if print_pivots_only and pivot2 == 0:
                continue
            price2 = jlr[self.col_ix['price2']]
            px_str = self.get_formatted_price(state2, pivot2, price2)
            output += '{0:s}{1:s}{2:6.2f} {3:s}\n'.\
                format(jlr[self.col_ix['dt']], px_str, jlr[self.col_ix['rg']],
                       '' if not print_dbg else self.jlr_print2(jlr))
        print(output)

    def get_num_pivots(self, num_pivs):
        ixx = -1
        end = -len(self.jl_recs)
        pivs = []
        while len(pivs) < num_pivs and ixx >= end:
            jlr = self.jl_recs[ixx]
            if jlr[self.col_ix['pivot2']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state2']],
                                    jlr[self.col_ix['price2']],
                                    jlr[self.col_ix['rg']]))
            if len(pivs) < num_pivs and jlr[self.col_ix['pivot']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state']],
                                    jlr[self.col_ix['price']],
                                    jlr[self.col_ix['rg']]))
            ixx -= 1
        pivs.reverse()
        return pivs

    def get_pivots_in_days(self, num_days):
        ixx = -1
        end = -len(self.jl_recs)
        pivs = []
        if end < -num_days:
            end = -num_days
        while ixx > end:
            jlr = self.jl_recs[ixx]
            if jlr[self.col_ix['pivot2']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state2']],
                                    jlr[self.col_ix['price2']],
                                    jlr[self.col_ix['rg']]))
            if jlr[self.col_ix['pivot']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state']],
                                    jlr[self.col_ix['price']],
                                    jlr[self.col_ix['rg']]))
            ixx -= 1
        pivs.reverse()
        return pivs

    def print_pivs(self, pivs):
        output = ''
        for piv in pivs:
            px_str = self.get_formatted_price(piv.state, 1, piv.price)
            output += '{0:s}{1:s}{2:6.2f}\n'.format(piv.dt, px_str, piv.rg)
        print(output)

    def last_rec(self, col_name, ixx=1):
        if ixx > len(self.jl_recs):
            ixx = len(self.jl_recs)
        jlr = self.jl_recs[-ixx]
        if col_name in ['state', 'price', 'pivot']:
            col_name2 = '{0:s}2'.format(col_name)
            if jlr[self.col_ix['state2']] != Stx_JL.Nil:
                return jlr[self.col_ix[col_name2]]
        return jlr[self.col_ix[col_name]]


if __name__ == '__main__':
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    dt = sys.argv[4]
    factor = float(sys.argv[5])
    ts = StxTS(stk, sd, ed)
    jl = Stx_JL(ts, factor)
    jlres = jl.jl(dt)
    jl.jl_print()
    pivs = jl.get_pivots_in_days(100)
    print("Pivs in 100 days:")
    jl.print_pivs(pivs)
    pivs = jl.get_num_pivots(4)
    print("4 pivs:")
    jl.print_pivs(pivs)
    # jl.jl_print(print_pivots_only = True)
    # pd.set_option('display.max_rows', 2000)
    # pd.set_option('display.max_columns', 1500)
    # pd.set_option('display.width', 1500)
    # print(jlres)
