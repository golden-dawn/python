import collections
import csv
import pandas as pd
from recordclass import recordclass
from stxcal import StxCal
from stxjl import StxJL, JLPivot
from stxts import StxTS


PivotAnalysis = recordclass("PivotAnalysis", "support_resistance, " \
                            "up_above, dn_below, above, below, min_px, " \
                            "max_px, hilo_pct, setup, obv, lns_150")
OptEntry      = recordclass("OptEntry", "dt, spot_px, opt_px")
OptStats      = recordclass("OptStats", "pl_pct, pl, success, gain_pct, " \
                            "loss_pct, gain, loss")
Trade         = recordclass("Trade", "stk, cp, exp, exp_bd, strike, opt_in, " \
                            "num, avg_range, opt_out, opt_stats, opts, " \
                            "pivot_analysis, ind_ranks")
IndRanks      = recordclass("IndRanks", "r_rs_252, udv_21, udv_42, udv_63, " \
                            "rs_21, min_rnk_252, min_rnk_252b")


# check for breakouts / breakdowns. Consider only the setups where the
# length of the base is >= 30 business days
def check_for_breaks(ts, jl, sc, pivs, sgn) :
    if sgn == 0 or len(pivs) < 2 :
        return None, None, None, None
    last_state = jl.last_rec('state')
    if(sgn == 1 and last_state != StxJL.UT) or \
      (sgn == -1 and last_state != StxJL.DT) :
        return None, None, None, None
#    prev_state = jl.last_rec('lns_s')
#    if prev_state == last_state :
#        return None, None, None, None
    edt        = ts.current_date()
    px         = ts.current('h') if sgn == 1 else ts.current('l')
    pivs_len   = -len(pivs) - 1
    max_px     = sgn * pivs[-2].price
    for ixx in range(-2, pivs_len, -2) :
        if sgn * px < sgn * pivs[ixx].price :
            return None, None, None, None
        if pivs[ixx].state in [StxJL.NRa, StxJL.NRe] or \
           sgn * pivs[ixx].price < max_px:
            continue
        max_px = sgn * pivs[ixx].price
        sdt = pivs[ixx].dt
        base_length = sc.num_busdays(sdt, edt)
        if base_length >= 30 :
            return 'call' if sgn == 1 else 'put', \
                'breakout' if sgn == 1 else 'breakdown', \
                obv(ts, pivs, jl.last_rec('dt')), jl.last_rec('rg')
    return None, None, None, None


def check_for_pullbacks(ts, jl, sc, pivs, sgn) :
    edt = ts.current_date()
    # pivs   = jl.get_num_pivots(4)
    if len(pivs) < 2 or sgn == 0:
        return None, None, None, None
    last_state = jl.last_rec('state')
    if last_state == StxJL.SRa :
        if sgn == 1 and pivs[-1].state == StxJL.UT and \
           pivs[-2].state == StxJL.NRe  and jl.last_rec('ls_s') == StxJL.NRe :
            return 'call', 'RevUpSRa', obv(ts, pivs, jl.last_rec('lns_dt')), \
                jl.last_rec('rg')
    elif last_state == StxJL.SRe :
        if sgn == -1 and pivs[-1].state == StxJL.DT and \
           pivs[-2].state == StxJL.NRa and jl.last_rec('ls_s') == StxJL.NRa :
            return 'put', 'RevDnSRe', obv(ts, pivs, jl.last_rec('lns_dt')), \
                jl.last_rec('rg')
    elif last_state in [StxJL.NRa, StxJL.UT] :
        if sgn == 1 and pivs[-1].state == StxJL.NRe and \
           pivs[-2].state == StxJL.UT and jl.last_rec('lns_s') == StxJL.NRe :
            return 'call', 'RevUpNRa' if last_state==StxJL.NRa else 'RevUpUT', \
                obv(ts, pivs, None), jl.last_rec('rg')
    elif last_state in [StxJL.NRe, StxJL.DT] :
        if sgn == -1 and pivs[-1].state == StxJL.NRa and \
           pivs[-2].state == StxJL.DT and jl.last_rec('lns_s') == StxJL.NRa :
            return 'put', 'RevDnNRe' if last_state==StxJL.NRe else 'RevDnDT', \
                obv(ts, pivs, None), jl.last_rec('rg')
    return None, None, None, None


def check_for_trend(ts, cp, min_call, max_put) :
    if cp is not None :
        if cp == 'call' :
            if ts.current('c') < min_call or min_call == -1:
                cp = None
            else :
                min_call = ts.current('c')
                # max_put  = 1000000
        elif cp == 'put' :
            if ts.current('c') > max_put or max_put == -1:
                cp = None
            else :
                max_put  = ts.current('c')
                # min_call = 0
    return cp, min_call, max_put


def get_trade_type(ts, sc, jl_150, jl_050, pivs, lt_trend, min_call, max_put) :
    # print('{0:s}: ldr = {1:.0f}'.format(ts.current_date(), ts.current('ldr')))
    cp, stp, obv, avg_rg  = check_for_breaks(ts, jl_150, sc, pivs, lt_trend)
    if cp == 'call' :
        min_call          = ts.current('c')
    if cp == 'put' :
        max_put           = ts.current('c')
    if ts.current('ldr') != 1 :
        return None, None, None, None, min_call, max_put
    cp, min_call, max_put = check_for_trend(ts, cp, min_call, max_put)
    if cp is not None :
        return cp, stp, obv, avg_rg, min_call, max_put
    cp, stp, obv, avg_rg  = check_for_pullbacks(ts, jl_050, sc, pivs, lt_trend)
    cp, min_call, max_put = check_for_trend(ts, cp, min_call, max_put)
    return cp, stp, obv, avg_rg, min_call, max_put


def obv(ts, pivs, dt) :
    if len(pivs) < 3 :
        return 0
    sd           = pivs[-3].dt if dt is None else pivs[-2].dt
    ed           = pivs[-1].dt if dt is None else dt
    s, e         = ts.find(sd), ts.find(ed)
    tot_vol      = 0
    obv          = 0
    for ixx in range(s + 1, e + 1) :
        sr       = ts.df.ix[ixx]
        sr_1     = ts.df.ix[ixx - 1]
        tot_vol += sr.v
        if sr.c > sr_1.c :
            obv += sr.v
        else :
            obv -= sr.v
    avg_vol      = tot_vol / (e - s)
    # print('obv: sd = {0:s}, ed = {1:s}, obv = {2:.0f} avg_vol = {3:.0f}'.\
    #       format(sd, ed, obv, avg_vol))
    obv         /= avg_vol
    return -3 if obv <= -3 else 3 if obv >= 3 else \
        float('{0:.0f}'.format(obv / 0.5)) * 0.5


def pivotal_analysis(cp, ts, jl_150, pivs) :
    min_px, max_px     = 1000000, 0
    dn_below, up_above = 0, 0
    below, above       = 0, 0
    sr                 = []
    lns_px             = jl_150.last_rec('lns_px')
    rg                 = jl_150.last_rec('rg')
    cc                 = ts.current('c')
    for piv in pivs :
        dt, state, price, rg = piv[0], piv[1], piv[2], piv[3]
        if cc >= price :
            above     += 1
        else :
            below     += 1
        if jl_150.up(state) :
            if price > max_px :
                max_px = price
            if cc >= price :
                up_above += 1
            if cp == 'call' and price - lns_px >= -0.5 * rg and \
               price - lns_px <= 0.5 * rg :
                sr.append('({0:s} {1:d} {2:.2f} {3:.2f})'. \
                          format(dt, state, price, rg))
        else : # jl.dn(state)
            if price < min_px :
                min_px = price
            if cc <= price :
                dn_below += 1
            if cp == 'put' and price - lns_px >= -0.5 * rg and \
               price - lns_px <= 0.5 * rg :
                sr.append('({0:s} {1:d} {2:.2f} {3:.2f})'.\
                          format(dt, state, price, rg))
    x        = 100 * (cc - min_px) / (max_px - min_px)
    hilo_pct = 100 if x >= 100 else 0 if x <= 0 else (x // 10) * 10
    return PivotAnalysis(sr, up_above, dn_below, above, below, min_px, max_px,
                         hilo_pct, None, None, jl_150.last_rec('lns'))


def trade(ts, sc, cp, sl, rg, piv_analysis, ind_ranks) :
    crt_dt    = ts.current_date()
    cc        = ts.current('c')
    exp       = str(sc.next_expiry(crt_dt, 15))
    exp_bd    = str(sc.prev_busday(sc.move_busdays(exp, 0)).date())
    if cp == "call" :
        all_q = "select strike,dt,bid,ask from opts where exp='%s' and und=" \
                "'%s' and strike <= %.2f and cp='call'" % (exp, ts.stk, cc)
        # "'%s' and strike <= %.2f and cp='call'" % (exp, ts.stk, lns_px)
        opt_q = "{0:s} and dt='{1:s}'".format(all_q, crt_dt)
    else :
        all_q = "select strike,dt,bid,ask from opts where exp='%s' and und="\
                "'%s' and strike >= %.2f and cp='put'" % (exp, ts.stk, cc)
        # "'%s' and strike >= %.2f and cp='put'" % (exp, ts.stk, lns_px)
    opt_q = "{0:s} and dt='{1:s}'".format(all_q, crt_dt)
    # print('crt_dt={0:s}, opt_q={1:s}'.format(crt_dt, opt_q))
    crt_opts  = pd.read_sql(opt_q, ts.cnx)
    opts      = pd.read_sql(all_q, ts.cnx)
    strike    = crt_opts['strike'].max() if cp == 'call' else \
                crt_opts['strike'].min()
    # print('strike={0:s}, strike={1:2f}'.format(crt_dt, strike))
    trd_opts  = opts[opts['strike']==strike]
    t_opts    = collections.OrderedDict()
    for row in trd_opts.iterrows() :
        t_opts[row[1][1]] = [row[1].bid, row[1].ask]
    # print(t_opts)
    if t_opts.get(crt_dt) is None or t_opts[crt_dt][1] == 0:
        return None
    num       = int(6 / t_opts[crt_dt][1]) * 100
    opt_in    = OptEntry(crt_dt, ts.current('c'), t_opts[crt_dt][1])
    return Trade(ts.stk, cp, exp, exp_bd, strike, opt_in, num, sl, None,
                 None, t_opts, piv_analysis, ind_ranks)


def to_short_string(trd) :
    return '  {0:s} {1:s} {2:s} {3:s} {4:.2f} {5:s}'.\
        format(trd.stk, trd.opt_in.dt, trd.cp, trd.exp, trd.strike, trd.exp_bd)


def risk_mgmt(ts, open_trades, trades) :
    crt_dt                   = ts.current_date()
    closed_trades            = []
    for trd in open_trades :
        if trd.exp_bd == crt_dt :
            bid_ask          = trd.opts.get(crt_dt)
            crt_spot         = ts.current('c')
            losing_trade     = False
            if (trd.cp == 'call' and crt_spot < trd.strike - trd.avg_range) or \
               (trd.cp == 'put' and crt_spot > trd.strike + trd.avg_range) :
                losing_trade = True
            if bid_ask is None and losing_trade == False :
                open_trades.remove(trd)
                print('Removing trade because no data for {0:s}:\n {1:s}'.\
                      format(crt_dt, to_short_string(trd)))
                continue
            in_px     = trd.opt_in.opt_px
            out_px    = bid_ask[0] if bid_ask is not None else 0
            opt_out   = OptEntry(crt_dt, crt_spot, out_px)
            success   = 1 if out_px > in_px else 0
            pnl_pct   = out_px / in_px - 1
            pnl       = trd.num * (out_px - in_px)
            opt_stats = OptStats \
                        (pnl_pct, pnl, success,
                         '{0:.2f}'.format(pnl_pct) if success == 1 else '',
                         '{0:.2f}'.format(pnl_pct) if success == 0 else '',
                         '{0:.2f}'.format(pnl) if success == 1 else '',
                         '{0:.2f}'.format(pnl) if success == 0 else '')
            trd.opt_stats = opt_stats
            trd.opt_out   = opt_out
            closed_trades.append(trd)
    for trd in closed_trades :
        open_trades.remove(trd)
        trades.append(trd)


def to_csv_list(trd) :
    return [trd.stk, trd.opt_in.dt, trd.opt_in.dt[:4], trd.cp, trd.exp,
            trd.strike, trd.opt_in.spot_px, trd.opt_in.opt_px, trd.num,
            trd.avg_range, trd.opt_out.dt, trd.opt_out.spot_px,
            trd.opt_out.opt_px, trd.opt_stats.pl_pct, trd.opt_stats.pl,
            trd.opt_stats.success, trd.opt_stats.gain_pct,
            trd.opt_stats.loss_pct, trd.opt_stats.gain, trd.opt_stats.loss,
            trd.pivot_analysis.support_resistance,
            trd.pivot_analysis.up_above, trd.pivot_analysis.dn_below,
            trd.pivot_analysis.above, trd.pivot_analysis.below,
            trd.pivot_analysis.hilo_pct, trd.pivot_analysis.setup,
            # trd.pivot_analysis.obv, trd.pivot_analysis.lns_150,
            trd.ind_ranks.r_rs_252, trd.ind_ranks.udv_21, trd.ind_ranks.udv_42,
            trd.ind_ranks.udv_63, trd.ind_ranks.rs_21,
            trd.ind_ranks.min_rnk_252, trd.ind_ranks.min_rnk_252b]


def get_trend(ts, jl, pivs) :
    if len(pivs) < 1 :
        return 0
    if jl.last_rec('lns') == StxJL.UT :
        return 1
    if jl.last_rec('lns') == StxJL.DT :
        return -1
    if jl.last_rec('lns') == StxJL.NRe and pivs[-1].state == StxJL.UT :
        return 1
    if jl.last_rec('lns') == StxJL.NRa and pivs[-1].state == StxJL.DT :
        return -1
   
    # if jl.last_rec('lns') in [StxJL.UT, StxJL.NRe] and \
    #    pivs[-1][1] in [StxJL.UT, StxJL.NRe] :
    #     return 1
    # if jl.last_rec('lns') == StxJL.NRa and pivs[-1][1] == StxJL.NRe and \
    #    len(pivs) >= 2 and pivs[-2][1] == StxJL.UT :
    #     return 1
    # if jl.last_rec('lns') in [StxJL.DT, StxJL.NRa] and \
    #    pivs[-1][1] in [StxJL.DT, StxJL.NRa] :
    #     return -1
    # if jl.last_rec('lns') == StxJL.NRe and pivs[-1][1] == StxJL.NRa and \
    #    len(pivs) >= 2 and pivs[-2][1] == StxJL.DT :
    #     return -1
    # if jl.last_rec('lns') == StxJL.UT and pivs[-1][1] == StxJL.DT and \
    #    len(pivs) >= 3 and pivs[-1][2] + jl.f * pivs[-1][3] > pivs[-3][2] :
    #     return 1
    # if jl.last_rec('lns') == StxJL.DT and pivs[-1][1] == StxJL.UT and \
    #    len(pivs) >= 3 and pivs[-1][2] - jl.f * pivs[-1][3] < pivs[-3][2] :
    #     return -1
    return 0


def analyze(ts, sc, calls, puts, fname, fmode) :
    trades          = []
    open_trades     = []
    min_call        = -1
    max_put         = -1
    for gap in ts.gaps:
        start, end  = gap[0], gap[1]
        ts.set_day(str(start.date()))
        ldr_df      = ts.df[start:end].query('ldr==1')
        # print('len(ldr_df) = {0:d}'.format(len(ldr_df)))
        if len(ldr_df) == 0 :
            continue       
        jl_150      = StxJL(ts, 1.50)
        jl_050      = StxJL(ts, 0.50)
        ts.set_day(str(ts.df.index[ts.start + 20].date()))
        start_w     = jl_150.initjl()
        start_w     = jl_050.initjl()
        print('start_w = {0:d}, jl_150.w = {1:d}'.format(start_w, jl_150.w))
        ixx         = start_w if start_w >= jl_150.w else -1
        lt_trend    = 0
        while ixx != -1 :
            ixx     = ts.next_day()
            if ixx == -1 :
                break
            jl_150.nextjl()
            jl_050.nextjl()
            pivs    = jl_150.get_pivots_in_days(400)
            risk_mgmt(ts, open_trades, trades)
            # print('{0:s}'.format(ts.current_date()))
            # for piv in pivs(
            lt_trend = get_trend(ts, jl_150, pivs)
            cp, stp, obv, sl, min_call, max_put = get_trade_type \
                                                  (ts, sc, jl_150, jl_050, pivs,
                                                   lt_trend, min_call, max_put)
            if cp is None :
                continue
            print('{0:s} {1:s} {2:s}'.format(ts.current_date(), cp, stp))
            trades_by_exp = calls if cp == 'call' else puts
            stk_trades_by_exp = trades_by_exp.get(ts.stk)
            if stk_trades_by_exp is None :
                stk_trades_by_exp = {}
                trades_by_exp[stk] = stk_trades_by_exp
            exp          = str(sc.next_expiry(ts.current_date(), 15))
            open_trades_by_exp = stk_trades_by_exp.get(exp)
            if open_trades_by_exp is not None :
                print('{0:s}: Trade already there {1:s}'.\
                      format(ts.current_date(),
                             to_short_string(open_trades_by_exp)))
                continue
            piv_an       = pivotal_analysis(cp, ts, jl_150, pivs)
            piv_an.setup = stp
            piv_an.obv   = obv
            ind_ranks    = IndRanks(ts.current('r_rs_252'),
                                    ts.current('udv_21b'),
                                    ts.current('udv_42b'),
                                    ts.current('udv_63b'),
                                    ts.current('rs_21b'),
                                    ts.current('rs_min_252'),
                                    ts.current('rs_min_252b'))
           
            # print(piv_an)
            trd          = trade(ts, sc, cp, sl, jl_150.last_rec('rg'), piv_an,
                                 ind_ranks)
            if trd is not None :
                open_trades.append(trd)
                stk_trades_by_exp[exp] = trd
                trades_by_exp[ts.stk]  = stk_trades_by_exp
                print('New Trade: {0:s}'.format(to_short_string(trd)))
    with open(fname, fmode, newline='') as fp :
        wrtr = csv.writer(fp, delimiter=',')
        if fmode == 'w' :
            wrtr.writerow(['Stock', 'Date', 'Year', 'CP', 'Expiry', 'Strike', \
                           'InSpot', 'InOpt', 'Contracts', 'Range', 'OutDate',
                           'OutSpot', 'OutOpt', 'P&L Pct', 'P&L', 'Success',
                           'GainPct', 'LossPct', 'Gain', 'Loss',
                           'SupportResistance', 'UpAbove', 'DnBelow', 'Above',
                           'Below', 'HiLoPct', 'Setup', 'RSRank252', 'UDV21',
                           'UDV42', 'UDV63', 'RS21', 'Best_252', 'Best_252b'])
        for trd in trades :
            wrtr.writerow(to_csv_list(trd))


stx   = 'NFLX'
# stx   = 'INTU,NFLX,VLO,TSLA,GIGM,NTES,ACAD,AMR,HOV,CSCO,INTC,CSIQ,FSLR,TASR,LUV,USB,GOOG,MSFT,GLD,SPY,YHOO,AAPL,AMZN,MA,TIE'
# stx   = 'FSLR,TASR,LUV,USB,GOOG,MSFT,GLD,SPY,YHOO,AAPL,AMZN,MA,TIE'
name  = 'c3'
sd    = '2001-01-01'
ed    = '2013-12-31'
sc    = StxCal()
ixx   = 0
slst  = stx.split(',')
fname = '{0:s}_8.csv'.format(stx if len(slst) == 1 else name)
for stk in slst :
    print('{0:s}: '.format(stk))
    ts    = StxTS(stk, sd, ed)
    tdf   = StxTS.gen_tdf(sd, ed)
    ts.addleader(tdf)
    ts.mergetbl('ranks', ['r_rs_252'], "and rm_ten='PctLiq'")
    ts.df['rs_min_252'] = pd.rolling_min(ts.df['r_rs_252'], 400, min_periods=1)
    ts.buckets('rs_min_252', 0, 1000, 100, 0)
    ts.buckets('r_rs_252', 0, 1000, 100, 0)
    ts.mergetbl('indx', ['udv_21', 'udv_42', 'udv_63', 'rs_21'])
    ts.buckets('udv_21', 0, 100, 5, 0)
    ts.buckets('udv_42', 0, 100, 5, 0)
    ts.buckets('udv_63', 0, 100, 5, 0)
    ts.buckets('rs_21', -24, 24, 3, 0)
    calls = {}
    puts  = {}
    fmode = 'w' if ixx == 0 else 'a'
    ixx  += 1
    analyze(ts, sc, calls, puts, fname, fmode)
