import collections
import csv
import pandas as pd
from recordclass import recordclass
import stxcal
import stxdb
from stxjl import StxJL
from stxts import StxTS

OptEntry = recordclass("OptEntry", "dt, spot_px, opt_px, spread")
OptStats = recordclass("OptStats", "pl_pct, pl, success, gain_pct, loss_pct, "
                       "gain, loss")
Trade = recordclass("Trade", "stk, cp, exp, exp_bd, strike, opt_in, num, "
                    "avg_range, opt_out, opt_stats, opts, stp")


# check for breakouts / breakdowns. Consider only the setups where the
# length of the base is >= 30 business days
def check_for_breaks(ts, jl, pivs, sgn):
    if sgn == 0 or len(pivs) < 2:
        return None, None, None
    last_state = jl.last_rec('state')
    if(sgn == 1 and last_state != StxJL.UT) or \
      (sgn == -1 and last_state != StxJL.DT):
        return None, None, None
    edt = ts.current_date()
    px = ts.current('h') if sgn == 1 else ts.current('l')
    pivs_len = -len(pivs) - 1
    max_px = sgn * pivs[-2].price
    for ixx in range(-2, pivs_len, -2):
        if sgn * px < sgn * pivs[ixx].price:
            return None, None, None
        if sgn * pivs[ixx].price < max_px:
            continue
        max_px = sgn * pivs[ixx].price
        prev_lns = jl.last_rec('lns_px', 2)
        prev_state = jl.last_rec('lns_s')
        if sgn * prev_lns > max_px and prev_state != StxJL.NRe:
            continue
        sdt = pivs[ixx].dt
        base_length = stxcal.num_busdays(sdt, edt)
        if base_length >= 30:
            return 'call' if sgn == 1 else 'put', \
                'breakout' if sgn == 1 else 'breakdown', \
                jl.last_rec('rg')
    return None, None, None


def check_for_pullbacks(ts, jl, sgn):
    pivs = jl.get_num_pivots(4)
    if len(pivs) < 2 or sgn == 0:
        return None, None, None
    last_state = jl.last_rec('state')
    if last_state == StxJL.SRa:
        if sgn == 1 and pivs[-1].state == StxJL.UT and \
           pivs[-2].state == StxJL.NRe and jl.last_rec('ls_s') == StxJL.NRe:
            return 'call', 'RevUpSRa', jl.last_rec('rg')
    elif last_state == StxJL.SRe:
        if sgn == -1 and pivs[-1].state == StxJL.DT and \
           pivs[-2].state == StxJL.NRa and jl.last_rec('ls_s') == StxJL.NRa:
            return 'put', 'RevDnSRe', jl.last_rec('rg')
    elif last_state in [StxJL.NRa, StxJL.UT]:
        if sgn == 1 and pivs[-1].state == StxJL.NRe and \
           pivs[-2].state == StxJL.UT and jl.last_rec('lns_s') == StxJL.NRe:
            return 'call', 'RevUpNRa' if last_state == StxJL.NRa \
                else 'RevUpUT', jl.last_rec('rg')
    elif last_state in [StxJL.NRe, StxJL.DT]:
        if sgn == -1 and pivs[-1].state == StxJL.NRa and \
           pivs[-2].state == StxJL.DT and jl.last_rec('lns_s') == StxJL.NRa:
            return 'put', 'RevDnNRe' if last_state == StxJL.NRe \
                else 'RevDnDT', jl.last_rec('rg')
    return None, None, None


def check_for_trend(ts, cp, min_call, max_put):
    print('check_for_trend: d={0:s},c={1:.2f},cp={2:s},mnc={3:.2f},mxp={4:.2f}'
          .format(ts.current_date(), ts.current('c'), cp, min_call, max_put))
    if cp is not None:
        if cp == 'call':
            if ts.current('c') < min_call or min_call == -1:
                cp = None
            else:
                min_call = ts.current('c')
        elif cp == 'put':
            if ts.current('c') > max_put or max_put == -1:
                cp = None
            else:
                max_put = ts.current('c')
    return cp, min_call, max_put


def get_trade_type(ts, jl_150, jl_050, pivs, lt_trend, min_call, max_put):
    cp, stp, avg_rg = check_for_breaks(ts, jl_150, pivs, lt_trend)
    if cp == 'call':
        min_call = ts.current('c')
    if cp == 'put':
        max_put = ts.current('c')
    # if ts.current('ldr') != 1:
    #     return None, None, None, min_call, max_put
    if cp is not None:
        return cp, stp, avg_rg, min_call, max_put
    cp, stp, avg_rg = check_for_pullbacks(ts, jl_050, lt_trend)
    if cp is not None:
        cp, min_call, max_put = check_for_trend(ts, cp, min_call, max_put)
    return cp, stp, avg_rg, min_call, max_put


def get_trade_opts(stk, exp, cp, cc, rg, dt, rg_fctr=0):
    if cp == "call":
        all_q = "select strike,dt,bid,ask from opts where exp='{0:s}' and "\
                "und='{1:s}' and strike <= {2:.2f} and cp='c'".format(
                    exp, ts.stk, cc + rg * rg_fctr)
    else:
        all_q = "select strike,dt,bid,ask from opts where exp='{0:s}' and "\
                "und='{1:s}' and strike >= {2:.2f} and cp='p'".format(
                    exp, ts.stk, cc - rg * rg_fctr)
    opt_q = "{0:s} and dt='{1:s}'".format(all_q, dt)
    crt_opts = pd.read_sql(opt_q, stxdb.db_get_cnx())
    opts = pd.read_sql(all_q, stxdb.db_get_cnx())
    strike = crt_opts['strike'].max() if cp == 'call' else \
        crt_opts['strike'].min()
    trd_opts = opts[opts['strike'] == strike]
    t_opts = collections.OrderedDict()
    for row in trd_opts.iterrows():
        t_opts[str(row[1][1])] = [row[1].bid, row[1].ask]
    return t_opts, strike


def trade(ts, cp, sl, rg, stp):
    crt_dt = ts.current_date()
    cc = ts.current('c')
    exp = str(stxcal.next_expiry(crt_dt, 15))
    exp_bd = stxcal.prev_busday(stxcal.move_busdays(exp, 0))
    t_opts, strike = get_trade_opts(ts.stk, exp, cp, cc, rg, crt_dt)
    if t_opts.get(crt_dt) is None or t_opts[crt_dt][1] == 0:
        return None
    num = int(6 / t_opts[crt_dt][1]) * 100
    opt_in = OptEntry(crt_dt, ts.current('c'), t_opts[crt_dt][1],
                      1 - t_opts[crt_dt][0] / t_opts[crt_dt][1])
    return Trade(ts.stk, cp, exp, exp_bd, strike, opt_in, num, sl, None,
                 None, t_opts, stp)


def to_short_string(trd):
    return '  {0:s} {1:s} {2:s} {3:s} {4:.2f} {5:s}'.\
        format(trd.stk, trd.opt_in.dt, trd.cp, trd.exp, trd.strike, trd.exp_bd)


def risk_mgmt(ts, open_trades, trades):
    crt_dt = ts.current_date()
    closed_trades = []
    for trd in open_trades:
        if trd.exp_bd == crt_dt:
            bid_ask = trd.opts.get(crt_dt)
            crt_spot = ts.current('c')
            losing_trade = False
            if (trd.cp == 'call' and crt_spot < trd.strike) or \
               (trd.cp == 'put' and crt_spot > trd.strike):
                losing_trade = True
            if bid_ask is None and not losing_trade:
                open_trades.remove(trd)
                print('Removing trade because no data for {0:s}:\n {1:s}'.
                      format(crt_dt, to_short_string(trd)))
                continue
            in_px = trd.opt_in.opt_px
            out_px = bid_ask[0] if bid_ask is not None else 0
            spread = 1 - out_px / in_px
            opt_out = OptEntry(crt_dt, crt_spot, out_px, spread)
            success = 1 if out_px > in_px else 0
            pnl_pct = out_px / in_px - 1
            pnl = trd.num * (out_px - in_px)
            opt_stats = OptStats(
                pnl_pct, pnl, success,
                '{0:.2f}'.format(pnl_pct) if success == 1 else '',
                '{0:.2f}'.format(pnl_pct) if success == 0 else '',
                '{0:.2f}'.format(pnl) if success == 1 else '',
                '{0:.2f}'.format(pnl) if success == 0 else '')
            trd.opt_stats = opt_stats
            trd.opt_out = opt_out
            closed_trades.append(trd)
    for trd in closed_trades:
        open_trades.remove(trd)
        trades.append(trd)


def to_csv_list(trd):
    return [trd.stk, trd.opt_in.dt, trd.opt_in.dt[:4], trd.cp, trd.exp,
            trd.strike, trd.opt_in.spot_px, trd.opt_in.opt_px,
            trd.opt_in.spread, trd.num, trd.avg_range, trd.opt_out.dt,
            trd.opt_out.spot_px, trd.opt_out.opt_px, trd.opt_stats.pl_pct,
            trd.opt_stats.pl, trd.opt_stats.success, trd.opt_stats.gain_pct,
            trd.opt_stats.loss_pct, trd.opt_stats.gain, trd.opt_stats.loss,
            trd.stp]


def get_trend(ts, jl, pivs):
    if len(pivs) < 2:
        return 0
    if jl.last_rec('lns') == StxJL.UT:
        return 1
    if jl.last_rec('lns') == StxJL.DT:
        return -1
    if pivs[-1].state == StxJL.UT and pivs[-2].state == StxJL.NRe:
        return 1
    if pivs[-1].state == StxJL.DT and pivs[-2].state == StxJL.NRa:
        return -1
    return 0


def analyze(ts, calls, puts, fname, fmode):
    trades = []
    open_trades = []
    min_call = -1
    max_put = -1
    for gap in ts.gaps:
        start, end = gap[0], gap[1]
        ts.set_day(str(start.date()))
        ldr_df = ts.df[start:end]
        if len(ldr_df) == 0:
            continue
        jl_150 = StxJL(ts, 1.50)
        jl_050 = StxJL(ts, 0.50)
        ts.set_day(str(ts.df.index[ts.start + 20].date()))
        start_w = jl_150.initjl()
        start_w = jl_050.initjl()
        ixx = start_w if start_w >= jl_150.w else -1
        lt_trend = 0
        while ixx != -1:
            ixx = ts.next_day()
            crt_dt = ts.current_date()
            if ixx == -1:
                break
            jl_150.nextjl()
            jl_050.nextjl()
            pivs = jl_150.get_pivots_in_days(400)
            risk_mgmt(ts, open_trades, trades)
            lt_trend = get_trend(ts, jl_150, pivs)
            cp, stp, sl, min_call, max_put = get_trade_type(
                ts, jl_150, jl_050, pivs, lt_trend, min_call, max_put)
            if cp is not None:
                print('{0:s} {1:s} {2:s}'.format(crt_dt, cp, stp))
                trades_by_exp = calls if cp == 'call' else puts
                stk_trades_by_exp = trades_by_exp.get(ts.stk)
                if stk_trades_by_exp is None:
                    stk_trades_by_exp = {}
                    trades_by_exp[stk] = stk_trades_by_exp
                exp = str(stxcal.next_expiry(crt_dt, 15))
                open_trades_by_exp = stk_trades_by_exp.get(exp)
                if open_trades_by_exp is not None:
                    print('{0:s}: Trade already there {1:s}'.
                          format(crt_dt, to_short_string(open_trades_by_exp)))
                else:
                    trd = trade(ts, cp, sl, jl_150.last_rec('rg'), stp)
                    if trd is not None:
                        open_trades.append(trd)
                        stk_trades_by_exp[exp] = trd
                        trades_by_exp[ts.stk] = stk_trades_by_exp
                        print('New Trade: {0:s}'.format(to_short_string(trd)))
            split_info = ts.splits.get(pd.Timestamp(crt_dt))
            if split_info is not None:
                next_bd = stxcal.next_busday(crt_dt)
                ratio = split_info[0]
                min_call = min_call * ratio
                max_put = max_put * ratio
                for trd in open_trades:
                    trd.strike = trd.strike * ratio
                    trd.opt_in.spot_px = trd.opt_in.spot_px * ratio
                    trd.opt_in.opt_px = trd.opt_in.opt_px * ratio
                    trd.num = trd.num / ratio
                    trd.avg_range = trd.avg_range * ratio
                    trd.opts, strike = get_trade_opts(
                        ts.stk, trd.exp, trd.cp, trd.strike, trd.avg_range,
                        next_bd, 0.1 / trd.avg_range)
    with open(fname, fmode, newline='') as fp:
        wrtr = csv.writer(fp, delimiter=',')
        if fmode == 'w':
            wrtr.writerow(['Stock', 'Date', 'Year', 'CP', 'Expiry', 'Strike',
                           'InSpot', 'InOpt', 'Spread', 'Contracts', 'Range',
                           'OutDate', 'OutSpot', 'OutOpt', 'P&L Pct', 'P&L',
                           'Success', 'GainPct', 'LossPct', 'Gain', 'Loss',
                           'Setup'])
        for trd in trades:
            wrtr.writerow(to_csv_list(trd))


stx = 'NFLX'
# stx = 'INTU,NFLX,VLO,TSLA,GIGM,NTES,ACAD,AMR,HOV,CSCO,INTC,CSIQ,FSLR,'\
#         'TASR,LUV,USB,GOOG,MSFT,GLD,SPY,YHOO,AAPL,AMZN,MA,TIE'
# stx   = 'FSLR,TASR,LUV,USB,GOOG,MSFT,GLD,SPY,YHOO,AAPL,AMZN,MA,TIE'
name = 'c3'
sd = '2001-01-01'
ed = '2016-12-31'
ixx = 0
slst = stx.split(',')
fname = '{0:s}_888.csv'.format(stx if len(slst) == 1 else name)
for stk in slst:
    print('{0:s}: '.format(stk))
    ts = StxTS(stk, sd, ed)
    tdf = StxTS.gen_tdf(sd, ed)
    ts.addleader(tdf)
    calls = {}
    puts = {}
    fmode = 'w' if ixx == 0 else 'a'
    ixx += 1
    analyze(ts, calls, puts, fname, fmode)
