import numpy as np
import pandas as pd
import pytz
import sys

from datetime import datetime
from dateutil import rrule

this = sys.modules[__name__]
this.cal = None


def get_cal():
    if this.cal is None:
        start = pd.Timestamp('1998-01-01', tz='UTC')
        end = pd.Timestamp('today', tz='UTC') + \
            pd.datetools.Timedelta(weeks=52)
        print('Initializing calendar between {0:s} and {1:s}'.
              format(str(start.date()), str(end.date())))
        non_trading_rules = []
        new_years = rrule.rrule(
            rrule.MONTHLY,
            byyearday=1,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(new_years)

        new_years_sunday = rrule.rrule(
            rrule.MONTHLY,
            byyearday=2,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(new_years_sunday)

        mlk_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=1,
            byweekday=rrule.MO,
            bysetpos=3,
            cache=True,
            dtstart=datetime(1998, 1, 1, tzinfo=pytz.utc),
            until=end
        )
        non_trading_rules.append(mlk_day)

        presidents_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=2,
            byweekday=rrule.MO,
            bysetpos=3,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(presidents_day)

        good_friday = rrule.rrule(
            rrule.DAILY,
            byeaster=-2,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(good_friday)

        memorial_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=5,
            byweekday=rrule.MO,
            bysetpos=-1,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(memorial_day)

        july_4th = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=4,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(july_4th)

        july_4th_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=5,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(july_4th_sunday)

        july_4th_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=7,
            bymonthday=3,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(july_4th_saturday)

        labor_day = rrule.rrule(
            rrule.MONTHLY,
            bymonth=9,
            byweekday=rrule.MO,
            bysetpos=1,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(labor_day)

        thanksgiving = rrule.rrule(
            rrule.MONTHLY,
            bymonth=11,
            byweekday=rrule.TH,
            bysetpos=4,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(thanksgiving)

        christmas = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=25,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(christmas)

        christmas_sunday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=26,
            byweekday=rrule.MO,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(christmas_sunday)

        # If Christmas is a Saturday then 24th, a Friday is observed.
        christmas_saturday = rrule.rrule(
            rrule.MONTHLY,
            bymonth=12,
            bymonthday=24,
            byweekday=rrule.FR,
            cache=True,
            dtstart=start,
            until=end
        )
        non_trading_rules.append(christmas_saturday)

        non_trading_ruleset = rrule.rruleset()
        for rule in non_trading_rules:
            non_trading_ruleset.rrule(rule)

        non_trading_days = non_trading_ruleset.between(start, end, inc=True)
        hols = []
        for non_trading_day in non_trading_days:
            if non_trading_day.weekday() < 5:
                hols.append(str(non_trading_day.date()))
        hols.append('2001-09-11')  # September 11, 2001
        hols.append('2001-09-12')  # September 11, 2001
        hols.append('2001-09-13')  # September 11, 2001
        hols.append('2001-09-14')  # September 11, 2001
        hols.append('2004-06-11')  # Reagan's funeral
        hols.append('2007-01-02')  # Ford's funeral
        hols.append('2012-10-29')  # Frankenstorm
        hols.append('2012-10-30')  # Frankenstorm
        hols.sort()
        this.cal = np.busdaycalendar(holidays=hols)
    return this.cal


def next_busday(dt):
    return str(pd.to_datetime(np.busday_offset(dt, 1, roll='backward',
                                               busdaycal=get_cal())).date())


def prev_busday(dt):
    return str(pd.to_datetime(np.busday_offset(dt, -1, roll='forward',
                                               busdaycal=get_cal())).date())


def is_busday(dt):
    return np.is_busday(dt, busdaycal=get_cal())


# Move backwards (if bdays < 0) or forwards (if bdays > 0) bdays
# business days.  If bdays is 0, then will not move if dt is already a
# business day, otherwise, will move to the previous business day. The
# parameter bdays can be 0 when we want to calculate the last business
# day when an option is traded, by moving 0 business days from the
# expiry, which would fall on a Saturday before Feb 2015, and on a
# Friday afterwards.
def move_busdays(dt, bdays):
    return str(np.busday_offset(dt, bdays, busdaycal=get_cal(),
                                roll='forward' if bdays < 0 else 'backward'))


def num_busdays(sdt, edt):
    return np.busday_count(sdt, edt, busdaycal=get_cal())


def expiry(dt):
    sat_fri_date = np.datetime64('2015-02')
    exp = np.busday_offset(dt, 2, roll='forward', weekmask='Fri')
    if np.datetime64(dt) < sat_fri_date:
        exp = np.busday_offset(exp, 1, weekmask='1111111')
    return str(exp)


# call it like this: expiries('2015-01', '2015-08')
def expiries(sdt, edt):
    return [expiry(dt) for dt in np.arange(sdt, edt, dtype='datetime64[M]')]


def next_expiry(dt, min_days=1):
    ym = np.datetime64(str(dt)[:-3])
    last_exp = move_busdays(expiry(ym), 0)
    while num_busdays(dt, last_exp) < min_days:
        ym += np.timedelta64(1, 'M')
        last_exp = move_busdays(expiry(ym), 0)
    return expiry(ym)


def prev_expiry(dt, min_days=0):
    ym = np.datetime64(str(dt)[:-3])
    last_exp = move_busdays(expiry(ym), 0)
    while num_busdays(last_exp, dt) < min_days:
        ym -= np.timedelta64(1, 'M')
        last_exp = move_busdays(expiry(ym), 0)
    return expiry(ym)


def long_expiries(starting_from=None):
    s_date = datetime.strftime(datetime.now(), '%Y-%m-%d')\
             if starting_from is None else starting_from
    e_date = next_expiry(s_date, 8 * 252 // 12)
    epoch = datetime.utcfromtimestamp(0)
    return [int((datetime.strptime(x, '%Y-%m-%d') - epoch).total_seconds())
            for x in expiries(s_date[:-3], e_date[:-3])]
