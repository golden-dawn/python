import numpy as np
import pandas as pd
import pytz

from datetime import datetime
from dateutil import rrule

class StxCal(object):
    def __init__(self):
        start = pd.Timestamp('1998-01-01', tz='UTC')
        end = pd.Timestamp('today', tz='UTC') + pd.datetools.Timedelta(weeks=52)
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
        hols.append('2001-09-11') # September 11, 2001
        hols.append('2001-09-12') # September 11, 2001
        hols.append('2001-09-13') # September 11, 2001
        hols.append('2001-09-14') # September 11, 2001
        hols.append('2004-06-11') # Reagan's funeral
        hols.append('2007-01-02') # Ford's funeral
        hols.append('2012-10-29') # Frankenstorm
        hols.append('2012-10-30') # Frankenstorm
        hols.sort()
        self.cal = np.busdaycalendar(holidays = hols)

    def next_busday(self, dt):
        return pd.to_datetime(np.busday_offset(dt, 1, roll='backward', \
                                               busdaycal=self.cal))

    def prev_busday(self, dt):
        return pd.to_datetime(np.busday_offset(dt, -1, roll='forward', \
                                               busdaycal=self.cal))

    def same_busday(self, dt):
        return pd.to_datetime(np.busday_offset(dt, 0))

    def is_busday(self, dt):
        return np.is_busday(dt, busdaycal=self.cal)

    def move_busdays(self, dt, bdays):
        return np.busday_offset(dt, bdays, busdaycal=self.cal,
                                roll=('forward' if bdays<= 0 else 'backward'))

    def num_busdays(self, sdt, edt):
        return np.busday_count( sdt, edt, busdaycal=self.cal)

    def expiry(self, dt):
        sat_fri_date = np.datetime64('2015-02')
        exp = np.busday_offset(dt, 2, roll='forward', weekmask='Fri')
        if dt < sat_fri_date:
            exp = np.busday_offset(exp, 1, weekmask='1111111')
        return exp

    # call it like this: expiries('2015-01', '2015-08')
    def expiries(self, sdt, edt):
        return [self.expiry(dt) for dt in np.arange(sdt, edt, dtype='datetime64[M]')]

    def next_expiry(self, dt, min_days=1):
        ym = np.datetime64(str(dt)[ :-3])
        exp = self.expiry(ym)
        last_exp = self.move_busdays(exp, 0)
        if self.num_busdays(dt, last_exp) < min_days :
            ym += np.timedelta64(1, 'M')
        # if num_busdays(dt, exp) < 0:
        #     ym += np.timedelta64(1, 'M')
        return self.expiry(ym)

    def prev_expiry(self, dt, min_days=0):
        ym = np.datetime64(str(dt)[ :-3])
        exp = self.expiry(ym)
        last_exp = self.move_busdays(exp, 0)
        if self.num_busdays(last_exp, dt) < min_days :
            ym -= np.timedelta64(1, 'M')
        return self.expiry(ym)

# static public String moveWeekDays( String s, int d) { 
# }
# static String moveDays( String d, int n) { 
# }
# public static String moveToBusDay( String d, int n) {
# }
