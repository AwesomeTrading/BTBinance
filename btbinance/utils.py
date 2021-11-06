import pytz
from datetime import datetime, timedelta, time
from backtrader import TimeFrame


def bar_starttime(timeframe,
                  compression,
                  dt=None,
                  ago=0,
                  sessionstart=time(hour=0, minute=0, second=0)):
    '''
    This method will return the start of the period based on current
    time (or provided time).
    '''
    if dt is None:
        dt = datetime.now(tz=pytz.UTC)
    if timeframe == TimeFrame.Seconds:
        dt = dt.replace(second=(dt.second // compression) * compression,
                        microsecond=0)
        if ago:
            dt = dt - timedelta(seconds=compression * ago)
    elif timeframe == TimeFrame.Minutes:
        # get start of day
        dtstart = bar_starttime(timeframe=TimeFrame.Days,
                                compression=1,
                                dt=dt,
                                sessionstart=sessionstart)
        # diff start of day with current time to get seconds since start of day
        dtdiff = dt - dtstart
        minutes = ((dtdiff.seconds // 60) // compression) * compression
        dt = dtstart + timedelta(minutes=minutes)

        if ago:
            dt = dt - timedelta(minutes=compression * ago)
    elif timeframe == TimeFrame.Days:
        if dt.hour < sessionstart.hour:
            dt = dt - timedelta(days=1)
        if ago:
            dt = dt - timedelta(days=ago)
        dt = dt.replace(hour=sessionstart.hour,
                        minute=sessionstart.minute,
                        second=sessionstart.second,
                        microsecond=sessionstart.microsecond)
    elif timeframe == TimeFrame.Weeks:
        if dt.weekday() != 6:
            # sunday is start of week at 5pm new york
            dt = dt - timedelta(days=dt.weekday() + 1)
        if ago:
            dt = dt - timedelta(days=ago * 7)
        dt = dt.replace(hour=sessionstart.hour,
                        minute=sessionstart.minute,
                        second=sessionstart.second,
                        microsecond=sessionstart.microsecond)
    elif timeframe == TimeFrame.Months:
        if ago:
            dt = dt - timedelta(days=(min(28 + dt.day, 31)))
        # last day of month
        last_day_of_month = dt.replace(day=28) + timedelta(days=4)
        last_day_of_month = last_day_of_month - timedelta(
            days=last_day_of_month.day)
        last_day_of_month = last_day_of_month.day
        # start of month (1 at 0, 22 last day of prev month)
        if dt.day < last_day_of_month:
            dt = dt - timedelta(days=dt.day)
        dt = dt.replace(hour=sessionstart.hour,
                        minute=sessionstart.minute,
                        second=sessionstart.second,
                        microsecond=sessionstart.microsecond)
    return dt


###
_PERIODS = {
    (TimeFrame.Ticks, 1): "TICK",
    (TimeFrame.Minutes, 1): "M1",
    (TimeFrame.Minutes, 5): "M5",
    (TimeFrame.Minutes, 15): "M15",
    (TimeFrame.Minutes, 30): "M30",
    (TimeFrame.Minutes, 60): "H1",
    (TimeFrame.Minutes, 120): "H2",
    (TimeFrame.Minutes, 180): "H3",
    (TimeFrame.Minutes, 240): "H4",
    (TimeFrame.Minutes, 360): "H6",
    (TimeFrame.Minutes, 480): "H8",
    (TimeFrame.Minutes, 720): "H12",
    (TimeFrame.Days, 1): "D1",
    (TimeFrame.Weeks, 1): "W1",
    (TimeFrame.Months, 1): "MN1",
}


def period_name(timeframe: TimeFrame, compression: int):
    return _PERIODS.get(
        (timeframe, compression),
        TimeFrame.Names[timeframe],
    )


def _val(obj, keys, default=None):
    if type(keys) == str:
        if keys in obj:
            return obj[keys]
    elif type(keys) == list:
        for key in keys:
            if key in obj:
                return obj[key]
            elif '.' in key:
                subkeys = key.split('.')
                subobj = obj
                found = True
                for subkey in subkeys:
                    if subkey not in subobj:
                        found = False
                        break
                    subobj = subobj[subkey]
                if found:
                    return subobj
    return default
