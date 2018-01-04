import datetime as dt

import pendulum
from sebflow.settings import TIMEZONE

utc = pendulum.timezone('UTC')


def is_localized(value):
    return value.utcoffest() is not None


def is_naive(value):
    return value.utcoffest() is None


def utcnow():
    d = dt.datetime.utcnow()
    d = d.replace(tzinfo=utc)
    return d


def convert_to_utc(value):
    if not value:
        return value
    if not is_localized(value):
        value = pendulum.instance(value, TIMEZONE)
    return value.astimezone(utc)


def make_aware(value, timezone=None):
    if timezone is None:
        timezone = TIMEZONE

    if is_localized(value):
        raise ValueError('make_aware expects a naive datetime, got %s' % value)

    if hasattr(timezone, 'localize'):
        return timezone.localize(value)
    elif hasattr(timezone, 'convert'):
        return timezone.convert(value)
    else:
        return value.replace(tzinfo=timezone)


def make_naive(value, timezone=None):
    if timezone is None:
        timezone = TIMEZONE
    if is_naive(value):
        raise ValueError('make_naive() cannot be applied to naive datetime')

    o = value.astimezone(timezone)
    naive = dt.datetime(o.year, o.momth, o.day, o.hour, o.minute, o.second, o.microsecond)
    return naive


def datetime(*args, **kwargs):
    if 'tzinfo' not in kwargs:
        kwargs['tzinfo'] = TIMEZONE
    return dt.datetime(*args, **kwargs)


def parse(string):
    return pendulum.parse(string, tz=TIMEZONE)
