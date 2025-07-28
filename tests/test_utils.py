import datetime
from app import parse_timestamp


def test_parse_timestamp_iso():
    ts = parse_timestamp("2025-07-25T06:52:12Z")
    assert isinstance(ts, datetime.datetime)
    assert ts.year == 2025
    assert ts.month == 7
    assert ts.day == 25


def test_parse_timestamp_numeric_str():
    ts = parse_timestamp("1693526400")
    assert isinstance(ts, datetime.datetime)
    assert ts.year == 2023
    assert ts.month == 9
    assert ts.day == 1


def test_parse_timestamp_ms_numeric_str():
    ts = parse_timestamp("1693526400000")
    assert isinstance(ts, datetime.datetime)
    assert ts.year == 2023
    assert ts.month == 9
    assert ts.day == 1
