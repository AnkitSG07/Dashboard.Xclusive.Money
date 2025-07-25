import datetime
from app import parse_timestamp


def test_parse_timestamp_iso():
    ts = parse_timestamp('2025-07-25T06:52:12Z')
    assert isinstance(ts, datetime.datetime)
    assert ts.year == 2025
    assert ts.month == 7
    assert ts.day == 25
