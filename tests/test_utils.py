import os
import sys
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
from app import parse_timestamp


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


def test_parse_timestamp_timezone_no_colon():
    ts = parse_timestamp("2025-07-29T14:30:45+0530")
    assert isinstance(ts, datetime.datetime)
    assert ts.year == 2025
    assert ts.month == 7
    assert ts.day == 29
