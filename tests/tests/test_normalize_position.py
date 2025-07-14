import pytest
from helpers import normalize_position


def test_normalize_dhan():
    raw = {
        'tradingSymbol': 'ABC',
        'buyQty': '5',
        'sellQty': '2',
        'buyAvg': '10.5',
        'sellAvg': '10',
        'lastTradedPrice': '11',
        'unrealizedProfit': '2.5'
    }
    pos = normalize_position(raw, 'dhan')
    assert pos == {
        'tradingSymbol': 'ABC',
        'buyQty': 5,
        'sellQty': 2,
        'netQty': 3,
        'buyAvg': 10.5,
        'sellAvg': 10.0,
        'ltp': 11.0,
        'profitAndLoss': 2.5,
    }


def test_normalize_finvasia():
    raw = {
        'tsym': 'XYZ-EQ',
        'buyqty': '1',
        'sellqty': '0',
        'netQty': '1',
        'avgprc': '100',
        'ltp': '102',
        'urmtm': '2'
    }
    pos = normalize_position(raw, 'finvasia')
    assert pos['tradingSymbol'] == 'XYZ-EQ'
    assert pos['buyQty'] == 1
    assert pos['sellQty'] == 0
    assert pos['netQty'] == 1
    assert pos['buyAvg'] == 100.0
    assert pos['sellAvg'] == 100.0
    assert pos['ltp'] == 102.0
    assert pos['profitAndLoss'] == 2.0
