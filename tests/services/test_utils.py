from services.utils import _decode_event


def test_decode_event_converts_bytes_and_ints():
    raw = {b"user_id": b"123", b"symbol": b"ABC", b"qty": b"5", b"note": b"hello"}
    assert _decode_event(raw) == {
        "user_id": 123,
        "symbol": "ABC",
        "qty": 5,
        "note": "hello",
    }


def test_decode_event_leaves_non_int_strings():
    raw = {b"type": b"market"}
    assert _decode_event(raw) == {"type": "market"}
