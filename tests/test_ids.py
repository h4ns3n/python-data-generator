import random
from unittest.mock import MagicMock

from mysql_generator.ids import IdTracker, pick_target_id, seed_from_table


def test_id_tracker_record_and_sample():
    tracker = IdTracker(maxlen=10)
    for i in range(5):
        tracker.record(i)
    assert len(tracker) == 5
    assert tracker.sample_recent() in range(5)


def test_id_tracker_discard():
    tracker = IdTracker(maxlen=10)
    tracker.record(1)
    tracker.record(2)
    tracker.discard(1)
    assert len(tracker) == 1
    assert tracker.sample_recent() == 2


def test_id_tracker_discard_missing_is_noop():
    tracker = IdTracker(maxlen=10)
    tracker.record(1)
    tracker.discard(999)
    assert len(tracker) == 1


def test_id_tracker_sample_recent_empty_returns_none():
    tracker = IdTracker(maxlen=10)
    assert tracker.sample_recent() is None


def test_id_tracker_respects_maxlen():
    tracker = IdTracker(maxlen=3)
    for i in range(10):
        tracker.record(i)
    assert len(tracker) == 3


def _make_conn(cursor_fetchone=None, cursor_fetchall=None):
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = cursor_fetchone
    cursor.fetchall.return_value = cursor_fetchall or []
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


def test_pick_target_id_uses_tracker_when_biased(monkeypatch):
    tracker = IdTracker(maxlen=10)
    tracker.record(42)
    conn, _ = _make_conn()
    monkeypatch.setattr(random, "random", lambda: 0.0)  # always take the biased path
    result = pick_target_id(conn, "transactions_1", tracker, recent_bias=0.85)
    assert result == 42


def test_pick_target_id_falls_back_to_range_query_when_tracker_empty():
    tracker = IdTracker(maxlen=10)
    conn, cursor = _make_conn(cursor_fetchone=(1, 100))
    cursor.fetchall.return_value = None
    cursor.fetchone.side_effect = [(1, 100), (5,)]
    result = pick_target_id(conn, "transactions_1", tracker, recent_bias=0.85)
    assert result == 5


def test_pick_target_id_returns_none_on_empty_table():
    tracker = IdTracker(maxlen=10)
    conn, cursor = _make_conn(cursor_fetchone=(None, None))
    result = pick_target_id(conn, "transactions_1", tracker, recent_bias=0.85)
    assert result is None


def test_seed_from_table_primes_tracker_in_ascending_order():
    conn, cursor = _make_conn()
    cursor.fetchall.return_value = [(3,), (2,), (1,)]  # ORDER BY id DESC
    tracker = seed_from_table(conn, "transactions_1", maxlen=10)
    assert len(tracker) == 3
