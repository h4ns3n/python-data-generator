from mysql_generator import workload
from mysql_generator.ids import IdTracker
from tests.doubles import FakeChangelog, FakeConn, FakeCursor


def test_generate_seed_batch_shape():
    batch = workload.generate_seed_batch(10, account_ids=[1, 2, 3])
    assert len(batch) == 10
    for amount, description, account_id in batch:
        assert 1.0 <= amount <= 1000.0
        assert len(description) == 10
        assert account_id in [1, 2, 3]


def test_insert_transaction_includes_description_when_column_present():
    # get_columns query result includes "description"
    cursor = FakeCursor(fetchall_return=[("id",), ("amount",), ("description",), ("account_id",)], lastrowid=7)
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    new_id = workload.insert_transaction(conn, "transactions_1", account_id=3, changelog=changelog)
    assert new_id == 7
    assert conn.committed is True
    sql, params = cursor.executed[-1]
    assert "description" in sql
    assert len(params) == 3
    assert changelog.entries[0]["op"] == "insert"
    assert changelog.entries[0]["account_id"] == 3


def test_insert_transaction_omits_description_when_column_dropped():
    cursor = FakeCursor(fetchall_return=[("id",), ("amount",), ("account_id",)], lastrowid=8)
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    workload.insert_transaction(conn, "transactions_1", account_id=3, changelog=changelog)
    sql, params = cursor.executed[-1]
    assert "description" not in sql
    assert len(params) == 2


def test_update_transaction_returns_none_when_no_target_row(monkeypatch):
    monkeypatch.setattr(workload.ids, "pick_target_id", lambda *a, **k: None)
    conn = FakeConn(FakeCursor())
    changelog = FakeChangelog()
    result = workload.update_transaction(conn, "transactions_1", IdTracker(), changelog)
    assert result is None
    assert changelog.entries == []


def test_update_transaction_returns_none_when_row_already_gone(monkeypatch):
    monkeypatch.setattr(workload.ids, "pick_target_id", lambda *a, **k: 5)
    cursor = FakeCursor(fetchall_return=[("id",), ("amount",)])
    cursor.rowcount = 0  # UPDATE matched nothing (row deleted concurrently)
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    result = workload.update_transaction(conn, "transactions_1", IdTracker(), changelog)
    assert result is None
    assert changelog.entries == []


def test_delete_transaction_hard_discards_from_tracker(monkeypatch):
    monkeypatch.setattr(workload.ids, "pick_target_id", lambda *a, **k: 5)
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    tracker = IdTracker()
    tracker.record(5)
    result = workload.delete_transaction(conn, "transactions_1", tracker, changelog, delete_mode="hard")
    assert result == 5
    assert len(tracker) == 0
    assert changelog.entries[0]["op"] == "delete"
    sql, _ = cursor.executed[-1]
    assert sql.startswith("DELETE FROM transactions_1")


def test_delete_transaction_soft_updates_status(monkeypatch):
    monkeypatch.setattr(workload.ids, "pick_target_id", lambda *a, **k: 5)
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    tracker = IdTracker()
    tracker.record(5)
    result = workload.delete_transaction(conn, "transactions_1", tracker, changelog, delete_mode="soft")
    assert result == 5
    assert len(tracker) == 0
    assert changelog.entries[0]["op"] == "soft_delete"
    sql, _ = cursor.executed[-1]
    assert "UPDATE transactions_1 SET status='deleted'" in sql


def test_cascading_delete_account_precomputes_expected_children(monkeypatch):
    monkeypatch.setattr(workload.ids, "pick_target_id", lambda conn, table, tracker, **k: 9 if table == "accounts" else None)

    # cascading_delete_account opens three separate `with conn.cursor()` blocks
    # (balance lookup, children lookup, the delete itself); FakeConn hands back
    # the same cursor every time, so fetchall needs a queue: first call serves
    # transactions_1's children, second serves transactions_2's.
    cursor = FakeCursor(fetchone_return=(500.00,), fetchall_sequence=[[(1,), (2,)], [(3,)]])
    conn = FakeConn(cursor)

    changelog = FakeChangelog()
    trackers = {"transactions_1": IdTracker(), "transactions_2": IdTracker()}
    for tid in (1, 2):
        trackers["transactions_1"].record(tid)
    trackers["transactions_2"].record(3)
    account_tracker = IdTracker()
    account_tracker.record(9)

    result = workload.cascading_delete_account(
        conn, ["transactions_1", "transactions_2"], trackers, account_tracker, changelog,
    )
    assert result == 9
    assert len(trackers["transactions_1"]) == 0
    assert len(trackers["transactions_2"]) == 0
    assert len(account_tracker) == 0

    entry = changelog.entries[0]
    assert entry["op"] == "cascade_delete"
    assert entry["account_id"] == 9
    assert entry["account_balance"] == "500.0"
    assert entry["expected_children"] == {"transactions_1": [1, 2], "transactions_2": [3]}
