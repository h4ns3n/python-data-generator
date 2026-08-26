from mysql_generator import schema
from tests.doubles import FakeChangelog, FakeConn, FakeCursor


def test_get_columns_queries_information_schema():
    cursor = FakeCursor(fetchall_return=[("id",), ("amount",), ("description",)])
    conn = FakeConn(cursor)
    assert schema.get_columns(conn, "transactions_1") == ["id", "amount", "description"]


def test_get_driftable_columns_excludes_protected():
    cursor = FakeCursor(fetchall_return=[
        ("id",), ("account_id",), ("status",), ("deleted_at",), ("amount",),
        ("transaction_ts",), ("description",), ("extra_ab12",),
    ])
    conn = FakeConn(cursor)
    assert schema.get_driftable_columns(conn, "transactions_1") == ["description", "extra_ab12"]


def test_ddl_add_column_executes_alter_and_logs():
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    result = schema.ddl_add_column(conn, "transactions_1", changelog)
    assert result is True
    assert conn.committed is True
    sql, _ = cursor.executed[0]
    assert sql.startswith("ALTER TABLE transactions_1 ADD COLUMN extra_")
    assert changelog.entries[0]["ddl_type"] == "add_column"


def test_ddl_drop_column_returns_false_with_no_candidates():
    cursor = FakeCursor(fetchall_return=[("id",), ("account_id",)])  # all protected
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    assert schema.ddl_drop_column(conn, "transactions_1", changelog) is False
    assert changelog.entries == []


def test_ddl_drop_column_executes_when_candidate_exists():
    cursor = FakeCursor(fetchall_return=[("id",), ("description",)])
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    assert schema.ddl_drop_column(conn, "transactions_1", changelog) is True
    sql, _ = cursor.executed[-1]
    assert sql == "ALTER TABLE transactions_1 DROP COLUMN description"


def test_ddl_add_index_uses_key_length_for_text_column():
    cursor = FakeCursor(fetchall_return=[("description", "text")])
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    assert schema.ddl_add_index(conn, "transactions_1", changelog) is True
    sql, _ = cursor.executed[-1]
    assert "description(50)" in sql


def test_ddl_add_index_no_key_length_for_non_text_column():
    cursor = FakeCursor(fetchall_return=[("extra_num", "int")])
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    assert schema.ddl_add_index(conn, "transactions_1", changelog) is True
    sql, _ = cursor.executed[-1]
    assert "extra_num(50)" not in sql
    assert "(extra_num)" in sql


def test_ddl_drop_index_only_targets_idx_prefixed_indexes():
    cursor = FakeCursor(fetchall_return=[("idx_description_ab12",)])
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    assert schema.ddl_drop_index(conn, "transactions_1", changelog) is True
    sql, _ = cursor.executed[-1]
    assert sql == "ALTER TABLE transactions_1 DROP INDEX idx_description_ab12"


def test_ddl_drop_index_returns_false_when_none_created_by_tool():
    cursor = FakeCursor(fetchall_return=[])
    conn = FakeConn(cursor)
    changelog = FakeChangelog()
    assert schema.ddl_drop_index(conn, "transactions_1", changelog) is False


def test_run_random_ddl_drift_tries_until_one_applies(monkeypatch):
    calls = []

    def always_fails(conn, table_name, changelog):
        calls.append("fail")
        return False

    def succeeds(conn, table_name, changelog):
        calls.append("succeed")
        return True

    monkeypatch.setattr(schema, "DDL_OPERATIONS", {
        "op_a": always_fails,
        "op_b": always_fails,
        "op_c": succeeds,
    })
    result = schema.run_random_ddl_drift(FakeConn(FakeCursor()), "transactions_1", FakeChangelog())
    assert result == "op_c"
    assert calls[-1] == "succeed"


def test_run_random_ddl_drift_returns_none_when_nothing_applies(monkeypatch):
    monkeypatch.setattr(schema, "DDL_OPERATIONS", {"op_a": lambda c, t, cl: False})
    result = schema.run_random_ddl_drift(FakeConn(FakeCursor()), "transactions_1", FakeChangelog())
    assert result is None


def test_protected_columns_are_never_drift_targets():
    assert schema.PROTECTED_TRANSACTION_COLUMNS == {
        "id", "account_id", "status", "deleted_at", "amount", "transaction_ts",
    }
