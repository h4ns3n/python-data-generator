import pymysql
import pytest

from mysql_generator import schema, workload
from mysql_generator.changelog import Changelog, read_entries
from mysql_generator.concurrency import QuiesceGate
from mysql_generator.guard import assert_writable
from mysql_generator.ids import seed_from_table


@pytest.fixture
def conn(db_params):
    connection = pymysql.connect(**db_params)
    yield connection
    connection.close()


def test_assert_writable_passes_on_real_mysql(conn):
    assert_writable(conn)  # a fresh testcontainers instance is never read-only


def test_schema_creation_and_cascade_delete(conn, tmp_path):
    schema.create_accounts_table(conn)
    schema.create_transactions_table(conn, "transactions_1")

    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    schema.seed_accounts(conn, num_accounts=2, changelog=changelog)

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM accounts ORDER BY id")
        account_ids = [row[0] for row in cur.fetchall()]
    assert len(account_ids) == 2

    tracker = seed_from_table(conn, "transactions_1")
    for _ in range(5):
        new_id = workload.insert_transaction(conn, "transactions_1", account_ids[0], changelog)
        tracker.record(new_id)

    account_tracker = seed_from_table(conn, "accounts")
    result = workload.cascading_delete_account(
        conn, ["transactions_1"], {"transactions_1": tracker}, account_tracker, changelog,
    )
    assert result == account_ids[0]

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM transactions_1 WHERE account_id=%s", (account_ids[0],))
        (remaining,) = cur.fetchone()
    assert remaining == 0  # ON DELETE CASCADE actually removed the child rows

    changelog.close()
    entries = read_entries(str(tmp_path / "cl.jsonl"))
    cascade_entries = [e for e in entries if e["op"] == "cascade_delete"]
    assert len(cascade_entries) == 1
    assert len(cascade_entries[0]["expected_children"]["transactions_1"]) == 5


def test_transfer_conserves_total_balance(conn, tmp_path):
    schema.create_accounts_table(conn)
    schema.create_transactions_table(conn, "transactions_1")
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    schema.seed_accounts(conn, num_accounts=2, changelog=changelog)

    with conn.cursor() as cur:
        cur.execute("SELECT SUM(balance) FROM accounts")
        (before,) = cur.fetchone()

    account_tracker = seed_from_table(conn, "accounts")
    result = workload.transfer(conn, ["transactions_1"], account_tracker, changelog)
    assert result is not None

    with conn.cursor() as cur:
        cur.execute("SELECT SUM(balance) FROM accounts")
        (after,) = cur.fetchone()
    assert before == after  # transfer moves balance, never creates or destroys it

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM transactions_1")
        (row_count,) = cur.fetchone()
    assert row_count == 2  # one ledger row per leg
    changelog.close()


def test_soft_delete_never_cascades(conn, tmp_path):
    schema.create_accounts_table(conn)
    schema.create_transactions_table(conn, "transactions_1")
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    schema.seed_accounts(conn, num_accounts=1, changelog=changelog)

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM accounts LIMIT 1")
        (account_id,) = cur.fetchone()

    tracker = seed_from_table(conn, "transactions_1")
    new_id = workload.insert_transaction(conn, "transactions_1", account_id, changelog)
    tracker.record(new_id)

    workload.delete_transaction(conn, "transactions_1", tracker, changelog, delete_mode="soft")

    with conn.cursor() as cur:
        cur.execute("SELECT status, deleted_at FROM transactions_1 WHERE id=%s", (new_id,))
        status, deleted_at = cur.fetchone()
    assert status == "deleted"
    assert deleted_at is not None
    changelog.close()


def test_ddl_drift_add_and_drop_column(conn, tmp_path):
    schema.create_accounts_table(conn)
    schema.create_transactions_table(conn, "transactions_1")
    changelog = Changelog(str(tmp_path / "cl.jsonl"))

    op_name = schema.ddl_add_column(conn, "transactions_1", changelog)
    assert op_name is True
    columns_after_add = schema.get_columns(conn, "transactions_1")
    added = [c for c in columns_after_add if c.startswith("extra_")]
    assert len(added) == 1

    dropped_ok = schema.ddl_drop_column(conn, "transactions_1", changelog)
    assert dropped_ok is True
    columns_after_drop = schema.get_columns(conn, "transactions_1")
    # either the new extra_ column or "description" was dropped — protected
    # columns must never be touched
    for protected in schema.PROTECTED_TRANSACTION_COLUMNS:
        assert protected in columns_after_drop
    changelog.close()


def test_ddl_drift_quiesce_gate_with_real_connections(conn, db_params, tmp_path):
    schema.create_accounts_table(conn)
    schema.create_transactions_table(conn, "transactions_1")
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    schema.seed_accounts(conn, num_accounts=1, changelog=changelog)

    gate = QuiesceGate()
    dml_conn = pymysql.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts LIMIT 1")
            (account_id,) = cur.fetchone()

        with gate.dml_guard():
            workload.insert_transaction(dml_conn, "transactions_1", account_id, changelog)

        with gate.ddl_section():
            op_name = schema.run_random_ddl_drift(conn, "transactions_1", changelog)
            assert op_name is not None
    finally:
        dml_conn.close()
        changelog.close()
