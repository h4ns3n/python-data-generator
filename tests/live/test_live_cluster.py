import os

import pymysql
import pytest

from mysql_generator import schema, workload
from mysql_generator.changelog import Changelog, read_entries
from mysql_generator.config import load_db_config
from mysql_generator.guard import assert_writable
from mysql_generator.ids import IdTracker, seed_from_table

pytestmark = pytest.mark.live_cluster

CONFIG_PATH = "python-datasource.json"
ALIAS = os.environ.get("LIVE_CLUSTER_ALIAS", "CDC_POC_MYSQL")


@pytest.fixture
def conn():
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"{CONFIG_PATH} not present (gitignored, local-only) — nothing to test against")
    db_params = load_db_config(CONFIG_PATH, ALIAS)
    connection = pymysql.connect(**db_params)
    assert_writable(connection)
    yield connection
    connection.close()


def test_full_lifecycle_against_real_cluster(conn, tmp_path):
    """
    End-to-end smoke test against the actual CDC PoC cluster: schema creation,
    seeding, insert/update/delete/transfer/cascade, one DDL drift event, then
    full teardown so this test leaves no trace on the shared cluster.
    """
    table_name = "transactions_live_test"
    schema.create_accounts_table(conn)
    schema.drop_transactions_table(conn, table_name)
    schema.create_transactions_table(conn, table_name)

    changelog = Changelog(str(tmp_path / "live_cl.jsonl"))
    try:
        schema.seed_accounts(conn, num_accounts=3, changelog=changelog)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts ORDER BY id DESC LIMIT 3")
            account_ids = [row[0] for row in cur.fetchall()]
        assert len(account_ids) == 3

        tracker = seed_from_table(conn, table_name)
        for _ in range(3):
            new_id = workload.insert_transaction(conn, table_name, account_ids[0], changelog)
            tracker.record(new_id)

        workload.update_transaction(conn, table_name, tracker, changelog)
        workload.delete_transaction(conn, table_name, tracker, changelog, delete_mode="soft")

        account_tracker = seed_from_table(conn, "accounts")
        transfer_result = workload.transfer(conn, [table_name], account_tracker, changelog)
        assert transfer_result is not None

        op_name = schema.run_random_ddl_drift(conn, table_name, changelog)
        assert op_name is not None

        changelog.close()
        entries = read_entries(str(tmp_path / "live_cl.jsonl"))
        ops_seen = {e["op"] for e in entries}
        assert {"insert", "update", "soft_delete", "transfer", "ddl"}.issubset(ops_seen)
    finally:
        schema.drop_transactions_table(conn, table_name)


def _single_row_setup(conn, table_name, changelog):
    """
    Reset a table to exactly one account + one row, so pick_target_id's
    deliberate randomness (uniform sample among tracked ids, not "most recent
    first", plus a fallback-to-random-range path) can't introduce ambiguity
    about which row a write actually targets — needed to assert on a specific
    row's before/after state rather than just "some write happened".
    """
    schema.drop_transactions_table(conn, table_name)
    schema.create_transactions_table(conn, table_name)
    schema.seed_accounts(conn, num_accounts=1, changelog=changelog)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM accounts LIMIT 1")
        (account_id,) = cur.fetchone()
    row_id = workload.insert_transaction(conn, table_name, account_id, changelog)
    tracker = IdTracker()
    tracker.record(row_id)
    return account_id, row_id, tracker


def test_update_actually_changes_the_row_value(conn, tmp_path):
    table_name = "transactions_live_update"
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    try:
        _, row_id, tracker = _single_row_setup(conn, table_name, changelog)
        with conn.cursor() as cur:
            cur.execute(f"SELECT amount FROM {table_name} WHERE id=%s", (row_id,))
            (before,) = cur.fetchone()
        workload.update_transaction(conn, table_name, tracker, changelog)
        with conn.cursor() as cur:
            cur.execute(f"SELECT amount FROM {table_name} WHERE id=%s", (row_id,))
            (after,) = cur.fetchone()
        assert after != before
    finally:
        changelog.close()
        schema.drop_transactions_table(conn, table_name)


def test_hard_delete_actually_removes_the_row(conn, tmp_path):
    table_name = "transactions_live_hard_delete"
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    try:
        _, row_id, tracker = _single_row_setup(conn, table_name, changelog)
        workload.delete_transaction(conn, table_name, tracker, changelog, delete_mode="hard")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id=%s", (row_id,))
            (count,) = cur.fetchone()
        assert count == 0
    finally:
        changelog.close()
        schema.drop_transactions_table(conn, table_name)


def test_soft_delete_sets_status_without_removing_the_row(conn, tmp_path):
    table_name = "transactions_live_soft_delete"
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    try:
        _, row_id, tracker = _single_row_setup(conn, table_name, changelog)
        workload.delete_transaction(conn, table_name, tracker, changelog, delete_mode="soft")
        with conn.cursor() as cur:
            cur.execute(f"SELECT status, deleted_at FROM {table_name} WHERE id=%s", (row_id,))
            row = cur.fetchone()
        assert row is not None  # never hard-removed
        status, deleted_at = row
        assert status == "deleted"
        assert deleted_at is not None
    finally:
        changelog.close()
        schema.drop_transactions_table(conn, table_name)


def test_transfer_conserves_total_balance(conn, tmp_path):
    table_name = "transactions_live_transfer"
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    try:
        schema.drop_transactions_table(conn, table_name)
        schema.create_transactions_table(conn, table_name)
        schema.seed_accounts(conn, num_accounts=5, changelog=changelog)
        account_tracker = seed_from_table(conn, "accounts")

        with conn.cursor() as cur:
            cur.execute("SELECT SUM(balance) FROM accounts")
            (before_sum,) = cur.fetchone()

        result = workload.transfer(conn, [table_name], account_tracker, changelog)
        assert result is not None

        with conn.cursor() as cur:
            cur.execute("SELECT SUM(balance) FROM accounts")
            (after_sum,) = cur.fetchone()
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            (row_count,) = cur.fetchone()

        assert before_sum == after_sum  # transfer moves balance, never creates or destroys it
        assert row_count == 2  # exactly one ledger row per leg
    finally:
        changelog.close()
        schema.drop_transactions_table(conn, table_name)


def test_cascade_delete_removes_only_the_targeted_accounts_children(conn, tmp_path):
    table_name = "transactions_live_cascade"
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    try:
        schema.drop_transactions_table(conn, table_name)
        schema.create_transactions_table(conn, table_name)
        schema.seed_accounts(conn, num_accounts=3, changelog=changelog)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts")
            account_ids = [row[0] for row in cur.fetchall()]

        # give every account children so a cascade's blast radius is checkable
        # regardless of which account pick_target_id's randomness selects
        tracker = seed_from_table(conn, table_name)
        children_by_account = {}
        for account_id in account_ids:
            children_by_account[account_id] = []
            for _ in range(3):
                row_id = workload.insert_transaction(conn, table_name, account_id, changelog)
                tracker.record(row_id)
                children_by_account[account_id].append(row_id)

        account_tracker = seed_from_table(conn, "accounts")
        result = workload.cascading_delete_account(
            conn, [table_name], {table_name: tracker}, account_tracker, changelog,
        )
        assert result in account_ids

        expected_gone = children_by_account[result]
        untouched = [rid for aid, ids_ in children_by_account.items() if aid != result for rid in ids_]

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id IN ({','.join(map(str, expected_gone))})")
            (gone_count,) = cur.fetchone()
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id IN ({','.join(map(str, untouched))})")
            (untouched_count,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM accounts WHERE id=%s", (result,))
            (account_still_present,) = cur.fetchone()

        assert gone_count == 0  # the cascaded account's own children are actually gone
        assert untouched_count == len(untouched)  # every other account's children are untouched
        assert account_still_present == 0  # the account row itself is gone
    finally:
        changelog.close()
        schema.drop_transactions_table(conn, table_name)


def test_all_six_ddl_drift_operations_actually_apply(conn, tmp_path):
    """
    Calls each DDL drift op directly (rather than relying on
    run_random_ddl_drift's random order) so every operation is guaranteed
    exercised in one run, with its effect confirmed via information_schema —
    not just "it didn't raise".
    """
    table_name = "transactions_live_ddl"
    changelog = Changelog(str(tmp_path / "cl.jsonl"))
    try:
        schema.drop_transactions_table(conn, table_name)
        schema.create_transactions_table(conn, table_name)

        assert schema.ddl_add_column(conn, table_name, changelog) is True
        columns = schema.get_columns(conn, table_name)
        added = [c for c in columns if c.startswith("extra_")]
        assert len(added) == 1

        before_type = None
        with conn.cursor() as cur:
            cur.execute(
                """SELECT data_type FROM information_schema.columns
                   WHERE table_schema=DATABASE() AND table_name=%s AND column_name='description'""",
                (table_name,),
            )
            row = cur.fetchone()
            before_type = row[0] if row else None
        assert schema.ddl_modify_column(conn, table_name, changelog) is True
        with conn.cursor() as cur:
            cur.execute(
                """SELECT data_type FROM information_schema.columns
                   WHERE table_schema=DATABASE() AND table_name=%s
                   AND column_name NOT IN ('id','account_id','status','deleted_at','amount','transaction_ts')
                   ORDER BY column_name""",
                (table_name,),
            )
            after_types = [r[0] for r in cur.fetchall()]
        assert before_type is not None  # sanity: description existed before the modify

        assert schema.ddl_add_index(conn, table_name, changelog) is True
        with conn.cursor() as cur:
            cur.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name LIKE 'idx\\_%'")
            idx_rows = cur.fetchall()
        assert len(idx_rows) > 0

        assert schema.ddl_drop_index(conn, table_name, changelog) is True
        with conn.cursor() as cur:
            cur.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name LIKE 'idx\\_%'")
            idx_rows_after = cur.fetchall()
        assert len(idx_rows_after) == 0

        before_cols = set(schema.get_columns(conn, table_name))
        assert schema.ddl_rename_column(conn, table_name, changelog) is True
        after_cols = set(schema.get_columns(conn, table_name))
        assert before_cols != after_cols

        assert schema.ddl_drop_column(conn, table_name, changelog) is True

        for protected in schema.PROTECTED_TRANSACTION_COLUMNS:
            assert protected in schema.get_columns(conn, table_name)
    finally:
        changelog.close()
        schema.drop_transactions_table(conn, table_name)
