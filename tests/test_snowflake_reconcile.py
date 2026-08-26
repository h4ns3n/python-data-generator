from decimal import Decimal

from snowflake_verify.reconcile import compare_accounts, compare_table, reconstruct_expected_state


def test_reconstruct_simple_insert_update_delete():
    entries = [
        {"op": "insert", "table": "transactions_1", "id": 1, "amount": "10.00"},
        {"op": "update", "table": "transactions_1", "id": 1, "amount": "20.00"},
        {"op": "insert", "table": "transactions_1", "id": 2, "amount": "5.00"},
        {"op": "delete", "table": "transactions_1", "id": 2},
    ]
    state = reconstruct_expected_state(entries)
    rows = state["tables"]["transactions_1"]
    assert rows[1] == {"amount": Decimal("20.00"), "status": "active", "deleted": False}
    assert rows[2]["deleted"] is True


def test_reconstruct_soft_delete_sets_status_not_deleted_flag():
    entries = [
        {"op": "insert", "table": "transactions_1", "id": 1, "amount": "10.00"},
        {"op": "soft_delete", "table": "transactions_1", "id": 1},
    ]
    state = reconstruct_expected_state(entries)
    row = state["tables"]["transactions_1"][1]
    assert row["status"] == "deleted"
    assert row["deleted"] is False  # soft delete never hard-removes the row


def test_reconstruct_accounts_and_transfer_conserves_balance():
    entries = [
        {"op": "insert", "table": "accounts", "id": 1, "balance": "100.00"},
        {"op": "insert", "table": "accounts", "id": 2, "balance": "50.00"},
        {
            "op": "transfer", "table": "transactions_1",
            "from_account_id": 1, "to_account_id": 2, "amount": "30.00",
            "leg_out_id": 101, "leg_in_id": 102,
        },
    ]
    state = reconstruct_expected_state(entries)
    assert state["accounts"][1]["balance"] == Decimal("70.00")
    assert state["accounts"][2]["balance"] == Decimal("80.00")
    total = state["accounts"][1]["balance"] + state["accounts"][2]["balance"]
    assert total == Decimal("150.00")  # conserved
    rows = state["tables"]["transactions_1"]
    assert rows[101]["amount"] == Decimal("-30.00")
    assert rows[102]["amount"] == Decimal("30.00")


def test_reconstruct_cascade_delete_marks_account_and_all_children_deleted():
    entries = [
        {"op": "insert", "table": "accounts", "id": 9, "balance": "500.00"},
        {"op": "insert", "table": "transactions_1", "id": 1, "amount": "10.00"},
        {"op": "insert", "table": "transactions_2", "id": 2, "amount": "20.00"},
        {
            "op": "cascade_delete", "table": "accounts", "account_id": 9, "account_balance": "500.00",
            "expected_children": {"transactions_1": [1], "transactions_2": [2]},
        },
    ]
    state = reconstruct_expected_state(entries)
    assert state["accounts"][9]["deleted"] is True
    assert state["tables"]["transactions_1"][1]["deleted"] is True
    assert state["tables"]["transactions_2"][2]["deleted"] is True


def test_bulk_insert_batch_and_ddl_entries_do_not_create_row_state():
    entries = [
        {"op": "bulk_insert_batch", "table": "transactions_1", "count": 1000},
        {"op": "ddl", "table": "transactions_1", "ddl_type": "add_column", "column": "extra_x"},
    ]
    state = reconstruct_expected_state(entries)
    assert state["tables"] == {}
    assert state["accounts"] == {}


def test_compare_table_flags_missing_and_extra_and_value_mismatches():
    expected = {
        1: {"amount": Decimal("10.00"), "status": "active", "deleted": False},
        2: {"amount": Decimal("5.00"), "status": "active", "deleted": True},
        3: {"amount": Decimal("1.00"), "status": "active", "deleted": False},
    }
    actual = {
        1: {"amount": Decimal("999.00"), "status": "active"},  # wrong amount
        2: {"amount": Decimal("5.00"), "status": "active"},    # should have been deleted
        # row 3 missing entirely
    }
    mismatches = compare_table(expected, actual)
    joined = "\n".join(mismatches)
    assert "row 1: amount mismatch" in joined
    assert "row 2: expected deleted" in joined
    assert "row 3: expected present, missing" in joined


def test_compare_table_no_mismatches_when_consistent():
    expected = {1: {"amount": Decimal("10.00"), "status": "active", "deleted": False}}
    actual = {1: {"amount": Decimal("10.00"), "status": "active"}}
    assert compare_table(expected, actual) == []


def test_compare_accounts_flags_balance_mismatch():
    expected = {1: {"balance": Decimal("70.00"), "deleted": False}}
    actual = {1: {"balance": Decimal("999.00")}}
    mismatches = compare_accounts(expected, actual)
    assert len(mismatches) == 1
    assert "balance mismatch" in mismatches[0]
