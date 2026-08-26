"""
Reconstructs expected MySQL end-state purely from the generator's ground-truth
changelog (mysql_generator.changelog) — never by re-querying MySQL, per the
verification design: deleted rows can't be re-queried from MySQL after the
fact anyway, so the changelog is the only consistent source of truth for
inserts, updates, and deletes alike.

Bulk-seed rows (op="bulk_insert_batch") are intentionally NOT reconstructed
row-by-row — only individually-logged operations (insert/update/delete/
soft_delete/cascade_delete/transfer, all emitted during --simulate) are
tracked here. That mirrors the changelog itself: batch-seeded rows were never
logged individually (see mysql_generator.changelog.Changelog docstring), so
there's nothing to reconstruct them from.
"""
from decimal import Decimal


def reconstruct_expected_state(entries):
    """
    Replay changelog entries in order and return:
      {
        "accounts": {account_id: {"balance": Decimal, "deleted": bool}},
        "tables": {table_name: {row_id: {"amount": Decimal, "status": str, "deleted": bool}}},
      }
    """
    accounts = {}
    tables = {}

    def table_state(table_name):
        return tables.setdefault(table_name, {})

    for entry in entries:
        op = entry["op"]
        table = entry["table"]

        if op == "insert" and table == "accounts":
            accounts[entry["id"]] = {"balance": Decimal(entry["balance"]), "deleted": False}

        elif op == "insert":
            table_state(table)[entry["id"]] = {
                "amount": Decimal(entry["amount"]), "status": "active", "deleted": False,
            }

        elif op == "update":
            row = table_state(table).get(entry["id"])
            if row is not None:
                row["amount"] = Decimal(entry["amount"])

        elif op == "soft_delete":
            row = table_state(table).get(entry["id"])
            if row is not None:
                row["status"] = "deleted"

        elif op == "delete":
            row = table_state(table).get(entry["id"])
            if row is not None:
                row["deleted"] = True

        elif op == "cascade_delete":
            account = accounts.get(entry["account_id"])
            if account is not None:
                account["deleted"] = True
            for child_table, child_ids in entry["expected_children"].items():
                for child_id in child_ids:
                    row = table_state(child_table).get(child_id)
                    if row is not None:
                        row["deleted"] = True

        elif op == "transfer":
            amount = Decimal(entry["amount"])
            from_account = accounts.get(entry["from_account_id"])
            to_account = accounts.get(entry["to_account_id"])
            if from_account is not None:
                from_account["balance"] -= amount
            if to_account is not None:
                to_account["balance"] += amount
            table_state(table)[entry["leg_out_id"]] = {"amount": -amount, "status": "active", "deleted": False}
            table_state(table)[entry["leg_in_id"]] = {"amount": amount, "status": "active", "deleted": False}

        # op in ("ddl", "bulk_insert_batch"): not row-level state, nothing to replay

    return {"accounts": accounts, "tables": tables}


def compare_table(expected_rows, actual_rows):
    """
    expected_rows: {row_id: {"amount": Decimal, "status": str, "deleted": bool}}
    actual_rows: {row_id: {"amount": Decimal, "status": str}} as observed in Snowflake
    Returns a list of human-readable mismatch descriptions (empty if consistent).
    """
    mismatches = []
    for row_id, expected in expected_rows.items():
        if expected["deleted"]:
            if row_id in actual_rows:
                mismatches.append(f"row {row_id}: expected deleted (hard or cascaded), but present in Snowflake")
            continue
        if row_id not in actual_rows:
            mismatches.append(f"row {row_id}: expected present, missing in Snowflake")
            continue
        actual = actual_rows[row_id]
        if expected["amount"] != actual.get("amount"):
            mismatches.append(f"row {row_id}: amount mismatch — expected {expected['amount']}, got {actual.get('amount')}")
        if expected["status"] != actual.get("status"):
            mismatches.append(f"row {row_id}: status mismatch — expected {expected['status']}, got {actual.get('status')}")
    return mismatches


def compare_accounts(expected_accounts, actual_accounts):
    mismatches = []
    for account_id, expected in expected_accounts.items():
        if expected["deleted"]:
            if account_id in actual_accounts:
                mismatches.append(f"account {account_id}: expected deleted, but present in Snowflake")
            continue
        if account_id not in actual_accounts:
            mismatches.append(f"account {account_id}: expected present, missing in Snowflake")
            continue
        actual_balance = actual_accounts[account_id].get("balance")
        if expected["balance"] != actual_balance:
            mismatches.append(
                f"account {account_id}: balance mismatch — expected {expected['balance']}, got {actual_balance}"
            )
    return mismatches
