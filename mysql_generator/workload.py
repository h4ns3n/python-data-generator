import random
import string

from . import ids, schema

ACCOUNTS_TABLE = "accounts"


def _random_description(k=10):
    return "".join(random.choices(string.ascii_letters + string.digits, k=k))


def _random_amount(lo=1.0, hi=1000.0):
    return round(random.uniform(lo, hi), 2)


def generate_seed_batch(batch_size, account_ids):
    return [
        (_random_amount(), _random_description(), random.choice(account_ids),
         random.choice(schema.TRANSACTION_TYPES), random.choice(schema.CURRENCIES))
        for _ in range(batch_size)
    ]


def bulk_insert_batch(conn, table_name, batch, changelog=None):
    insert_sql = (
        f"INSERT INTO {table_name} (amount, description, account_id, transaction_type, currency) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    with conn.cursor() as cur:
        cur.executemany(insert_sql, batch)
    conn.commit()
    if changelog:
        changelog.write("bulk_insert_batch", table_name, count=len(batch))


def _has_description(conn, table_name):
    # description is left driftable (unlike amount/id/account_id/status/deleted_at
    # — see schema.PROTECTED_TRANSACTION_COLUMNS), so DDL drift can drop/rename it
    # mid-run. Re-introspect rather than hardcoding it into every statement, so a
    # DDL event never breaks the DML layer (the concurrency.QuiesceGate ensures
    # this check and the statement it informs happen against a stable schema).
    return "description" in schema.get_columns(conn, table_name)


def insert_transaction(conn, table_name, account_id, changelog):
    amount = _random_amount()
    transaction_type = random.choice(schema.TRANSACTION_TYPES)
    currency = random.choice(schema.CURRENCIES)
    has_description = _has_description(conn, table_name)
    if has_description:
        sql = (
            f"INSERT INTO {table_name} (amount, description, account_id, transaction_type, currency) "
            "VALUES (%s, %s, %s, %s, %s)"
        )
        params = (amount, _random_description(), account_id, transaction_type, currency)
    else:
        sql = f"INSERT INTO {table_name} (amount, account_id, transaction_type, currency) VALUES (%s, %s, %s, %s)"
        params = (amount, account_id, transaction_type, currency)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        new_id = cur.lastrowid
    conn.commit()
    changelog.write("insert", table_name, id=new_id, amount=str(amount), account_id=account_id)
    return new_id


def update_transaction(conn, table_name, tracker, changelog, recent_bias=0.85):
    target_id = ids.pick_target_id(conn, table_name, tracker, recent_bias)
    if target_id is None:
        return None
    new_amount = _random_amount()
    if _has_description(conn, table_name):
        sql = f"UPDATE {table_name} SET amount=%s, description=%s WHERE id=%s AND status='active'"
        params = (new_amount, _random_description(), target_id)
    else:
        sql = f"UPDATE {table_name} SET amount=%s WHERE id=%s AND status='active'"
        params = (new_amount, target_id)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        affected = cur.rowcount
    conn.commit()
    if affected == 0:
        return None
    changelog.write("update", table_name, id=target_id, amount=str(new_amount))
    return target_id


def delete_transaction(conn, table_name, tracker, changelog, delete_mode="hard", recent_bias=0.85):
    target_id = ids.pick_target_id(conn, table_name, tracker, recent_bias)
    if target_id is None:
        return None

    if delete_mode == "soft":
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table_name} SET status='deleted', deleted_at=NOW() WHERE id=%s AND status='active'",
                (target_id,),
            )
            affected = cur.rowcount
        conn.commit()
        if affected == 0:
            return None
        tracker.discard(target_id)
        changelog.write("soft_delete", table_name, id=target_id)
        return target_id

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table_name} WHERE id=%s", (target_id,))
        affected = cur.rowcount
    conn.commit()
    if affected == 0:
        return None
    tracker.discard(target_id)
    changelog.write("delete", table_name, id=target_id)
    return target_id


def cascading_delete_account(conn, table_names, tracker_by_table, account_tracker, changelog):
    """
    Delete a parent account and let ON DELETE CASCADE remove its child rows.
    MySQL's binlog only reflects the client's DELETE FROM accounts statement
    explicitly — the cascaded child-row deletes are only visible as row events
    at the storage-engine level (this cluster runs binlog_row_image=FULL /
    ROW format specifically so those events exist at all). We can't ask MySQL
    afterward which children were removed, so we compute the expected set
    ourselves before issuing the delete and log it as ground truth.
    """
    account_id = ids.pick_target_id(conn, ACCOUNTS_TABLE, account_tracker, recent_bias=0.7)
    if account_id is None:
        return None

    with conn.cursor() as cur:
        cur.execute(f"SELECT balance FROM {ACCOUNTS_TABLE} WHERE id=%s", (account_id,))
        row = cur.fetchone()
    if row is None:
        return None
    (account_balance,) = row

    expected_children = {}
    with conn.cursor() as cur:
        for table_name in table_names:
            cur.execute(f"SELECT id FROM {table_name} WHERE account_id=%s", (account_id,))
            child_ids = [row[0] for row in cur.fetchall()]
            if child_ids:
                expected_children[table_name] = child_ids

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {ACCOUNTS_TABLE} WHERE id=%s", (account_id,))
        affected = cur.rowcount
    conn.commit()
    if affected == 0:
        return None

    account_tracker.discard(account_id)
    for table_name, child_ids in expected_children.items():
        for child_id in child_ids:
            tracker_by_table[table_name].discard(child_id)

    changelog.write(
        "cascade_delete", ACCOUNTS_TABLE,
        account_id=account_id, account_balance=str(account_balance), expected_children=expected_children,
    )
    return account_id


def transfer(conn, table_names, account_tracker, changelog):
    """
    Debit one account, credit another, and record a matching ledger row in
    each account's transactions table — all in a single commit. This is what
    makes the balance-conservation invariant checkable end-to-end: Snowflake-
    side verification can recompute balances from the transaction rows and
    confirm they match what actually landed.
    """
    account_id_from = ids.pick_target_id(conn, ACCOUNTS_TABLE, account_tracker, recent_bias=0.7)
    if account_id_from is None:
        return None

    account_id_to = None
    for _ in range(5):
        candidate = ids.pick_target_id(conn, ACCOUNTS_TABLE, account_tracker, recent_bias=0.7)
        if candidate is not None and candidate != account_id_from:
            account_id_to = candidate
            break
    if account_id_to is None:
        return None

    amount = _random_amount()
    currency = random.choice(schema.CURRENCIES)
    table_name = random.choice(table_names)
    has_description = _has_description(conn, table_name)
    if has_description:
        insert_sql = (
            f"INSERT INTO {table_name} (amount, description, account_id, transaction_type, currency, "
            "counterparty_account_id) VALUES (%s, %s, %s, %s, %s, %s)"
        )
    else:
        insert_sql = (
            f"INSERT INTO {table_name} (amount, account_id, transaction_type, currency, counterparty_account_id) "
            "VALUES (%s, %s, %s, %s, %s)"
        )

    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {ACCOUNTS_TABLE} SET balance = balance - %s WHERE id=%s AND balance >= %s",
            (amount, account_id_from, amount),
        )
        if cur.rowcount == 0:
            conn.rollback()
            return None

        cur.execute(f"UPDATE {ACCOUNTS_TABLE} SET balance = balance + %s WHERE id=%s", (amount, account_id_to))

        leg_out_params = (-amount, _random_description(), account_id_from, "transfer_out", currency, account_id_to) \
            if has_description else (-amount, account_id_from, "transfer_out", currency, account_id_to)
        cur.execute(insert_sql, leg_out_params)
        leg_out_id = cur.lastrowid

        leg_in_params = (amount, _random_description(), account_id_to, "transfer_in", currency, account_id_from) \
            if has_description else (amount, account_id_to, "transfer_in", currency, account_id_from)
        cur.execute(insert_sql, leg_in_params)
        leg_in_id = cur.lastrowid

    conn.commit()

    changelog.write(
        "transfer",
        table_name,
        from_account_id=account_id_from,
        to_account_id=account_id_to,
        amount=str(amount),
        leg_out_id=leg_out_id,
        leg_in_id=leg_in_id,
    )
    return {"from": account_id_from, "to": account_id_to, "amount": amount}
