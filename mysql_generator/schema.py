import random
import string

CUSTOMERS_TABLE = "customers"
ACCOUNTS_TABLE = "accounts"

# Columns that DDL drift must never DROP/MODIFY/RENAME: id/account_id are the
# PK/FK the cascading-delete relationship depends on, status/deleted_at are
# load-bearing for soft-delete, amount/transaction_ts are core row metadata
# referenced by name throughout workload.py, and currency/transaction_type/
# counterparty_account_id are the fintech-ledger fields transfer legs and any
# downstream analysis depend on by name. Drift is interesting and safe on
# description and anything a prior ADD COLUMN event introduced; the DML layer
# re-introspects columns before writing so drift on those never breaks it.
PROTECTED_TRANSACTION_COLUMNS = {
    "id", "account_id", "status", "deleted_at", "amount", "transaction_ts",
    "currency", "transaction_type", "counterparty_account_id",
}

DDL_COLUMN_TYPES = ["VARCHAR(50)", "INT", "DECIMAL(10,2)", "DATETIME", "TEXT"]

ACCOUNT_TYPES = ["checking", "savings", "wallet"]
CURRENCIES = ["USD", "EUR", "GBP", "ZAR"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "payment", "fee"]
KYC_STATUSES = ["verified", "pending", "rejected"]


def _random_suffix(n=6):
    return "".join(random.choices(string.ascii_lowercase, k=n))


def create_customers_table(conn):
    """
    The top-level parent. Static reference data: deliberately frozen from
    DDL drift and never deleted by this PoC, same rationale as accounts
    below — accounts.customer_id references it, so it stays structurally
    stable for the whole run.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CUSTOMERS_TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        full_name VARCHAR(100) NOT NULL,
        email VARCHAR(150) NOT NULL,
        kyc_status VARCHAR(20) NOT NULL DEFAULT 'verified',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def create_accounts_table(conn):
    """
    Shared parent for every transactions_{n} table. Deliberately frozen from
    DDL drift (see mysql_generator.schema.PROTECTED_TRANSACTION_COLUMNS
    docstring above and run_random_ddl_drift's table selection in the CLI):
    every transactions_{n} table references it, and balance is load-bearing
    for the transfer conservation invariant, so it stays structurally stable
    for the whole run.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {ACCOUNTS_TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        account_type VARCHAR(20) NOT NULL DEFAULT 'checking',
        currency CHAR(3) NOT NULL DEFAULT 'USD',
        balance DECIMAL(14,2) NOT NULL DEFAULT 0,
        status VARCHAR(20) NOT NULL DEFAULT 'active',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES {CUSTOMERS_TABLE}(id) ON DELETE CASCADE
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def create_transactions_table(conn, table_name):
    """
    Child table. account_id FK cascades on delete of the parent account (fixed
    at creation, never part of the DDL-drift pool — the cascade relationship
    is the thing under test, not itself a variable). status/deleted_at support
    soft-delete mode without relying on a drift event to add them.
    currency/transaction_type/counterparty_account_id are the fintech-ledger
    fields transfer() writes on each leg; see PROTECTED_TRANSACTION_COLUMNS.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        transaction_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        transaction_type VARCHAR(20) NOT NULL DEFAULT 'payment',
        amount DECIMAL(14,2) NOT NULL,
        currency CHAR(3) NOT NULL DEFAULT 'USD',
        counterparty_account_id BIGINT NULL,
        description TEXT,
        account_id BIGINT NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'active',
        deleted_at DATETIME NULL,
        FOREIGN KEY (account_id) REFERENCES {ACCOUNTS_TABLE}(id) ON DELETE CASCADE
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def drop_transactions_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()


def drop_accounts_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {ACCOUNTS_TABLE};")
    conn.commit()


def drop_customers_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {CUSTOMERS_TABLE};")
    conn.commit()


def reset_schema(conn, table_names):
    """Drop children first (FK dependency), then accounts, then customers, then recreate all."""
    for table_name in table_names:
        drop_transactions_table(conn, table_name)
    drop_accounts_table(conn)
    drop_customers_table(conn)
    create_customers_table(conn)
    create_accounts_table(conn)
    for table_name in table_names:
        create_transactions_table(conn, table_name)


def teardown_all(conn):
    """
    --drop_database: remove every table this tool owns (transactions_*,
    accounts, and customers), not the database object itself. Keeping the
    database avoids disturbing grants/config tied to it if the config ever
    points somewhere shared; dropping every owned table gets the same
    practical "make it disappear" outcome for this ephemeral PoC schema.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
              AND (table_name IN (%s, %s) OR table_name LIKE 'transactions\\_%%')
            """,
            (ACCOUNTS_TABLE, CUSTOMERS_TABLE),
        )
        tables = [row[0] for row in cur.fetchall()]

    children = [t for t in tables if t not in (ACCOUNTS_TABLE, CUSTOMERS_TABLE)]
    with conn.cursor() as cur:
        for table_name in children:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        if ACCOUNTS_TABLE in tables:
            cur.execute(f"DROP TABLE IF EXISTS {ACCOUNTS_TABLE};")
        if CUSTOMERS_TABLE in tables:
            cur.execute(f"DROP TABLE IF EXISTS {CUSTOMERS_TABLE};")
    conn.commit()
    return tables


def seed_customers(conn, num_customers, changelog=None):
    """
    Ensure at least num_customers rows exist in the shared customers table.
    Low-volume like accounts, so log one changelog entry per customer rather
    than an aggregate count (see seed_accounts below for the same rationale).
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {CUSTOMERS_TABLE}")
        (count,) = cur.fetchone()
    if count >= num_customers:
        return 0

    to_create = num_customers - count
    rows = [
        (f"Customer {count + i}", f"customer_{count + i}@example.test", random.choice(KYC_STATUSES))
        for i in range(to_create)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {CUSTOMERS_TABLE} (full_name, email, kyc_status) VALUES (%s, %s, %s)", rows
        )
        first_id = cur.lastrowid
    conn.commit()
    if changelog:
        for i, (full_name, email, kyc_status) in enumerate(rows):
            changelog.write(
                "insert", CUSTOMERS_TABLE, id=first_id + i, full_name=full_name, email=email, kyc_status=kyc_status
            )
    return to_create


def seed_accounts(conn, num_accounts, customer_ids, changelog=None):
    """
    Ensure at least num_accounts rows exist in the shared accounts table.
    Accounts are low-volume (dozens/hundreds, not millions like transaction
    rows), so unlike bulk transaction-row seeding, log one changelog entry per
    account rather than an aggregate count: the balance-conservation
    invariant (Q17/Q23) can only be verified from the changelog alone (Q16)
    if it records each account's actual starting balance.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {ACCOUNTS_TABLE}")
        (count,) = cur.fetchone()
    if count >= num_accounts:
        return 0

    to_create = num_accounts - count
    rows = [
        (
            random.choice(customer_ids),
            random.choice(ACCOUNT_TYPES),
            random.choice(CURRENCIES),
            round(random.uniform(100.0, 100000.0), 2),
        )
        for _ in range(to_create)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {ACCOUNTS_TABLE} (customer_id, account_type, currency, balance) VALUES (%s, %s, %s, %s)",
            rows,
        )
        first_id = cur.lastrowid
    conn.commit()
    if changelog:
        for i, (customer_id, account_type, currency, balance) in enumerate(rows):
            changelog.write(
                "insert", ACCOUNTS_TABLE, id=first_id + i, customer_id=customer_id,
                account_type=account_type, currency=currency, balance=str(balance),
            )
    return to_create


def get_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def get_driftable_columns(conn, table_name):
    return [c for c in get_columns(conn, table_name) if c not in PROTECTED_TRANSACTION_COLUMNS]


def get_driftable_columns_with_types(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        rows = cur.fetchall()
    return [(name, data_type) for name, data_type in rows if name not in PROTECTED_TRANSACTION_COLUMNS]


def _execute_ddl(conn, table_name, changelog, ddl_type, stmt, **details):
    with conn.cursor() as cur:
        cur.execute(stmt)
    conn.commit()
    changelog.write("ddl", table_name, ddl_type=ddl_type, statement=stmt, **details)
    return True


def ddl_add_column(conn, table_name, changelog):
    col_name = f"extra_{_random_suffix()}"
    col_type = random.choice(DDL_COLUMN_TYPES)
    stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} NULL"
    return _execute_ddl(conn, table_name, changelog, "add_column", stmt, column=col_name, column_type=col_type)


def ddl_drop_column(conn, table_name, changelog):
    candidates = get_driftable_columns(conn, table_name)
    if not candidates:
        return False
    col_name = random.choice(candidates)
    stmt = f"ALTER TABLE {table_name} DROP COLUMN {col_name}"
    return _execute_ddl(conn, table_name, changelog, "drop_column", stmt, column=col_name)


def ddl_modify_column(conn, table_name, changelog):
    candidates = get_driftable_columns(conn, table_name)
    if not candidates:
        return False
    col_name = random.choice(candidates)
    new_type = random.choice(DDL_COLUMN_TYPES)
    stmt = f"ALTER TABLE {table_name} MODIFY COLUMN {col_name} {new_type} NULL"
    return _execute_ddl(conn, table_name, changelog, "modify_column", stmt, column=col_name, new_type=new_type)


def ddl_rename_column(conn, table_name, changelog):
    candidates = get_driftable_columns(conn, table_name)
    if not candidates:
        return False
    old_name = random.choice(candidates)
    new_name = f"{old_name}_r{_random_suffix(4)}"
    stmt = f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"
    return _execute_ddl(conn, table_name, changelog, "rename_column", stmt, old_name=old_name, new_name=new_name)


TEXT_BLOB_TYPES = {"text", "tinytext", "mediumtext", "longtext", "blob", "tinyblob", "mediumblob", "longblob"}


def ddl_add_index(conn, table_name, changelog):
    candidates = get_driftable_columns_with_types(conn, table_name)
    if not candidates:
        return False
    col_name, data_type = random.choice(candidates)
    idx_name = f"idx_{col_name}_{_random_suffix(4)}"
    # TEXT/BLOB columns can't be indexed outright in MySQL — they need an
    # explicit key-length prefix.
    key_expr = f"{col_name}(50)" if data_type in TEXT_BLOB_TYPES else col_name
    stmt = f"ALTER TABLE {table_name} ADD INDEX {idx_name} ({key_expr})"
    return _execute_ddl(conn, table_name, changelog, "add_index", stmt, index_name=idx_name, column=col_name)


def _pick_droppable_index(conn, table_name):
    # Only ever drop indexes this tool itself created (the "idx_" naming
    # convention from ddl_add_index) — never PRIMARY, never the FK-required
    # index on account_id.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT index_name FROM information_schema.statistics
            WHERE table_schema = DATABASE() AND table_name = %s AND index_name LIKE 'idx\\_%%'
            """,
            (table_name,),
        )
        rows = [row[0] for row in cur.fetchall()]
    return random.choice(rows) if rows else None


def ddl_drop_index(conn, table_name, changelog):
    idx_name = _pick_droppable_index(conn, table_name)
    if not idx_name:
        return False
    stmt = f"ALTER TABLE {table_name} DROP INDEX {idx_name}"
    return _execute_ddl(conn, table_name, changelog, "drop_index", stmt, index_name=idx_name)


DDL_OPERATIONS = {
    "add_column": ddl_add_column,
    "drop_column": ddl_drop_column,
    "modify_column": ddl_modify_column,
    "rename_column": ddl_rename_column,
    "add_index": ddl_add_index,
    "drop_index": ddl_drop_index,
}


def run_random_ddl_drift(conn, table_name, changelog, enabled_ops=None):
    """
    Try DDL operations in random order until one actually applies (some, like
    drop_column, are no-ops when there's no eligible target yet). Returns the
    operation name that ran, or None if nothing was applicable.
    """
    ops = list(enabled_ops or DDL_OPERATIONS.keys())
    random.shuffle(ops)
    for op_name in ops:
        if DDL_OPERATIONS[op_name](conn, table_name, changelog):
            return op_name
    return None
