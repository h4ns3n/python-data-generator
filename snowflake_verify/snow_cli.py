"""
Thin wrapper around the `snow` CLI for read-only verification queries.

Shells out rather than using snowflake-connector-python directly: this
environment's Snowflake connection (`snowflake-cli`) uses EXTERNALBROWSER
(interactive SSO) auth, which the `snow` CLI already handles the session for
— adding a second Python-native Snowflake driver would mean duplicating that
auth setup for no real benefit. This mirrors the existing project convention
of using `snow sql -c <connection>` for account-usage/cost queries.
"""
import json
import subprocess
from decimal import Decimal


class SnowflakeQueryError(RuntimeError):
    pass


def run_query(query, connection="snowflake-cli"):
    """Run a query via `snow sql` and return a list of row dicts."""
    result = subprocess.run(
        ["snow", "sql", "-c", connection, "--format", "json", "--query", query],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise SnowflakeQueryError(f"snow sql failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def fetch_transaction_rows(database, schema, table, connection="snowflake-cli"):
    """
    Returns {row_id: {"amount": Decimal, "status": str}} for a transactions_{n}-shaped
    table. Assumes the Openflow/Artie landing table uses the same column names as the
    MySQL source (id, amount, status) — adjust here once the actual ingestion mapping
    for this PoC is confirmed (see HANDOFF.md; not yet known as of writing this).
    """
    rows = run_query(f'SELECT id, amount, status FROM {database}.{schema}.{table}', connection)
    return {
        int(row["ID"]): {"amount": Decimal(str(row["AMOUNT"])), "status": row["STATUS"]}
        for row in rows
    }


def fetch_account_rows(database, schema, table="accounts", connection="snowflake-cli"):
    """Returns {account_id: {"balance": Decimal}}."""
    rows = run_query(f'SELECT id, balance FROM {database}.{schema}.{table}', connection)
    return {int(row["ID"]): {"balance": Decimal(str(row["BALANCE"]))} for row in rows}
