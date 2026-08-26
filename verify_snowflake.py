#!/usr/bin/env python3
"""
Compare a generator changelog (ground truth) against actual Snowflake state,
to check whether Artie/Openflow correctly captured DDL/DML mutations —
including the tricky ones: cascading deletes and transfer balance
conservation. See snowflake_verify/reconcile.py for the replay logic.

NOTE: the Snowflake database/schema/table names below are placeholders. The
actual Openflow ingestion mapping for this PoC isn't known yet as of writing
this (the Openflow track "not yet started" per the CDC PoC handoff) — pass
--sf-database/--sf-schema once it lands, and adjust snowflake_verify/snow_cli.py's
column-name assumptions if the landing schema doesn't mirror MySQL 1:1.
"""
import argparse
import sys

from mysql_generator.changelog import read_entries
from snowflake_verify.reconcile import compare_accounts, compare_table, reconstruct_expected_state
from snowflake_verify.snow_cli import SnowflakeQueryError, fetch_account_rows, fetch_transaction_rows


def main():
    parser = argparse.ArgumentParser(description="Verify Snowflake state against the generator's changelog")
    parser.add_argument("--changelog", required=True, help="Path to the generator's JSONL changelog.")
    parser.add_argument("--sf-database", required=True, help="Snowflake database the data landed in.")
    parser.add_argument("--sf-schema", required=True, help="Snowflake schema the data landed in.")
    parser.add_argument("--sf-connection", default="snowflake-cli", help="snow CLI connection name.")
    parser.add_argument("--tables", required=True,
                         help="Comma-separated transactions_{n} table names to verify (e.g. transactions_1,transactions_2).")
    args = parser.parse_args()

    entries = read_entries(args.changelog)
    expected = reconstruct_expected_state(entries)

    all_mismatches = []

    try:
        actual_accounts = fetch_account_rows(args.sf_database, args.sf_schema, connection=args.sf_connection)
    except SnowflakeQueryError as e:
        print(f"Error querying Snowflake: {e}")
        sys.exit(1)
    account_mismatches = compare_accounts(expected["accounts"], actual_accounts)
    if account_mismatches:
        print(f"accounts: {len(account_mismatches)} mismatch(es)")
        for m in account_mismatches:
            print(f"  - {m}")
    all_mismatches.extend(account_mismatches)

    for table_name in args.tables.split(","):
        table_name = table_name.strip()
        expected_rows = expected["tables"].get(table_name, {})
        try:
            actual_rows = fetch_transaction_rows(args.sf_database, args.sf_schema, table_name, connection=args.sf_connection)
        except SnowflakeQueryError as e:
            print(f"Error querying Snowflake for {table_name}: {e}")
            sys.exit(1)
        mismatches = compare_table(expected_rows, actual_rows)
        if mismatches:
            print(f"{table_name}: {len(mismatches)} mismatch(es)")
            for m in mismatches:
                print(f"  - {m}")
        all_mismatches.extend(mismatches)

    if all_mismatches:
        print(f"\nFAILED: {len(all_mismatches)} total mismatch(es).")
        sys.exit(1)

    print("OK: Snowflake state matches the changelog for all checked accounts/tables.")


if __name__ == "__main__":
    main()
