#!/usr/bin/env python3
"""
Insert-only bulk population of the wide transactions_4 table: large VARCHAR,
a BLOB carrying a 5-10KB sample JSON payload, and a spread of numeric/date/
enum/JSON column types. Meant to be re-run manually (e.g. once an hour) to
push ~1M rows per run into an already-seeded cluster (accounts must exist).
No update/delete/DDL-drift support — this table is insert-only by design.
"""
import argparse
import concurrent.futures
import time

import pymysql

from mysql_generator import schema, workload
from mysql_generator.changelog import Changelog
from mysql_generator.config import load_db_config
from mysql_generator.guard import assert_writable
from mysql_generator.pool import SimpleConnectionPool


def insert_wide_records_concurrent(db_params, num_records, account_ids, batch_size, num_workers, changelog):
    try:
        from tqdm import tqdm
        progress = tqdm(total=(num_records + batch_size - 1) // batch_size, desc="Seeding transactions_4 (wide)")
    except ImportError:
        progress = None

    pool = SimpleConnectionPool(db_params, num_workers)

    def work(batch, max_attempts=3):
        conn = pool.getconn()
        inserted = 0
        for attempt in range(1, max_attempts + 1):
            try:
                workload.bulk_insert_wide_batch(conn, batch, changelog)
                inserted = len(batch)
                break
            except Exception as e:
                print(f"Error inserting wide batch (attempt {attempt}/{max_attempts}): {e}")
                # The connection may be dead (e.g. a silently-dropped socket)
                # rather than just having a rolled-back transaction, so
                # replace it instead of reusing it for the retry. Note: if the
                # error was actually a lost response after the write already
                # committed server-side, this retry (and the row count it
                # reports as "inserted") can't distinguish that from a clean
                # retry — a real duplicate is possible, not just a possible
                # under-count.
                try:
                    conn.close()
                except Exception:
                    pass
                conn = pymysql.connect(**db_params)
                if attempt == max_attempts:
                    print(f"Giving up on wide batch after {max_attempts} attempts; {len(batch)} rows lost.")
        pool.putconn(conn)
        if progress:
            progress.update(1)
        return inserted

    # Each batch's rows carry a ~16KB VARCHAR + a 5-10KB BLOB apiece, so
    # holding every batch's data in memory at once (as the original
    # transactions_{n} seeder does) scales badly here — observed several GB
    # of RSS growth over a 1M-row/2000-batch run. Cap how far batch
    # generation can run ahead of the workers actually consuming it.
    max_in_flight = num_workers * 4
    total_inserted = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        pending = set()
        remaining = num_records
        while remaining > 0:
            this_batch = min(batch_size, remaining)
            batch = workload.generate_seed_batch_wide(this_batch, account_ids)
            pending.add(executor.submit(work, batch))
            remaining -= this_batch
            if len(pending) >= max_in_flight:
                done, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                for f in done:
                    total_inserted += f.result()
        for future in concurrent.futures.as_completed(pending):
            total_inserted += future.result()
    if progress:
        progress.close()
    pool.closeall()
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=str, default="python-datasource.json")
    parser.add_argument("--alias", type=str, default="CDC_POC_MYSQL")
    parser.add_argument("--records", type=int, default=1_000_000,
                         help="Rows to insert into transactions_4 this run.")
    parser.add_argument("--batch-size", type=int, default=500, dest="batch_size",
                         help="Rows per executemany batch. Kept low relative to the other "
                              "tables' seeding since each wide row is ~20-26KB (large VARCHAR + BLOB).")
    parser.add_argument("--num-workers", type=int, default=16, dest="num_workers",
                         help="Concurrent connections. 16 was the empirically validated sweet spot on this "
                              "cluster's connection path (~234 rows/sec aggregate; 24 gave no further gain, "
                              "32 regressed) — see HANDOFF.md.")
    parser.add_argument("--changelog", type=str, default="generator_changelog.jsonl")
    args = parser.parse_args()

    db_params = load_db_config(args.config, args.alias)
    conn = pymysql.connect(**db_params)
    assert_writable(conn)

    schema.create_wide_transactions_table(conn)

    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {workload.ACCOUNTS_TABLE}")
        account_ids = [row[0] for row in cur.fetchall()]
    conn.close()

    if not account_ids:
        raise SystemExit("No accounts found — seed the base schema (./cdcgen.py seed ...) before populating transactions_4.")

    changelog = Changelog(args.changelog)

    print(f"Inserting {args.records} rows into {schema.WIDE_TRANSACTIONS_TABLE} "
          f"(batch_size={args.batch_size}, num_workers={args.num_workers})...")
    start = time.time()
    total_inserted = insert_wide_records_concurrent(
        db_params, args.records, account_ids, args.batch_size, args.num_workers, changelog
    )
    elapsed = time.time() - start
    rate = total_inserted / elapsed if elapsed > 0 else 0
    if total_inserted < args.records:
        print(f"WARNING: requested {args.records} rows but only confirmed {total_inserted} inserted "
              f"({args.records - total_inserted} rows given up on after retries — see 'Giving up on wide batch' "
              f"lines above).")
    print(f"Done: confirmed {total_inserted}/{args.records} rows inserted into {schema.WIDE_TRANSACTIONS_TABLE} "
          f"in {elapsed:.1f}s ({rate:.1f} rows/sec).")


if __name__ == "__main__":
    main()
