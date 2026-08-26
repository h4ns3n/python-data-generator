#!/usr/bin/env python3
import argparse
import concurrent.futures
import random
import threading
import time

import pymysql

from mysql_generator import ids, schema, workload
from mysql_generator.changelog import Changelog
from mysql_generator.concurrency import QuiesceGate
from mysql_generator.config import load_db_config
from mysql_generator.guard import assert_writable
from mysql_generator.pool import SimpleConnectionPool

DDL_OP_NAMES = list(schema.DDL_OPERATIONS.keys())


def insert_seed_records(conn, table_name, num_records, account_ids, batch_size, changelog):
    try:
        from tqdm import tqdm
        progress = tqdm(total=num_records, desc=f"Seeding {table_name}")
    except ImportError:
        progress = None

    remaining = num_records
    while remaining > 0:
        this_batch = min(batch_size, remaining)
        batch = workload.generate_seed_batch(this_batch, account_ids)
        workload.bulk_insert_batch(conn, table_name, batch, changelog)
        remaining -= this_batch
        if progress:
            progress.update(this_batch)
    if progress:
        progress.close()


def insert_seed_records_concurrent(db_params, table_name, num_records, account_ids, batch_size, num_workers, changelog):
    try:
        from tqdm import tqdm
        progress = tqdm(total=(num_records + batch_size - 1) // batch_size, desc=f"Seeding {table_name} (concurrent)")
    except ImportError:
        progress = None

    pool = SimpleConnectionPool(db_params, num_workers)

    def work(batch):
        conn = pool.getconn()
        try:
            workload.bulk_insert_batch(conn, table_name, batch, changelog)
        except Exception as e:
            print(f"Error inserting batch for {table_name}: {e}")
            conn.rollback()
        finally:
            pool.putconn(conn)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        remaining = num_records
        while remaining > 0:
            this_batch = min(batch_size, remaining)
            batch = workload.generate_seed_batch(this_batch, account_ids)
            futures.append(executor.submit(work, batch))
            remaining -= this_batch
            if progress:
                progress.update(1)
        for future in concurrent.futures.as_completed(futures):
            future.result()
    if progress:
        progress.close()
    pool.closeall()


def _pick_operation(insert_ratio, update_ratio, delete_ratio, transfer_ratio):
    weights = [("insert", insert_ratio), ("update", update_ratio), ("delete", delete_ratio), ("transfer", transfer_ratio)]
    total = sum(w for _, w in weights)
    r = random.uniform(0, total)
    upto = 0
    for name, w in weights:
        upto += w
        if r <= upto:
            return name
    return weights[-1][0]


def _dml_tick(conn, table_names, trackers, account_tracker, changelog, args):
    op = _pick_operation(args.insert_ratio, args.update_ratio, args.delete_ratio, args.transfer_ratio)
    try:
        if op == "insert":
            table_name = random.choice(table_names)
            account_id = ids.pick_target_id(conn, workload.ACCOUNTS_TABLE, account_tracker, recent_bias=0.7)
            if account_id is None:
                return
            new_id = workload.insert_transaction(conn, table_name, account_id, changelog)
            trackers[table_name].record(new_id)
        elif op == "update":
            table_name = random.choice(table_names)
            workload.update_transaction(conn, table_name, trackers[table_name], changelog, recent_bias=args.recent_bias)
        elif op == "delete":
            if args.delete_mode == "hard" and random.uniform(0, 100) < args.cascade_delete_ratio:
                workload.cascading_delete_account(conn, table_names, trackers, account_tracker, changelog)
            else:
                table_name = random.choice(table_names)
                workload.delete_transaction(
                    conn, table_name, trackers[table_name], changelog,
                    delete_mode=args.delete_mode, recent_bias=args.recent_bias,
                )
        elif op == "transfer":
            workload.transfer(conn, table_names, account_tracker, changelog)
    except pymysql.err.MySQLError as e:
        # Expected under concurrency: e.g. an account this worker just picked
        # for an insert/transfer got cascade-deleted by another worker between
        # the pick and the statement. Roll back and move on rather than kill
        # the worker thread over a benign race.
        conn.rollback()
        print(f"[DML] operation '{op}' failed (likely a benign concurrent-delete race): {e}")


def run_simulation(db_params, table_names, changelog, args):
    setup_conn = pymysql.connect(**db_params)
    trackers = {t: ids.seed_from_table(setup_conn, t) for t in table_names}
    account_tracker = ids.seed_from_table(setup_conn, workload.ACCOUNTS_TABLE)
    setup_conn.close()

    gate = QuiesceGate()
    stop_event = threading.Event()
    enabled_ddl_ops = args.ddl_ops.split(",") if args.ddl_ops else DDL_OP_NAMES
    next_ddl_fire = {
        t: time.time() + random.uniform(args.ddl_interval_seconds * 0.5, args.ddl_interval_seconds * 1.5)
        for t in table_names
    }

    def ddl_loop():
        conn = pymysql.connect(**db_params)
        try:
            while not stop_event.is_set():
                now = time.time()
                for table_name in table_names:
                    if now >= next_ddl_fire[table_name]:
                        try:
                            with gate.ddl_section():
                                op_name = schema.run_random_ddl_drift(conn, table_name, changelog, enabled_ddl_ops)
                                if op_name:
                                    print(f"[DDL] {table_name}: {op_name}")
                        except pymysql.err.MySQLError as e:
                            conn.rollback()
                            print(f"[DDL] drift on {table_name} failed: {e}")
                        next_ddl_fire[table_name] = time.time() + random.uniform(
                            args.ddl_interval_seconds * 0.5, args.ddl_interval_seconds * 1.5
                        )
                stop_event.wait(min(5, args.ddl_interval_seconds))
        finally:
            conn.close()

    def dml_worker():
        conn = pymysql.connect(**db_params)
        try:
            while not stop_event.is_set():
                with gate.dml_guard():
                    _dml_tick(conn, table_names, trackers, account_tracker, changelog, args)
                stop_event.wait(args.delay)
        finally:
            conn.close()

    threads = [threading.Thread(target=ddl_loop, daemon=True)]
    threads += [threading.Thread(target=dml_worker, daemon=True) for _ in range(max(1, args.num_workers))]

    duration_note = f" Auto-stopping after {args.duration}s." if args.duration else ""
    print("Starting production simulation. Press Ctrl+C to stop." + duration_note)
    for th in threads:
        th.start()

    try:
        if args.duration:
            stop_event.wait(args.duration)
            stop_event.set()
        else:
            while not stop_event.is_set():
                time.sleep(0.5)
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
        stop_event.set()

    for th in threads:
        th.join(timeout=10)


def main():
    parser = argparse.ArgumentParser(description="MySQL Sample Data Generator (DDL + DML, CDC PoC)")
    parser.add_argument("--config", type=str, default="python-datasource.json",
                        help="Path to the DB config JSON file.")
    parser.add_argument("--alias", type=str, default="MYSQLDS1",
                        help="Configuration alias to use from the config file.")
    parser.add_argument("--num_tables", type=int, default=1,
                        help="Total number of transactions_{n} tables to consider (1 to 1000).")
    parser.add_argument("--start_table", type=int, default=1,
                        help="Starting table number to process. Tables with lower numbers are skipped.")
    parser.add_argument("--records_per_table", type=int, default=1000,
                        help="Number of rows to bulk-seed per table before simulation (1000 to 10000000).")
    parser.add_argument("--num_accounts", type=int, default=100,
                        help="Number of rows to seed in the shared accounts table.")
    parser.add_argument("--batch_size", type=int, default=10000,
                        help="Batch size used for the initial bulk-seed inserts.")
    parser.add_argument("--num_workers", type=int, default=1,
                        help="Number of concurrent workers, for both bulk-seed inserts and --simulate mode.")
    parser.add_argument("--reset_all", action="store_true",
                        help="Drop and recreate all processed transactions_{n} tables (accounts is left intact).")
    parser.add_argument("--reset_tables", type=str, default="",
                        help="Comma-separated table numbers to reset, e.g. '1,3,5'.")
    parser.add_argument("--drop_database", action="store_true",
                        help="Teardown: drop every table this tool owns (transactions_* and accounts), then exit.")

    parser.add_argument("--simulate", action="store_true",
                        help="After bulk-seeding, run a continuous production-like DML+DDL simulation.")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Delay (seconds) between simulated operations, per worker.")
    parser.add_argument("--duration", type=float, default=None,
                        help="Auto-stop --simulate after this many seconds (default: run until Ctrl+C).")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed Python's random module for reproducible runs (needed to compare two ingestion "
                             "mechanisms on statistically equivalent workloads).")

    parser.add_argument("--insert_ratio", type=float, default=60,
                        help="Relative weight of single-row inserts in --simulate mode.")
    parser.add_argument("--update_ratio", type=float, default=30,
                        help="Relative weight of single-row updates in --simulate mode.")
    parser.add_argument("--delete_ratio", type=float, default=10,
                        help="Relative weight of single-row deletes in --simulate mode.")
    parser.add_argument("--transfer_ratio", type=float, default=5,
                        help="Relative weight of multi-statement transfer transactions in --simulate mode.")
    parser.add_argument("--recent_bias", type=float, default=0.85,
                        help="Probability that update/delete targets a recently-inserted row rather than a "
                             "uniformly random one.")
    parser.add_argument("--delete_mode", choices=["hard", "soft"], default="hard",
                        help="hard: DELETE the row. soft: mark status='deleted' instead (never cascades).")
    parser.add_argument("--cascade_delete_ratio", type=float, default=10,
                        help="Percent of hard deletes that are account-level cascading deletes instead of a "
                             "single transaction row.")

    parser.add_argument("--ddl_interval_seconds", type=float, default=300,
                        help="Approximate seconds between DDL drift events, per table (jittered 0.5x-1.5x).")
    parser.add_argument("--ddl_ops", type=str, default="",
                        help=f"Comma-separated subset of DDL drift ops to enable (default: all of {DDL_OP_NAMES}).")
    parser.add_argument("--changelog", type=str, default="generator_changelog.jsonl",
                        help="Path to the ground-truth JSONL changelog.")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    try:
        db_params = load_db_config(args.config, args.alias)
    except Exception as e:
        print("Error loading config:", e)
        return

    try:
        conn = pymysql.connect(**db_params)
        assert_writable(conn)
    except Exception as e:
        print("Error connecting to the database:", e)
        return

    if args.drop_database:
        dropped = schema.teardown_all(conn)
        print(f"Dropped tables: {dropped}")
        conn.close()
        return

    if not (1 <= args.num_tables <= 1000):
        raise ValueError("num_tables must be between 1 and 1000.")
    if not (1000 <= args.records_per_table <= 10000000):
        raise ValueError("records_per_table must be between 1000 and 10000000.")
    if args.start_table < 1:
        raise ValueError("start_table must be 1 or greater.")
    if args.start_table > args.num_tables:
        print("start_table is greater than num_tables. Nothing to process.")
        return
    if args.delete_mode not in ("hard", "soft"):
        raise ValueError("delete_mode must be 'hard' or 'soft'.")

    reset_tables_set = set()
    if args.reset_tables:
        try:
            reset_tables_set = set(int(x.strip()) for x in args.reset_tables.split(',') if x.strip() != "")
        except Exception as e:
            print("Error processing --reset_tables:", e)
            return

    changelog = Changelog(args.changelog)

    schema.create_accounts_table(conn)
    schema.seed_accounts(conn, args.num_accounts, changelog=changelog)

    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {workload.ACCOUNTS_TABLE}")
        account_ids = [row[0] for row in cur.fetchall()]

    table_names = []
    for i in range(args.start_table, args.num_tables + 1):
        table_name = f"transactions_{i}"
        table_names.append(table_name)
        if args.reset_all or (i in reset_tables_set):
            print(f"Resetting table: {table_name}")
            schema.drop_transactions_table(conn, table_name)
        else:
            print(f"Creating table (if not exists): {table_name}")
        schema.create_transactions_table(conn, table_name)

        print(f"Seeding {args.records_per_table} records into {table_name}.")
        if args.num_workers > 1:
            insert_seed_records_concurrent(
                db_params, table_name, args.records_per_table, account_ids,
                batch_size=args.batch_size, num_workers=args.num_workers, changelog=changelog,
            )
        else:
            insert_seed_records(conn, table_name, args.records_per_table, account_ids,
                                 batch_size=args.batch_size, changelog=changelog)

    if args.simulate:
        run_simulation(db_params, table_names, changelog, args)

    conn.close()
    changelog.close()


if __name__ == "__main__":
    main()
