#!/usr/bin/env python3
"""
Convenience CLI wrapping the MySQL CDC PoC transaction simulator scripts.

This is a thin dispatcher, not new business logic: each subcommand shells out
to the existing simulate_data_mysql.py / test_connection_mysql.py /
verify_snowflake.py with sensible defaults and shorter, kebab-case flags, so
day-to-day use doesn't require remembering the full underlying flag set.
The underlying scripts remain the source of truth and stay runnable directly
for anything this wrapper doesn't expose (see --help on each, or .cursorrules).

Examples:
  ./cdcgen.py check
  ./cdcgen.py seed --num-tables 3 --records-per-table 10000
  ./cdcgen.py simulate --num-tables 3 --duration 600 --seed 42 --num-workers 4
  ./cdcgen.py reset --num-tables 3
  ./cdcgen.py teardown
  ./cdcgen.py verify --changelog run1_changelog.jsonl --sf-database CDC_POC --sf-schema PUBLIC --tables transactions_1
  ./cdcgen.py selftest
  ./cdcgen.py selftest --live
"""
import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable


def _run(cmd):
    print("+ " + " ".join(cmd), flush=True)
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    sys.exit(result.returncode)


def _forward(args, mapping):
    """
    Build a flag list for the underlying script from a subset of parsed args.
    mapping: {arg_attr: underlying_flag_name}. store_true flags forward as a
    bare flag when True; everything else forwards as `--flag value` when set.
    """
    forwarded = []
    for attr, flag in mapping.items():
        value = getattr(args, attr, None)
        if value is None or value is False:
            continue
        if value is True:
            forwarded.append(flag)
        else:
            forwarded.extend([flag, str(value)])
    return forwarded


def cmd_check(args):
    _run([PYTHON, "test_connection_mysql.py", "--config", args.config, "--alias", args.alias])


def cmd_seed(args):
    mapping = {
        "num_tables": "--num_tables",
        "records_per_table": "--records_per_table",
        "num_accounts": "--num_accounts",
        "batch_size": "--batch_size",
        "num_workers": "--num_workers",
        "reset_all": "--reset_all",
    }
    cmd = [PYTHON, "simulate_data_mysql.py", "--config", args.config, "--alias", args.alias]
    cmd += _forward(args, mapping)
    _run(cmd)


def cmd_simulate(args):
    mapping = {
        "num_tables": "--num_tables",
        "records_per_table": "--records_per_table",
        "num_accounts": "--num_accounts",
        "num_workers": "--num_workers",
        "duration": "--duration",
        "seed": "--seed",
        "delay": "--delay",
        "insert_ratio": "--insert_ratio",
        "update_ratio": "--update_ratio",
        "delete_ratio": "--delete_ratio",
        "transfer_ratio": "--transfer_ratio",
        "recent_bias": "--recent_bias",
        "delete_mode": "--delete_mode",
        "cascade_delete_ratio": "--cascade_delete_ratio",
        "ddl_interval_seconds": "--ddl_interval_seconds",
        "ddl_ops": "--ddl_ops",
        "changelog": "--changelog",
        "reset_all": "--reset_all",
    }
    cmd = [PYTHON, "simulate_data_mysql.py", "--config", args.config, "--alias", args.alias, "--simulate"]
    cmd += _forward(args, mapping)
    _run(cmd)


def cmd_reset(args):
    mapping = {
        "num_tables": "--num_tables",
        "records_per_table": "--records_per_table",
        "tables": "--reset_tables",
    }
    cmd = [PYTHON, "simulate_data_mysql.py", "--config", args.config, "--alias", args.alias, "--reset_all"]
    cmd += _forward(args, mapping)
    _run(cmd)


def cmd_teardown(args):
    _run([PYTHON, "simulate_data_mysql.py", "--config", args.config, "--alias", args.alias, "--drop_database"])


def cmd_verify(args):
    cmd = [
        PYTHON, "verify_snowflake.py",
        "--changelog", args.changelog,
        "--sf-database", args.sf_database,
        "--sf-schema", args.sf_schema,
        "--tables", args.tables,
        "--sf-connection", args.sf_connection,
    ]
    _run(cmd)


def cmd_selftest(args):
    cmd = [PYTHON, "-m", "pytest", "tests/"]
    if args.live:
        cmd += ["-m", "live_cluster"]
    _run(cmd)


def build_parser():
    # --config/--alias are common to every subcommand and need to work both
    # before AND after the subcommand name (argparse subparsers don't inherit
    # the top-level parser's optionals otherwise). The real defaults live only
    # on the top-level parser; the shared `common` parent used by each
    # subparser uses argparse.SUPPRESS as its default — otherwise, when the
    # flag is given BEFORE the subcommand, the subparser's own default would
    # silently overwrite it during its parsing pass even though the user
    # never touched it at that level. (Caught by a unit test, not manual
    # testing — manual testing happened to use values identical to the
    # defaults, which masked exactly this bug.)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default=argparse.SUPPRESS, help="DB config JSON file.")
    common.add_argument("--alias", default=argparse.SUPPRESS, help="Config alias to use.")

    parser = argparse.ArgumentParser(prog="cdcgen", description=__doc__.strip().splitlines()[0])
    parser.add_argument("--config", default="python-datasource.json", help="DB config JSON file.")
    parser.add_argument("--alias", default="CDC_POC_MYSQL", help="Config alias to use.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p = subparsers.add_parser("check", help="Test connectivity and report writer/reader role.", parents=[common])
    p.set_defaults(func=cmd_check)

    p = subparsers.add_parser("seed", help="Bulk-seed accounts + transactions tables (no simulation).", parents=[common])
    p.add_argument("--num-tables", type=int, default=1, dest="num_tables")
    p.add_argument("--records-per-table", type=int, default=10000, dest="records_per_table")
    p.add_argument("--num-accounts", type=int, default=100, dest="num_accounts")
    p.add_argument("--batch-size", type=int, default=None, dest="batch_size")
    p.add_argument("--num-workers", type=int, default=None, dest="num_workers")
    p.add_argument("--reset-all", action="store_true", dest="reset_all")
    p.set_defaults(func=cmd_seed)

    p = subparsers.add_parser("simulate", help="Bulk-seed, then run the full DDL+DML production-like simulation.", parents=[common])
    p.add_argument("--num-tables", type=int, default=1, dest="num_tables")
    p.add_argument("--records-per-table", type=int, default=10000, dest="records_per_table")
    p.add_argument("--num-accounts", type=int, default=100, dest="num_accounts")
    p.add_argument("--num-workers", type=int, default=None, dest="num_workers")
    p.add_argument("--duration", type=float, default=None, help="Auto-stop after this many seconds (default: until Ctrl+C).")
    p.add_argument("--seed", type=int, default=None, help="Reproducible run — needed to compare Artie vs Openflow fairly.")
    p.add_argument("--delay", type=float, default=None)
    p.add_argument("--insert-ratio", type=float, default=None, dest="insert_ratio")
    p.add_argument("--update-ratio", type=float, default=None, dest="update_ratio")
    p.add_argument("--delete-ratio", type=float, default=None, dest="delete_ratio")
    p.add_argument("--transfer-ratio", type=float, default=None, dest="transfer_ratio")
    p.add_argument("--recent-bias", type=float, default=None, dest="recent_bias")
    p.add_argument("--delete-mode", choices=["hard", "soft"], default=None, dest="delete_mode")
    p.add_argument("--cascade-delete-ratio", type=float, default=None, dest="cascade_delete_ratio")
    p.add_argument("--ddl-interval-seconds", type=float, default=None, dest="ddl_interval_seconds")
    p.add_argument("--ddl-ops", default=None, dest="ddl_ops")
    p.add_argument("--changelog", default="generator_changelog.jsonl")
    p.add_argument("--reset-all", action="store_true", dest="reset_all")
    p.set_defaults(func=cmd_simulate)

    p = subparsers.add_parser("reset", help="Drop and recreate transactions_{n} tables (accounts left intact).", parents=[common])
    p.add_argument("--num-tables", type=int, default=1, dest="num_tables")
    p.add_argument("--records-per-table", type=int, default=10000, dest="records_per_table")
    p.add_argument("--tables", default=None, help="Comma-separated table numbers to reset instead of all, e.g. '1,3'.")
    p.set_defaults(func=cmd_reset)

    p = subparsers.add_parser("teardown", help="Drop every table this tool owns (accounts + all transactions_*).", parents=[common])
    p.set_defaults(func=cmd_teardown)

    p = subparsers.add_parser("verify", help="Compare Snowflake state against a changelog.")
    p.add_argument("--changelog", required=True)
    p.add_argument("--sf-database", required=True)
    p.add_argument("--sf-schema", required=True)
    p.add_argument("--tables", required=True, help="Comma-separated transactions_{n} table names.")
    p.add_argument("--sf-connection", default="snowflake-cli")
    p.set_defaults(func=cmd_verify)

    p = subparsers.add_parser("selftest", help="Run the pytest suite (unit tests by default).")
    p.add_argument("--live", action="store_true", help="Also run tests against the real cluster (-m live_cluster).")
    p.set_defaults(func=cmd_selftest)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
