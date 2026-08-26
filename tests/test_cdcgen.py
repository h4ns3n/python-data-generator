import argparse

import cdcgen


def test_forward_skips_none_and_false():
    args = argparse.Namespace(a=None, b=False, c=5)
    assert cdcgen._forward(args, {"a": "--a", "b": "--b", "c": "--c"}) == ["--c", "5"]


def test_forward_bare_flag_for_true():
    args = argparse.Namespace(reset_all=True)
    assert cdcgen._forward(args, {"reset_all": "--reset_all"}) == ["--reset_all"]


def test_forward_preserves_mapping_order():
    args = argparse.Namespace(x=1, y=2)
    assert cdcgen._forward(args, {"x": "--x", "y": "--y"}) == ["--x", "1", "--y", "2"]


def test_config_alias_work_before_and_after_subcommand():
    parser = cdcgen.build_parser()
    before = parser.parse_args(["--config", "c.json", "--alias", "A1", "check"])
    after = parser.parse_args(["check", "--config", "c.json", "--alias", "A1"])
    assert before.config == after.config == "c.json"
    assert before.alias == after.alias == "A1"


def test_defaults_point_at_the_poc_alias():
    parser = cdcgen.build_parser()
    args = parser.parse_args(["check"])
    assert args.config == "python-datasource.json"
    assert args.alias == "CDC_POC_MYSQL"


def test_simulate_subcommand_builds_expected_flags():
    parser = cdcgen.build_parser()
    args = parser.parse_args(["simulate", "--num-tables", "2", "--duration", "60", "--seed", "7"])
    mapping = {
        "num_tables": "--num_tables", "records_per_table": "--records_per_table",
        "num_accounts": "--num_accounts", "num_customers": "--num_customers", "num_workers": "--num_workers",
        "duration": "--duration", "seed": "--seed", "delay": "--delay",
        "insert_ratio": "--insert_ratio", "update_ratio": "--update_ratio",
        "delete_ratio": "--delete_ratio", "transfer_ratio": "--transfer_ratio",
        "recent_bias": "--recent_bias", "delete_mode": "--delete_mode",
        "cascade_delete_ratio": "--cascade_delete_ratio",
        "ddl_interval_seconds": "--ddl_interval_seconds", "ddl_ops": "--ddl_ops",
        "changelog": "--changelog", "reset_all": "--reset_all",
    }
    forwarded = cdcgen._forward(args, mapping)
    assert "--num_tables" in forwarded and forwarded[forwarded.index("--num_tables") + 1] == "2"
    assert "--duration" in forwarded and forwarded[forwarded.index("--duration") + 1] == "60.0"
    assert "--seed" in forwarded and forwarded[forwarded.index("--seed") + 1] == "7"
    # unset optional flags (e.g. --delete-mode) must not appear at all
    assert "--delete_mode" not in forwarded
