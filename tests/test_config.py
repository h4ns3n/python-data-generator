import json

import pytest

from mysql_generator.config import load_db_config, parse_jdbc_url


def test_parse_jdbc_url_basic():
    assert parse_jdbc_url("jdbc:mysql://myhost:3306/mydb") == {
        "host": "myhost",
        "port": 3306,
        "database": "mydb",
    }


def test_parse_jdbc_url_rejects_non_mysql_scheme():
    with pytest.raises(ValueError):
        parse_jdbc_url("jdbc:postgresql://myhost:5432/mydb")


def test_parse_jdbc_url_rejects_malformed_url():
    with pytest.raises(ValueError):
        parse_jdbc_url("jdbc:mysql://myhost-no-port-or-db")


def test_load_db_config_with_ssl(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "MYSQLDS1": {
            "url": "jdbc:mysql://myhost:3306/mydb",
            "username": "user1",
            "password": "pass1",
            "ssl": True,
        }
    }))
    params = load_db_config(str(config_path), "MYSQLDS1")
    assert params == {
        "host": "myhost",
        "port": 3306,
        "database": "mydb",
        "user": "user1",
        "password": "pass1",
        "ssl": {},
    }


def test_load_db_config_without_ssl_has_no_ssl_kwarg(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "MYSQLDS1": {
            "url": "jdbc:mysql://myhost:3306/mydb",
            "username": "user1",
            "password": "pass1",
            "ssl": False,
        }
    }))
    params = load_db_config(str(config_path), "MYSQLDS1")
    assert "ssl" not in params


def test_load_db_config_unknown_alias_raises(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"MYSQLDS1": {"url": "jdbc:mysql://h:1/d", "username": "u", "password": "p"}}))
    with pytest.raises(ValueError):
        load_db_config(str(config_path), "NOT_AN_ALIAS")
