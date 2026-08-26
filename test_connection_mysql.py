#!/usr/bin/env python3
import argparse
import pymysql

from mysql_generator.config import load_db_config


def test_connection(db_params):
    """
    Test the database connection by connecting and querying the MySQL version.
    Also reports read_only status, since Aurora readers reject all DML/DDL and
    this is a common source of confusion when picking a config alias.
    """
    try:
        connection = pymysql.connect(**db_params)
        print("Successfully connected to the database!")
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION();")
            print("Database version:", cursor.fetchone())
            # Aurora readers report @@read_only=0 (that variable reflects standalone
            # MySQL replication state, unused by Aurora's shared storage) — the
            # actual signal is @@innodb_read_only=1.
            cursor.execute("SELECT @@innodb_read_only;")
            (read_only,) = cursor.fetchone()
            role = "READER (innodb_read_only=ON, no DDL/DML, no binlog)" if read_only else "WRITER (innodb_read_only=OFF)"
            print("Endpoint role:", role)
        connection.close()
    except Exception as e:
        print("Failed to connect to the database. Error:", e)


def main():
    parser = argparse.ArgumentParser(description="Test Connection to MySQL Data Source")
    parser.add_argument("--config", type=str, default="python-datasource.json",
                        help="Path to the database config JSON file.")
    parser.add_argument("--alias", type=str, default="MYSQLDS1",
                        help="Configuration alias to use from the config file.")
    args = parser.parse_args()

    try:
        db_params = load_db_config(args.config, args.alias)
        test_connection(db_params)
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
